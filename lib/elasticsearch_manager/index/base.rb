require 'active_support/inflector'
require 'active_support/core_ext/numeric/time'

# This class is not intended for direct use and should be subclassed. Any
# subclasses can register types in the following way:
#
#   class SomeComplexIndex < Index::Base
#     register_type :product, mapping_class: SomeProductMapping
#     register_type :category, mapping_class: SomeCategoryMapping
#   end
#
# This should be all that is required for a simple index. The subclasses can
# then implement the #import method to define how they get initially populated.
#
# The Mapping classes define how documents should be mapped from other objects
# and should ideally inherit from Mapping::Base, but don't necessarily need to
# so long as they provide the following methods:
#
#   .definition
#   .document_for(object)
#
# See Mapping::Base for more info.
#
# NOTE: The public and private methods in this file are split out fairly
# logically into different concerns, could be worth a further refactor
#
module Elasticsearch
  class Index::Base

    attr_reader :index_name, :interface

    def initialize(elasticsearch_interface = default_interface)
      @interface = elasticsearch_interface
      # Note, the index_name can be nil if there's no index that's
      # already been created here.  This is our way of indicating that
      # there is no pre-existing index in ES.
      @index_name = @interface.index_from_alias(index_alias_for_search)
    end

    ###########################################################################
    # Index configuration methods
    ###########################################################################

    # All types for an index must be defined with a type name (which should be
    # a symbol) and the mapping class which can be used to create JSON
    # documents for it. e.g.
    #   {
    #     product: Elasticearch::Mapping::SearchProduct,
    #     category: Elasticearch::Mapping::Category
    #   }
    def self.register_type(name, mapping_class:)
      raise "The name registered must be a Symbol, not #{name.class}" unless name.is_a?(Symbol)

      @document_types ||= {}
      @document_types.merge!(name => mapping_class)
    end

    def self.document_types
      @document_types || {}
    end

    def document_types
      self.class.document_types
    end

    ###########################################################################
    # Index Management methods
    ###########################################################################

    def create
      @index_name = generate_index_name_from_alias
      @interface.create(
        @index_name,
        {
          mappings: mapping_definition,
          settings: settings_definition
        }
      )
    end

    def delete
      @interface.delete(@index_name) unless @index_name.nil?
    end

    def refresh
      @interface.refresh(@index_name)
    end

    def store_and_refresh(params)
      store(params)
      refresh
    end

    def switch_alias
      @interface.set_alias(index_alias_for_search, @index_name)

      if index_importing?
        @interface.unassign_index_alias(index_being_imported, index_importing_alias)
      end
    end

    def create_import_and_switch(options = {})
      create
      @interface.set_alias(index_importing_alias, @index_name)
      import
      switch_alias
      cleanup_old_indices unless options[:no_cleanup]
    end

    # This is primarily intended for use in tests
    #
    # NOTE: THIS IS SHOCKING, YES WE KNOW!
    #
    # We're hitting problems on Elasticsearch in CI at the moment where indexes
    # are inexplicably missing or there at weird times. Given that when using
    # this method we don't *care* about the previous indexes, we just try and
    # force deletion and creation however we can.
    def rebuild(options = {})
      begin
        delete
        create
      rescue Elasticsearch::Transport::Transport::Errors::BadRequest => e
        # The creation failed due to the index already existing for some
        # reason, so try again!
        delete
        create
      rescue Elasticsearch::Transport::Transport::Errors::NotFound => e
        # Index doesn't exist when trying to delete, so just create
        create
      end

      switch_alias unless options[:dont_switch_alias]

      if options[:import]
        import
        refresh
      end
    end

    def self.rebuild(options = {})
      new.rebuild(options)
    end

    # This method must be overridden in subclasses. This generic superclass has
    # no idea how specific indexes should be populated, but it's useful having
    # it here in order to define the #rebuild and #create_import_and_switch
    # methods, as they are the only things that should usuall call this.
    def import
      raise NotImplementedError.new("Index::Base#import must be overridden")
    end

    def index_alias_for_search
      index_alias = root_index_name
      index_alias << "_" + AppConfig.site.to_s.underscore
      index_alias << "_" + environment
      index_alias << "_" + ENV['EXECUTOR_NUMBER'] if ENV && ENV['EXECUTOR_NUMBER']
      index_alias << "_" + ENV['TEST_ENV_NUMBER'] if ENV && ENV['TEST_ENV_NUMBER']

      index_alias
    end

    ###########################################################################
    # Document storage methods
    ###########################################################################

    # Purely a helper method to wrap up turning an object into a document
    # It should be called with a hash of params, where the key is the document
    # type, and the value is the object being stored.  For example:
    #
    #   Elasticsearch::Index::SomeIndex.store(
    #     product:  Product.first,
    #     category: Category.find_by_path("/home")
    #   )
    #
    def self.store(params)
      new.store(params)
    end

    def store(params)
      params.each do |document_type, target|
        store_document(
          document: document_for(document_type: document_type, target: target),
          document_type: document_type
        )
      end
    end

    # Purely a helper method to wrap up turning an object into a document
    # It should be called with a hash of params, where the key is the document
    # type, and the value is the object(s) being stored.  For example:
    #
    #   Elasticsearch::Index::SomeIndex.bulk_store(
    #     product:  Product.listable,
    #     category: Category.find_by_path("/home").children
    #   )
    #
    def self.bulk_store(params)
      new.bulk_store(params)
    end

    def bulk_store(params)
      params.each do |document_type, targets|
        documents = targets.map do |target|
          document_for(document_type: document_type, target: target)
        end

        bulk_store_documents(
          documents: documents,
          document_type: document_type
        )
      end
    end

    # Purely a helper method to wrap up turning an object into a document
    # It should be called with a hash of params, where the key is the document
    # type, and the value is the object(s) being stored.  For example:
    #
    #   Elasticsearch::Index::SomeIndex.delete(
    #     product:  Product.first,
    #     category: Category.find_by_path("/home")
    #   )
    #
    # Ideally, this would be called #delete. However, a ballsup in my own
    # foresight means that there's already a #delete method for deleting the
    # index, so #delete_by_id it is.
    #
    def self.delete_by_id(params)
      new.delete_by_id(params)
    end

    def delete_by_id(params)
      params.each do |document_type, target_id|
        delete_document_by_id(
          document_type: document_type,
          id:            target_id
        )
      end
    end

    private

    ###########################################################################
    # Private index management methods
    ###########################################################################

    def default_interface
      Elasticsearch::MultiClusterInterface.new
    end

    def root_index_name
      self.class.name.demodulize.underscore.pluralize
    end

    def mapping_definition
      unless document_types
        raise "#{self.class.name} has no mappings defined"
      end

      full_mapping = {}
      document_types.each do |type, mapping|
        full_mapping.merge!(type => { properties: mapping.definition })
      end
      full_mapping
    end

    def settings_definition
      settings = {
        analysis: {
          analyzer: {
            snowball: {
              type:      "snowball",
              language:  language
            },
            light_english: {
              type:      "custom",
              tokenizer: "standard",
              filter:    english_analyzer_filters
            },
            light_german: {
              type:      "custom",
              tokenizer: "standard",
              filter:    ["standard", "lowercase", "german_stop", "light_german"]
            },
          },
          filter: {
            light_english: {
              type: "stemmer",
              name: "light_english"
            },
            light_german: {
              type: "stemmer",
              name: "light_german"
            },
            english_stop: {
              type:      "stop",
              stopwords: "_english_"
            },
            german_stop: {
              type:      "stop",
              stopwords: "_german_"
            }
          }
        }
      }

      settings.merge!(number_of_shards: number_of_shards) if number_of_shards
      settings.merge!(number_of_replicas: number_of_replicas) if number_of_replicas
      settings[:analysis][:filter].merge!(
        en_synonym: {
          type: "synonym",
          synonyms_path: synonyms_file_path
        }
      ) if Feature.enabled?(:elasticsearch_en_synonyms)
      settings
    end

    def cleanup_old_indices
      indices = @interface.all_existing_indices

      for_deletion = indices.select {|i| i =~ /\A#{index_alias_for_search}_\d{14}\Z/ }.sort
      not_for_deletion = for_deletion.pop(2)

      for_deletion.each { |i| @interface.delete(i) }

      # Close the index we're no longer using, unless we've just got one index!
      @interface.close(not_for_deletion.first) unless not_for_deletion.size <= 1
    end

    def generate_index_name_from_alias
      index_alias_for_search + '_' + Time.current.strftime(Time::DATE_FORMATS[:number])
    end

    def index_importing_alias
      index_alias_for_search + "_importing"
    end

    def index_importing?
      !!index_being_imported
    end

    def index_being_imported
      @interface.index_from_alias(index_importing_alias)
    end

    ###########################################################################
    # Private document storage methods
    ###########################################################################

    # Returns a document which can be stored by the interface object
    #
    #  target - raw object which is being indexed
    #  document_type - key for the mapping class
    #
    def document_for(target:, document_type:)
      unless document_types[document_type]
        raise "#{self.class.name} has no mappings defined for type: #{document_type}"
      end

      {type: document_type}.merge(
        document_types[document_type].document_for(target)
      )
    end

    def store_document(document:, document_type:)
      @interface.store(
        index_name:    @index_name,
        document_type: document_type.to_s,
        document:      document
      )

      if index_importing?
        @interface.store(
          index_name:    index_being_imported,
          document_type: document_type.to_s,
          document:      document
        )
      end
    end

    def bulk_store_documents(documents:, document_type:)
      @interface.bulk_store(
        index_name:    @index_name,
        document_type: document_type.to_s,
        documents:     documents
      )

      if index_importing?
        @interface.bulk_store(
          index_name:    index_being_imported,
          document_type: document_type.to_s,
          documents:     documents
        )
      end
    end

    def delete_document_by_id(document_type:, id:)
      @interface.delete_by_id(
        index_name:    @index_name,
        document_type: document_type.to_s,
        id:            id
      )

      if index_importing?
        @interface.delete_by_id(
          index_name:    index_being_imported,
          document_type: document_type.to_s,
          id:            id
        )
      end
    end

    ###########################################################################
    # Private index configuration methods
    ###########################################################################

    def number_of_shards
      AppConfig[:elasticsearch][:number_of_shards][root_index_name.to_sym] ||
        AppConfig[:elasticsearch][:number_of_shards][:default]
    end

    def number_of_replicas
      AppConfig[:elasticsearch][:number_of_replicas][root_index_name.to_sym] ||
        AppConfig[:elasticsearch][:number_of_replicas][:default]
    end

    def language
      AppConfig[:elasticsearch][:language]
    end

    def synonyms_file_path
      AppConfig[:elasticsearch][:synonyms_file] || File.join(Rails.root, "config", "locales", "elasticsearch", "en_synonyms.txt")
    end

    def english_analyzer_filters
      filters = ["standard", "lowercase", "english_stop", "apostrophe", "light_english"]
      filters << "en_synonym" if Feature.enabled?(:elasticsearch_en_synonyms)
      filters
    end

    def environment
      # Preview needs to hijack production indexes in order to show
      # the same products
      if Rails.env.downcase == "preview"
        "production"
      else
        Rails.env.downcase
      end
    end
  end
end
