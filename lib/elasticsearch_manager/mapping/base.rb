require 'active_record'

module Elasticsearch
  # Elasticsearch::Mapping::Base provides the framework for mappings
  # on other classes.
  #
  # The mapping class will be used by a corresponding Elasticsearch::Index
  # class which will use the .definition method to generate the specific index.
  # The index class only manages the initial import of data.
  #
  # For example, a simplified Product mapping may have its definition defined:
  #
  #  def self.definition
  #   {
  #     id:       {type: 'long',   index:    :not_analyzed},
  #     title:    {type: 'string', analyzer: 'keyword'},
  #     keywords: {type: 'string', analyzer: 'keyword'}
  #   }
  #  end
  #
  # Some fields may not come directly from the object which is being used as
  # the target. These can be defined within the Mapping class instead.
  #
  # For example, with the example above, #keywords doesn't exist on Product, so
  # when we're given a product we can't just call #keywords on it. Instead,
  # this can be defined within the Mapping object as:
  #
  # def keywords
  #   @target.product_details.keywords
  # end
  #
  class Mapping::Base

    def self.definition
      raise "Definition must be set in subclasses"
    end

    def self.document_for(target)
      new(target).document
    end

    def initialize(mapping_target)
      @target = mapping_target
    end

    def document
      definition.keys.inject({}) do |values, key|
        values[key] = fetch_value(key)
        values
      end
    end

    private

    attr_reader :target

    def definition
      self.class.definition
    end

    def fetch_value(key)
      if self.respond_to?(key)
        send(key)
      else
        @target.send(key)
      end
    end
  end
end

