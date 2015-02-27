require 'active_support/inflector'
require_relative '../mapping'

# This module can be included on any index which is intended to be a
# single-type index of a particular ActiveRecord model. The index class must:
#
#   - inherit from Elasticsearch::Index::Base
#   - be named the same as the model being indexed
#   - have a mapping class of the same name
#
# For example:
#
# class Elasticsearch
#   class Index::Product < Index::Base
#     include ActiveRecord
#   end
# end
#
# And that is it!  The only thing left to do is override the #import method.
# The main reason for doing this is so that we get a consistent naming
# convention and wrap up the basic implementation of the #import method.
#
# In order to provide further customisation (such as only indexing
# Product.listable on import, for example), override the #import_options method,
# with the Product.listable example:
#
# def import_options
#   {listable: true}
# end
#
module Elasticsearch
  module Index
    module ActiveRecord

      def self.included(klass)
        model_name = klass.name.demodulize
        klass.register_type(
          model_name.underscore.to_sym,
          mapping_class: "Elasticsearch::Mapping::#{model_name}".constantize
        )
      end

      # The conditions parameter allows us to pop additional conditions to the
      # import, so to only do a specific set of product ID's, for example,
      # instead of reimporting everything.  Alternatively, bulk_store can be
      # called directly, but #import may have some optimisations around what it
      # #includes on the AR relation
      def import(conditions: "")
        options            = import_options
        includes           = options.delete(:include)
        default_conditions = options.delete(:conditions)

        if default_conditions && default_conditions.kind_of?(String)
          conditions = default_conditions + " " + conditions
        end

        "::#{model_name}".constantize
          .includes(includes)
          .where(conditions)
          .find_in_batches(options) do |targets|

          Rails.logger.info("Elasticsearch: #{self.class.name} - Storing #{model_name} #{targets.first.id} -> #{targets.last.id}")
          bulk_store(model_type => targets)
        end
      end

      def model_name
        self.class.name.demodulize
      end

      def model_type
        model_name.underscore.to_sym
      end

      private

      # This is the only method which should be overridden where this is
      # included, and allows for the scoping of ActiveRecord objects for the
      # import method
      def import_options
        {}
      end
    end
  end
end
