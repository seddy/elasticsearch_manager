#
# TODO: By removing the whole "fetch from db" nonsense, does the pagination
# still belong here?  I think it does...
#
module Elasticsearch
  class Results
    # Unlikely this will be needed, but keeps us in-line with default
    # elasticsearch functionality.
    DEFAULT_HITS_PER_PAGE = 10

    attr_reader :facets, :results, :query

    def self.fetch(index_name, query)
      new(
        query,
        Elasticsearch::Interface.
          new(AppConfig[:elasticsearch][:clusters][:cluster_1_x]).
          search(index_name, query)
      )
    end

    def initialize(query, results)
      @query = query
      @results = results

      process_facets if results_have_facets?
    end

    def ids
      raw_hits.map {|item| item["_id"].to_i }
    end

    # If the Elasticsearch::Results object has been initialised with raw
    # results, this allows you to iterate through those and access the source
    # values using method calls.  See the spec for an example.
    def each_with_method_access_on_source(&block)
      raw_hits.each do |item|
        block.call( HashAccessor.new(item) )
      end
    end

    def aggregations(key:)
      return {} unless results["aggregations"]
      return {} unless results["aggregations"][key]
      # Try and get buckets, but if there are none, return full hash. Not ideal.
      # TODO: move all this logic into aggregation-specific objects
      results["aggregations"][key]["buckets"] || results["aggregations"][key]
    end

    def current_page
      query_page
    end

    def per_page
      query[:size] || DEFAULT_HITS_PER_PAGE
    end

    def total_pages
      ( total.to_f / per_page ).ceil
    end

    def next_page
      current_page < total_pages ? (current_page + 1) : nil
    end

    def previous_page
      current_page > 1 ? (current_page - 1) : nil
    end

    def total_entries
      total
    end

    def out_of_bounds?
      (current_page > total_pages) || total_pages == 0
    end

    def offset
      current_page > 0 ? per_page * (current_page - 1) : 0
    end

    private

    def raw_hits
      results["hits"]["hits"]
    end

    def process_facets
      @facets = {}

      raw_facets.each do |facet_name, facet_data|
        @facets[facet_name] ||= {}
        facet_data["terms"].each do |term|
          @facets[facet_name][term["term"].to_s] = term["count"].to_i
        end
      end
    end

    def raw_facets
      results["facets"]
    end

    def results_have_facets?
      !!raw_facets
    end

    def query_page
      from = query.fetch(:from, 0) + 1
      size = query.fetch(:size, 1) # Simply to protect from divide by nil error
      (from.to_f / size).ceil
    end

    def total
      results["hits"]["total"] || 0
    end

    class HashAccessor

      # Leaving with an underscore in case of a "score" value in the source
      attr_reader :_score

      def initialize(values)
        @source = values["_source"]
        @_score = values["_score"]
      end

      def method_missing(method_name, *args)
        source.fetch(method_name.to_s)
      end

      private

      attr_reader :source

    end

  end
end
