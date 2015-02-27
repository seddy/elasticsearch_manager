# This relies on the elasticsearch-ruby gem.  In theory any other gem
# could be used, in which case this file shouldn't be modified, but
# another class (e.g.  TireInterface) should be implemented which has
# the same public methods.
#
# This class is soley responsible for physically managing our
# interface with Elasticsearch. The contents of indexes and how they
# are defined are managed from within classes within the
# Elasticsearch::Index namespace.
module Elasticsearch
  class Interface

    # We make our clients singeltons in order to make use of persistent
    # HTTP connections
    def self.client_for(host, port)
      @clients ||= {}

      if @clients[host].nil?
        # Not seen host before
        @clients[host] = {port => new_client_for(host, port)}
      else
        # Not seen port before
        if @clients[host][port].nil?
          @clients[host][port] = new_client_for(host, port)
        end
      end

      @clients[host][port]
    end

    def self.new_client_for(host, port)
      Elasticsearch::Client.new(
        transport_options: {
          request: { timeout: AppConfig[:elasticsearch][:http_timeout] }
        },
        # Warning - using multiple hosts here has proven to be highly unstable
        # in production environments, so we just don't do it.
        hosts: [{host: host, port: port}],
        retry_on_failure: AppConfig[:elasticsearch][:retries]
      )
    end

    attr_reader :host

    def initialize(host:, port: 9200, policy: Elasticsearch::InterfacePolicy_1_x)
      if policy.respond_to?(:constantize)
        # If we've come straight from config hash, this may be a string
        @version_policy = policy.constantize
      else
        @version_policy = policy
      end

      @client = self.class.client_for(host, port)
      @host = host
      @port = port
    end

    # Creates an Elasticsearch index.
    #
    # Indexes are created and named with a timestamp.  For example, if
    # the alias is "product", we'll create a product index called
    # "product_20130514124608".  Aliases are then used to manage which
    # index is currently used by search operations.
    #
    # This allows us to avoid index operations overlapping and create
    # new indexes on the fly.  Indexing can take some time (e.g. new
    # product index which has millions of rows) so it's better to do
    # this in the background and then switch the alias.
    def create(index_name, options={})
      payload = {
        index: index_name,
        body: options
      }

      response = client.indices.create(payload)

      unless @version_policy.create_successful?(response)
        raise "Elasticsearch::Interface - Failed to create index #{index_name}: #{response.inspect}."
      end

      response
    end

    def store(params)
      index_name = params.fetch(:index_name)
      document_type = params.fetch(:document_type)
      document = params.fetch(:document)

      response = client.index(
        index:  index_name,
        type:   document_type,
        id:     document[:id],
        body:   document
      )

      unless @version_policy.store_successful?(response)
        raise "Elasticsearch::Interface - Failed to store document #{document} in #{index_name}: #{response.inspect}. #{error_help_for("This")}"
      end

      if Feature.enabled?(:elasticsearch_logging)
        Rails.logger.info("Elasticsearch Response: #{response.inspect}")
      end

      response
    end

    def bulk_store(params)
      index_name = params[:index_name]
      type       = params[:document_type]
      documents  = params[:documents]

      body = []

      documents.each do |doc|
        # Not sure if the document is string or symbol indexed....
        id = doc[:id] || doc["id"]
        body <<  { index:  { _index: index_name, _type: type, _id: id, data: doc } }
      end

      response = client.bulk(
        body: body
      )

      # "item" will be something like:
      #   {
      #      "create" => {
      #          "_index"    => "products_uk_test_20140207132456",
      #          "_type"     => "product",
      #          "_id"       => "LxkekAPqSmeFpkX2d36xGA",
      #          "_version"  => 1,
      #          "ok"        => true
      #      }
      #   }
      #
      response["items"].each do |item|
        errors = []
        unless sucessful_operation_on_document?(item)
          errors << "(#{index_name}: #{item.inspect})"
        end

        raise ["Elasticsearch::Interface.bulk_store: Failed to store documents, #{error_help_for("these")}:"].concat(errors).join(", ") if errors.any?
      end

      response
    end

    def update(index_name, type, id, document)
      client.update(
        index:  index_name,
        type:   type,
        id:     id,
        body: { doc: document }
      )
      # TODO: This clearly doesn't work because response doesn't exist. Leaving
      # as-is for the moment, but bear in mind that response_status_field also
      # doesn't exist either if you want to fix this!
      unless response[@version_policy.response_status_field] == true
        raise "Elasticsearch::Interface.update: Failed to update document (##{id}) #{document} in #{index_name}: #{response.inspect}. #{error_help_for("This")}"
      end
      response
    end

    def delete_by_id(params)
      index_name    = params.fetch(:index_name)
      document_type = params.fetch(:document_type)
      id            = params.fetch(:id)

      begin
        response = client.delete(
          index:  index_name,
          type:   document_type,
          id:     id
        )
      rescue Elasticsearch::Transport::Transport::Errors::NotFound
        # Don't care, it's already been deleted
        Rails.logger.warn("Elasticsearch failed to delete (404) #{index_name}/#{document_type}/#{id}")
      end

      if Feature.enabled?(:elasticsearch_logging)
        Rails.logger.info("Elasticsearch Response: #{response.inspect}")
      end

      response
    end

    def refresh(index_name)
      client.indices.refresh(index: index_name)
    end

    def close(index_name)
      client.indices.close(index: index_name)
    end

    def delete(index_name)
      client.indices.delete(index: index_name)
    end

    def assign_index_alias(index_name, alias_name)
      client.indices.put_alias(index: index_name, name: alias_name)
    end

    def unassign_index_alias(index_name, alias_name)
      client.indices.delete_alias(index: index_name, name: alias_name)
    end

    # Note, this method assumes an alias will only every apply to a
    # single index, though Elasticsearch does allow for multiple
    # indexes to be tagged with a single alias.
    def set_alias(alias_name, index_name)
      if client.indices.exists_alias(name: alias_name)
        current_index_names = client.indices.get_alias(name: alias_name).keys
        current_index_names.each do |index|
          unassign_index_alias(index, alias_name)
        end
      end

      assign_index_alias(index_name, alias_name)
    end

    def index_from_alias(alias_name)
      index_name = nil
      index_aliases = client.indices.get_aliases

      index_aliases.each do |index, index_config|
        aliases = index_config.fetch("aliases") { next }

        if aliases && aliases.keys.include?(alias_name)
          index_name = index
          break
        end
      end

      index_name
    end

    def all_existing_indices
      client.indices.get_aliases.keys
    end

    def search(index_name, query)
      Rails.logger.info(
        "Running elasticsearch query on #{@host}:#{@port}/#{index_name} with: #{query.to_json}"
      )

      initial_time = Time.now # Don't care about localisation, reduce dependancy on Rails
      results = client.search(index: index_name, body: query)
      final_time = Time.now
      difference = (final_time - initial_time) * 1000

      Rails.logger.info(
        "  => Query on #{@host}:#{@port}/#{index_name} took #{'%.2f' % difference}ms (#{results["took"]}ms on ES)"
      )

      # TODO: Don't use MONITOR here, putting it in for quick monitoring but
      # really this Elasticsearch module should know nothing about MONITOR
      MONITOR.gauge("elasticsearch.search.#{index_name}.total_time", difference.to_i)
      MONITOR.gauge("elasticsearch.search.#{index_name}.query_time", results["took"].to_i)

      results
    end

    private

    attr_reader :client

    def error_help_for(subject = "")
      "#{subject} can be retried by running (e.g. for a product 1234) 'Elasticsearch::Index::Product.new.store(Product.find(1234))' in a console"
    end

    def sucessful_operation_on_document?(item)
      @version_policy.successful_operation_on_document?(item)
    end
  end
end
