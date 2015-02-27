require "elasticsearch"

require "elasticsearch_manager/interface"
require "elasticsearch_manager/configuration"

# require "elasticsearch_manager/multi_cluster_interface"

# require "elasticsearch_manager/results"

# require "elasticsearch_manager/index"
# require "elasticsearch_manager/mapping"

module ElasticsearchManager

  class << self
    def configure(&block)
      yield(configuration)
    end

    def configuration
      @configuration ||= Configuration.new
    end
  end

end
