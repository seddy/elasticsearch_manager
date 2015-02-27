module ElasticsearchManager
  class Configuration

    class << self
      def config_option(name, options = {})
        define_method(name) do
          read_value(name)
        end

        define_method("#{name}=") do |value|
          set_value(name, value)
        end

        set_default(name, options[:default]) if options[:default]
      end

      def default_values
        @default_values ||= {}
      end

      def set_default(name, value)
        default_values[name] = value
      end
    end

    config_option :http_timeout, default: 10

    config_option :retries, default: 1

    attr_reader :config_values

    def initialize
      @config_values = {}
    end

    private

    def read_value(name)
      config_values[name] || self.class.default_values[name]
    end

    def set_value(name, value)
      config_values[name] = value
    end
  end
end

