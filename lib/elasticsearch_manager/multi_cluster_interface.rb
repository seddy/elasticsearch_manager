module Elasticsearch
  class MultiClusterInterface

    # The intention of this interface is to allow for some clusters
    def initialize
      @critical_clusters = AppConfig[:elasticsearch][:critical_clusters].map do |cluster|
        Elasticsearch::Interface.new(AppConfig[:elasticsearch][:clusters][cluster.to_sym])
      end

      @dispensable_clusters = AppConfig[:elasticsearch][:dispensable_clusters].map do |cluster|
        Elasticsearch::Interface.new(AppConfig[:elasticsearch][:clusters][cluster.to_sym])
      end
    end

    def method_missing(method_name, *args)
      dispensable_clusters.each do |cluster|
        begin
          cluster.send(method_name, *args)
        rescue Exception => e
          Rails.logger.error("Failed to reach dispensable ES cluster on #{cluster.host}: #{e}, #{e.backtrace}")
          Services.for(:error_notification).notify(e)
        end
      end

      result = nil
      critical_clusters.each do |cluster|
        result = cluster.send(method_name, *args)
      end

      # Saving to a 'result' object and returning.may look weird, because it
      # is.  We need to return some response from our call (for example, if
      # we're doing an #index_from_alias or something like that), so just
      # return whatever the last thing is we do.
      #
      # If there are multiple critical clusters, they should all be kept in
      # step with each other, so this shouldn't be a problem. If they get out
      # of step, then it's safest just to rebuild everything.
      result
    end

    private

    attr_reader :critical_clusters, :dispensable_clusters
  end
end
