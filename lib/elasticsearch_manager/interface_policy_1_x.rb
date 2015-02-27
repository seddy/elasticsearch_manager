class Elasticsearch::InterfacePolicy_1_x
  def self.hosts
    AppConfig[:elasticsearch][:upgrade][:hosts].map(&:symbolize_keys)
  end

  def self.response_status_field
    "acknowledged"
  end

  def self.create_successful?(response)
    response["acknowledged"] == true
  end

  def self.store_successful?(response)
    # Not sure what the error is here, but think it's handled in the HTTP
    # response status... TODO: verify!!
    true
  end

  def self.successful_operation_on_document?(item)
    # Because the action may not be "create" but "update" or
    # "delete", we need this unreadable chain to fetch the result of
    # the operation.  Note that for creation, status will be 201, update will
    # give 200
    #
    # Example item: {"create"=>{"_index"=>"ed_test", "_type"=>"ed", "_id"=>"1", "_version"=>3, "status"=>200}}
    item.first && item.first.second && [200, 201].include?(item.first.second["status"])
  end
end
