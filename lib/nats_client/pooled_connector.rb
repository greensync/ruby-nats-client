# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

# NOT THREAD SAFE
class NatsClient::PooledConnector

  def initialize(connectors)
    @connectors = connectors
    @last_connector = @current_connector = nil
  end

  def open!
    open_first!(shuffled_connectors)
  end

  private

  # randomized, don't use the last one we connected to unless we have to (i.e. only one option)
  def shuffled_connectors
    result = (@connectors - [@last_connector]).shuffle
    result << @last_connector if @last_connector
    result
  end

  def open_first!(connectors)
    connectors.shift.open!

  rescue Errno::ECONNREFUSED
    if connectors.empty?
      raise
    else
      retry
    end
  end

end
