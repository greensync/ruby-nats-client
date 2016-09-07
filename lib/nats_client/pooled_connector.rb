# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

# NOT THREAD SAFE
class NatsClient::PooledConnector

  def initialize(connectors)
    @connectors = connectors.freeze
    freeze
  end

  def open!
    @connectors.shuffle.each do |connector|
      socket = connector.open!
      return socket if socket
    end

    nil
  end

end
