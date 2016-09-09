# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

class NatsClient::SocketConnector

  attr_reader :host, :port

  def initialize(host = '127.0.0.1', port = 4222)
    @host = host.freeze
    @port = port.freeze
    freeze
  end

  def open!
    TCPSocket.new(@host, @port)

  rescue Errno::ECONNREFUSED, Errno::ECONNRESET
    nil
  end

end
