# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

class NatsClient::SocketConnector

  attr_reader :host, :port

  def initialize(host = '127.0.0.1', port = 4222)
    @host = host
    @port = port
  end

  def open!
    TCPSocket.new(@host, @port)
  end

end

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

# class NatsClient::ActualConnectionconnector
#
#   def initialize(stream)
#     @stream = stream
#     @receiver = NatsClient::Receiver.new
#     @sender = NatsClient::Sender.new(@stream)
#   end
#
# end
#

class NatsClient::Connection

  CONNECTION_MAX_TIMEOUT = 365 * 24 * 60 * 60   # a year...

  def initialize(connector)
    @connector = connector
    @subscriptions = NatsClient::SubscriptionManager.new

    @mutex = Mutex.new
    @receiver = NatsClient::Receiver.new
  end

  def closed?
    @mutex.synchronize do
      @stream.nil? || @stream.closed?
    end
  end

  def close!
    puts "CLOSE"

    @mutex.synchronize do
      @stream.close if @stream && !@stream.closed?
      @stream = @sender = @server_info = nil
    end
  end

  def connect!
    puts "RECONNECT"

    @mutex.synchronize do
      raise "already connected" unless @stream.nil? || @stream.closed?

      @stream = @connector.open!
      @sender = NatsClient::Sender.new(@stream)
      @receiver.reset!
    end

    try_or_close do
      @sender.connect!({})
      @subscriptions.each do |topic_filter, subscription_id, options|
        @sender.sub!(topic_filter, subscription_id, options)
      end
    end

  rescue Errno::ECONNREFUSED
    close!
    sleep 1
    retry
  end

  def publish!(topic, payload, options = {})
    retry_until_open { @sender.pub!(topic, payload, options) }
  end

  def subscribe!(topic_filter, options = {}, &block)
    subscription_id = @subscriptions.add!(topic_filter, options, block)
    retry_until_open { @sender.sub!(topic_filter, subscription_id, options) }
    subscription_id
  end

  def unsubscribe!(subscription_id)
    @subscriptions.remove!(subscription_id)
    retry_until_open { @sender.unsub!(subscription_id) }
  end

  def run!
    loop do
      connect! if closed?

      bytes = read_bytes
      next unless bytes

      @receiver.parse!(bytes) do |event, info|
        case event
        when :info_received
          puts info
          @server_info = info
        when :msg_received
          try_or_close { @sender.unsub!(info.fetch(:subscription_id)) } unless @subscriptions.notify!(info)
        when :ping_received
          try_or_close { @sender.pong! }
        when :protocol_error
          STDERR.puts "Protocol Error: #{info.fetch(:message)}"
          close!
          break
        end
      end
    end
  end

  private

  def read_bytes
    unless @stream.ready?
      wait_readable(@stream)
    end

    @stream.read_nonblock(NatsClient::Receiver::MAX_BUFFER)

  rescue Errno::EPIPE, Errno::ECONNRESET, EOFError
    STDERR.puts "#{$!} read_bytes closing"
    close!
    nil

  rescue IO::WaitReadable
    sleep 0.1
    retry
  end

  def try_or_close
    @mutex.synchronize { yield }
  rescue Errno::EPIPE, Errno::ECONNRESET, EOFError
    STDERR.puts "#{$!} try or close retry"
    close!
  end

  def wait_until_open
    sleep 0.1 while closed?
  end

  def retry_until_open
    wait_until_open
    @mutex.synchronize { yield }
  rescue Errno::EPIPE, Errno::ECONNRESET, EOFError
    STDERR.puts "#{$!} retry until open"
    close!
    retry
  end

  # JRuby 1.7 doesn't seem to have IO.wait
  def wait_readable(stream)
    IO.select([stream])
  end

end

class NatsClient::QueuedMessageStream

  MAX_MESSAGES = 100

  def initialize(connection, topic_filter)
    @connection = connection
    @topic_filter = topic_filter
  end

  def each
    queue = Queue.new
    subscription_id = @connection.subscribe!(@topic_filter) { |topic, payload, reply_to| queue << [topic, payload, reply_to] unless queue.size > MAX_MESSAGES }

    loop do
      topic, payload, reply_to = queue.pop
      yield topic, payload, reply_to
    end
  ensure
    @connection.unsubscribe!(subscription_id) if subscription_id
  end

end

class NatsClient::QueuedRequest

  def initialize(connection)
    @connection = connection
  end

  def request!(topic, request_payload)
    future = Concurrent::IVar.new
    reply_topic = "INBOX.#{Thread.current.object_id.to_s(36)}.#{SecureRandom.hex(4)}"
    subscription_id = @connection.subscribe!(reply_topic) { |topic, payload| future.try_set(payload) }

    @connection.publish!(topic, request_payload, reply_to: reply_topic)

    future.value
  ensure
    @connection.unsubscribe!(subscription_id) if subscription_id
  end

end
