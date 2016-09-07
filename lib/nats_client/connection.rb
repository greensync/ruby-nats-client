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

# class NatsClient::ActualConnection
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

    @sender_mutex = Mutex.new
    @live = Concurrent::AtomicBoolean.new(false)
    @stream = StringIO.new
    @stream.close   # so that it's never null
    @sender = NatsClient::Sender.new(@stream)
    @receiver = NatsClient::Receiver.new
  end

  def publish!(topic, payload, options = {})
    retry_forever { @sender.pub!(topic, payload, options) }
  end

  def subscribe!(topic_filter, options = {}, &block)
    subscription_id = @subscriptions.add!(topic_filter, options, block)
    try_once { @sender.sub!(topic_filter, subscription_id, options) }
    subscription_id
  end

  def unsubscribe!(subscription_id)
    @subscriptions.remove!(subscription_id)
    try_once { @sender.unsub!(subscription_id) }
  end

  def run!
    loop do
      connect! unless live?

      bytes = read_bytes
      next unless bytes

      @receiver.parse!(bytes) do |event, info|
        case event
        when :info_received
          @server_info = info
        when :msg_received
          try_once { @sender.unsub!(info.fetch(:subscription_id)) } unless @subscriptions.notify!(info)
        when :ping_received
          try_once { @sender.pong! }
        when :protocol_error
          STDERR.puts "Protocol Error: #{info.fetch(:message)}"
          close!
          break
        end
      end
    end
  end

  private

  def live?
    @live.true?
  end

  def dead!
    @live.make_false
  end

  def connect!
    @sender_mutex.synchronize do
      dead!
      @stream.close unless @stream.closed?

      @stream = @connector.open!
      @sender = NatsClient::Sender.new(@stream)
      @receiver.reset!
      @live.make_true
    end

    try_once do
      @sender.connect!({})
      @subscriptions.each do |topic_filter, subscription_id, options|
        @sender.sub!(topic_filter, subscription_id, options)
      end
    end

  rescue Errno::ECONNREFUSED
    dead!
    sleep 1
    retry
  end

  def read_bytes
    @stream.readpartial(NatsClient::Receiver::MAX_BUFFER)

  rescue IOError, Errno::EPIPE, Errno::ECONNRESET, EOFError
    STDERR.puts "#{$!} read_bytes closing"
    dead!
    nil
  end

  def try_once
    return unless live?
    @sender_mutex.synchronize { yield }
  rescue IOError, Errno::EPIPE, Errno::ECONNRESET, EOFError
    STDERR.puts "#{$!} try or close retry"
    dead!
  end

  def retry_forever
    sleep 0.1 until live?
    @sender_mutex.synchronize { yield }
  rescue IOError, Errno::EPIPE, Errno::ECONNRESET, EOFError
    STDERR.puts "#{$!} retry until open"
    dead!
    retry
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
