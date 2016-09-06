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

class NatsClient::Connection

  class Subscription
    attr_reader :topic_filter, :options, :block

    def initialize(topic_filter, options, block)
      @topic_filter = topic_filter
      @options = options
      @block = block
      freeze
    end
  end

  CONNECTION_MAX_TIMEOUT = 365 * 24 * 60 * 60   # a year...

  def initialize(*connectors)
    @connectors = connectors
    @connector_index = 0

    @next_subscription_id = "A1"
    @subscriptions = {}

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

      @stream = next_connector!.open!
      @sender = NatsClient::Sender.new(@stream)
      @receiver.reset!
    end

    try_or_close do
      @sender.connect!({})

      @subscriptions.each do |subscription_id, subscription|
        @sender.sub!(subscription.topic_filter, subscription_id, subscription.options)
      end
    end
  end

  def publish!(topic, payload, options = {})
    retry_until_open { @sender.pub!(topic, payload, options) }
  end

  def subscribe!(topic_filter, options = {}, &block)
    subscription_id = generate_subscription_id!

    @subscriptions[subscription_id] = Subscription.new(topic_filter, options, block)
    retry_until_open { @sender.sub!(topic_filter, subscription_id, options) }

    subscription_id
  end

  def unsubscribe!(subscription_id)
    @subscriptions.delete(subscription_id)
    retry_until_open { @sender.unsub!(subscription_id) }
  end

  def run!
    loop do
      bytes = read_bytes
      next unless bytes

      @receiver.parse!(bytes) do |event, info|
        case event
        when :info_received
          puts info
          @server_info = info
        when :msg_received
          notify_subscriptions(info)
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

  def notify_subscriptions(message_info)
    subscription = @subscriptions.fetch(message_info.fetch(:subscription_id), nil)

    if subscription
      subscription.block.call(message_info.fetch(:topic), message_info.fetch(:payload), message_info.fetch(:reply_to, nil)) if subscription.block
    else
      @sender.unsub!(message_info.fetch(:subscription_id))
    end
  end

  def read_bytes
    connect! if closed?

    unless @stream.ready?
      wait_readable(@stream)
    end

    @stream.read_nonblock(NatsClient::Receiver::MAX_BUFFER)

  rescue Errno::EPIPE, Errno::ECONNRESET, Errno::ECONNREFUSED, EOFError
    STDERR.puts "#{$!} read_bytes closing"
    close!
    sleep 1
    retry

  rescue IO::WaitReadable
    sleep 0.1
    retry
  end

  def generate_subscription_id!
    subid = @next_subscription_id
    @next_subscription_id = @next_subscription_id.succ
    subid
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

  def next_connector!
    connector = @connectors[@connector_index]
    @connector_index = (@connector_index + 1) % @connectors.length
    connector
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
    queue = Queue.new
    reply_topic = "INBOX.#{Thread.current.object_id}.#{SecureRandom.hex(13)}"
    subscription_id = @connection.subscribe!(reply_topic) { |topic, payload| queue << [topic, payload] unless queue.size > 1 }

    @connection.publish!(topic, request_payload, reply_to: reply_topic)

    topic, response_payload = queue.pop
    response_payload
  ensure
    @connection.unsubscribe!(subscription_id) if subscription_id
  end

end
