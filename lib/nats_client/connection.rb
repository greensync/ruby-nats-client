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

  class ProtocolError < RuntimeError; end

  CONNECTION_INFO_TIMEOUT = 10
  CONNECTION_MAX_TIMEOUT = 365 * 24 * 60 * 60   # a year...

  def initialize(*connectors)
    @connectors = connectors
    @connector_index = 0

    @next_subscription_id = "A1"
    @subscriptions = {}

    reconnect!
  end

  def reconnect!
    puts "RECONNECT"
    @stream.close if @stream && !@stream.closed?

    @stream = @sender = @receiver = nil
    @server_info = nil

    @stream = next_connector!.open!
    @sender = NatsClient::Sender.new(@stream)
    @receiver = NatsClient::Receiver.new

    @sender.connect!({})

    run!(CONNECTION_INFO_TIMEOUT) do
      break if @server_info
    end

    @subscriptions.each do |subscription_id, subscription|
      @sender.sub!(subscription.topic_filter, subscription_id, subscription.options)
    end

  rescue Errno::EPIPE, Errno::ECONNREFUSED, EOFError, NatsClient::Connection::ProtocolError
    STDERR.puts "#{$!} retry"
    sleep 1
    retry
  end

  def publish!(topic, payload, options = {})
    retry_reconnect { @sender.pub!(topic, payload, options) }
  end

  def subscribe!(topic_filter, options = {}, &block)
    subscription_id = generate_subscription_id!

    @subscriptions[subscription_id] = Subscription.new(topic_filter, options, block)
    retry_reconnect { @sender.sub!(topic_filter, subscription_id, options) }

    subscription_id
  end

  def unsubscribe!(subscription_id)
    @subscriptions.delete(subscription_id)
    retry_reconnect { @sender.unsub!(subscription_id) }
  end

  def each(topic_filter)
    subscription_id = subscribe!(topic_filter)

    run! do |event, info|
      yield info.fetch(:topic), info.fetch(:payload) if event == :msg_received && info.fetch(:subscription_id) == subscription_id
    end
  ensure
    unsubscribe!(subscription_id) if subscription_id
  end

  def run!(timeout = CONNECTION_MAX_TIMEOUT)
    finish_time = timeout ? Time.now + timeout : nil

    loop do
      bytes = retry_reconnect { read_bytes(finish_time) }
      break unless bytes

      @receiver.parse!(bytes) do |event, info|
        case event
        when :info_received
          puts info
          @server_info = info
        when :msg_received
          notify_subscriptions(info)
        when :ping_received
          @sender.pong!
        when :protocol_error
          raise ProtocolError.new(info.fetch(:message))
        end

        yield event, info if block_given?
      end
    end
  end

  private

  def notify_subscriptions(message_info)
    subscription = @subscriptions.fetch(message_info.fetch(:subscription_id), nil)

    if subscription
      subscription.block.call(message_info.fetch(:topic), message_info.fetch(:payload)) if subscription.block
    else
      @sender.unsub!(message_info.fetch(:subscription_id))
    end
  end

  def read_bytes(finish_time)
    unless @stream.ready?
      timeout = finish_time ? finish_time - Time.now : 0
      @stream.wait(timeout) if timeout > 0
    end

    @stream.read_nonblock(NatsClient::Receiver::MAX_BUFFER)
  rescue IO::WaitReadable
    nil
  end

  def generate_subscription_id!
    subid = @next_subscription_id
    @next_subscription_id = @next_subscription_id.succ
    subid
  end

  def retry_reconnect
    return unless @stream

    yield
  rescue Errno::EPIPE, EOFError, NatsClient::Connection::ProtocolError
    STDERR.puts "#{$!} retry"
    sleep 1
    reconnect!
    retry
  end

  def next_connector!
    connector = @connectors[@connector_index]
    @connector_index = (@connector_index + 1) % @connectors.length
    connector
  end

end
