# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

class NatsClient::Connection

  CONNECTION_RETRY_INTERVAL = 1
  OPERATION_RETRY_INTERVAL = 0.2

  def initialize(connector)
    @connector = connector
    @subscriptions = NatsClient::SubscriptionManager.new

    @conn = Concurrent::AtomicReference.new(NatsClient::ServerConnection.empty)
    @server_info = nil
  end

  def publish!(topic, payload, options = {})
    retry_forever { current_conn.pub!(topic, payload, options) }
  end

  def subscribe!(topic_filter, options = {}, &block)
    subscription_id = @subscriptions.add!(topic_filter, options, &block)
    retry_forever { current_conn.sub!(topic_filter, subscription_id, options) }
    subscription_id
  end

  def unsubscribe!(subscription_id)
    @subscriptions.remove!(subscription_id)
    try_once { current_conn.unsub!(subscription_id) }
  end

  def request!(topic, request_payload)
    # TODO some sort of expiry on the subscription?
    reply_topic = "INBOX.#{Thread.current.object_id.to_s(36)}.#{SecureRandom.hex(4)}"

    subscription_id = @subscriptions.add!(reply_topic) do |topic, response_payload|
      unsubscribe!(subscription_id)
      yield(response_payload)
    end

    retry_forever do
      current_conn.batch do |c|
        c.sub!(reply_topic, subscription_id)
        c.pub!(topic, request_payload, reply_to: reply_topic)
      end
    end

    subscription_id
  end

  def live?
    current_conn.live?
  end

  def run!
    loop do
      reconnect! unless live?

      current_conn.parse! do |event, info|
        case event
        when :info_received
          @server_info = info
        when :msg_received
          try_once { current_conn.unsub!(info.fetch(:subscription_id)) } unless @subscriptions.notify!(info)
        when :ping_received
          try_once { current_conn.pong! }
        when :protocol_error, :connection_dead
          STDERR.puts "Protocol Error: #{info.fetch(:message)}"
        end
      end
    end

  ensure
    current_conn.close!
  end

  private

  def current_conn
    @conn.get
  end

  def reconnect!
    current_conn.close!

    new_connection = NatsClient::ServerConnection.new(connect_until_connected)
    new_connection.batch do |c|
      c.connect!({})
      c.multi_sub!(@subscriptions)
    end

    # raise NatsClient::ServerConnection::ConnectionDead.new

    @conn.set(new_connection)

  rescue NatsClient::ServerConnection::ConnectionDead
    new_connection.close! if new_connection
    sleep CONNECTION_RETRY_INTERVAL
    retry
  end

  def connect_until_connected
    loop do
      new_socket = @connector.open!
      return new_socket if new_socket
      sleep CONNECTION_RETRY_INTERVAL
    end
  end

  def try_once
    yield
  rescue NatsClient::ServerConnection::ConnectionDead
    # oh well, we tried
  end

  def retry_forever
    yield
  rescue NatsClient::ServerConnection::ConnectionDead
    sleep OPERATION_RETRY_INTERVAL until live?
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
  #   future = Concurrent::IVar.new
  #   reply_topic = "INBOX.#{Thread.current.object_id.to_s(36)}.#{SecureRandom.hex(4)}"
  #   subscription_id = @connection.subscribe!(reply_topic) { |topic, payload| future.try_set(payload) }
  #
  #   @connection.publish!(topic, request_payload, reply_to: reply_topic)
  #
  #   future.value
  # ensure
  #   @connection.unsubscribe!(subscription_id) if subscription_id

    future = Concurrent::IVar.new
    @connection.request!(topic, request_payload) { |response_payload| future.try_set(response_payload) }
    future.value
  end

end
