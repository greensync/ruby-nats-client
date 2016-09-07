# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

class NatsClient::SubscriptionManager

  class Subscription
    attr_reader :topic_filter, :options, :block

    def initialize(topic_filter, options, block)
      @topic_filter = topic_filter
      @options = options
      @block = block
      freeze
    end
  end

  def initialize
    @next_subscription_id = Concurrent::AtomicFixnum.new(0)
    @subscriptions = Concurrent::Map.new
  end

  def add!(topic_filter, options, block)
    subscription_id = generate_subscription_id!
    @subscriptions[subscription_id] = Subscription.new(topic_filter, options, block)
    subscription_id
  end

  def remove!(subscription_id)
    @subscriptions.delete(subscription_id)
  end

  def notify!(message_info)
    subscription = @subscriptions.fetch(message_info.fetch(:subscription_id), nil)

    if subscription
      subscription.block.call(message_info.fetch(:topic), message_info.fetch(:payload), message_info.fetch(:reply_to, nil)) if subscription.block
      true
    else
      false
    end

    # TODO rescue exceptions
  end

  def each
    @subscriptions.each_pair do |subscription_id, subscription|
      yield subscription.topic_filter, subscription_id, subscription.options
    end
  end

  private

  def generate_subscription_id!
    @next_subscription_id.increment.to_s(36)
  end

end
