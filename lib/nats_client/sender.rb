# encoding: us-ascii
# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

class NatsClient::Sender

  class InvalidTopicError < RuntimeError; end
  class InvalidNameError < RuntimeError; end
  class InvalidPayloadEncodingError < RuntimeError; end
  class InvalidNumberError < RuntimeError; end

  def initialize(stream)
    @stream = stream
  end

  CONNECT_SPACE = "CONNECT ".freeze
  PUB_SPACE = "PUB ".freeze
  SUB_SPACE = "SUB ".freeze
  UNSUB_SPACE = "UNSUB ".freeze
  PING_CR_LF = "PING\r\n".freeze
  PONG_CR_LF = "PONG\r\n".freeze

  SPACE = " ".freeze
  CR_LF = "\r\n".freeze

  DEFAULT_CONNECT_OPTIONS = {
    verbose: false,
    pedantic: true,
    lang: "ruby",
    version: NatsClient::VERSION,
    protocol: NatsClient::PROTOCOL_VERSION
  }.freeze

  VALID_NAME = /\A[a-z\d]+\z/i.freeze
  VALID_TOPIC = /\A(?:(?:[a-z\d]+|\*)\.)*(?:[a-z\d]+|\*|\>)\z/i.freeze

  def connect!(info)
    @stream << CONNECT_SPACE << JSON.generate(DEFAULT_CONNECT_OPTIONS.merge(info)) << CR_LF

    self
  end

  def pub!(topic, payload, options = {})
    reply_topic = options.fetch(:reply_to, nil)

    validate_topic!(topic)
    validate_topic!(reply_topic) if reply_topic
    validate_payload!(payload)

    @stream << PUB_SPACE << topic << SPACE
    @stream << reply_topic << SPACE if reply_topic
    @stream << payload.bytesize << CR_LF << payload << CR_LF

    self
  end

  def sub!(topic, subscription_id, options = {})
    queue_group = options.fetch(:queue_group, nil)

    validate_topic!(topic)
    validate_name!(subscription_id)
    validate_name!(queue_group) if queue_group

    @stream << SUB_SPACE << topic << SPACE
    @stream << queue_group << SPACE if queue_group
    @stream << subscription_id << CR_LF

    self
  end

  def unsub!(subscription_id, options = {})
    max_msgs = options.fetch(:max_msgs, nil)

    validate_name!(subscription_id)
    validate_integer!(max_msgs) if max_msgs

    @stream << UNSUB_SPACE << subscription_id
    @stream << SPACE << max_msgs if max_msgs
    @stream << CR_LF

    self
  end

  def ping!
    @stream << PING_CR_LF
  end

  def pong!
    @stream << PONG_CR_LF
  end

  private

  def validate_topic!(topic)
    raise InvalidTopicError.new(topic) unless topic =~ VALID_TOPIC
  end

  def validate_name!(name)
    raise InvalidNameError.new(name) unless name =~ VALID_NAME
  end

  def validate_payload!(payload)
    # raise InvalidPayloadEncodingError.new(payload.encoding.name) unless payload.encoding == @stream.external_encoding
  end

  def validate_integer!(number)
    raise InvalidNumberError.new(number.to_s) unless number.is_a?(Integer)
  end

end
