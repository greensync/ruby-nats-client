# encoding: us-ascii
# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

class NatsClient::Receiver

  class InvalidModeError < StandardError; end

  MAX_BUFFER = 1024*1024
  MAX_COMMAND = 8192

  COMMAND_MODE = :command
  PAYLOAD_MODE = :payload
  PAYLOAD_SKIP_MODE = :payload_skip
  CLOSED_MODE = :closed

  CR_LF = "\r\n".freeze

  INFO_COMMAND = 'INFO'.freeze
  PING_COMMAND = 'PING'.freeze
  PONG_COMMAND = 'PONG'.freeze
  MSG_COMMAND = 'MSG'.freeze

  OK_RESPONSE = '+OK'.freeze
  ERR_RESPONSE = '-ERR'.freeze

  COMMANDS = {
    INFO_COMMAND => /\A(\{.+\})\z/,
    MSG_COMMAND => /\A([a-z\d\.]+)\s+([a-z\d]+)\s+(\d+)\z/i,     # TODO more strict
    PING_COMMAND => nil,
    PONG_COMMAND => nil,
    OK_RESPONSE => nil,
    ERR_RESPONSE => /\A'([^']+)'\z/
  }

  COMMAND_REGEX = /\A(#{COMMANDS.keys.map { |c| Regexp.escape(c) }.join('|')})(?:[ \t]+(.+)|)\r\n\z/o

  def initialize
    reset!
  end

  def reset!
    @buffer = "".force_encoding(Encoding::ASCII_8BIT)
    @mode = COMMAND_MODE

    @current_message = nil
  end

  def closed?
    @mode == CLOSED_MODE
  end

  def close!
    @mode = CLOSED_MODE
  end

  def parse!(bytes, &block)
    return protocol_error!("Buffer length exceeded #{MAX_BUFFER}", &block) unless bytes.length + @buffer.length <= MAX_BUFFER

    @buffer << bytes

    loop do
      case @mode
      when COMMAND_MODE
        break unless @buffer.index(CR_LF)
        process_command!(&block)
      when PAYLOAD_MODE
        break unless @buffer.length >= @current_message.fetch(:payload_length) + 2
        process_payload!(&block)
      else
        raise InvalidModeError.new(@mode.to_s)
      end
    end
  end

  private

  def bytes_allowed?(length)
    case @mode
    when COMMAND_MODE
      length + @buffer.length <= MAX_COMMAND
    when CLOSED_MODE
      false
    end
  end

  EMPTY_ARGS = [].freeze
  EMPTY_HASH = {}.freeze

  def parse_args(command, args)
    args_regex = COMMANDS.fetch(command)

    return EMPTY_ARGS if args.nil? && args_regex.nil?

    args.strip!
    match = args_regex.match(args)
    return nil unless match

    match.to_a.drop(1)
  end

  def process_command!(&block)
    line = @buffer.slice!(0, @buffer.index(CR_LF) + CR_LF.length)
    return protocol_error!("Invalid command line #{line.inspect}", &block) unless line =~ COMMAND_REGEX

    command = $1

    args = parse_args(command, $2)
    return protocol_error!("Invalid arguments in line #{line.inspect}", &block) unless args

    case command
    when MSG_COMMAND
      info = { topic: args[0], subscription_id: args[1], payload_length: args[2].to_i }
      yield :msg_started, info

      start_payload!(info)

    when INFO_COMMAND
      info = JSON.parse(args[0])
      yield :info_received, info

    when PING_COMMAND
      yield :ping_received, EMPTY_HASH

    when PONG_COMMAND
      yield :pong_received, EMPTY_HASH

    when OK_RESPONSE
      yield :ok_received, EMPTY_HASH

    when ERR_RESPONSE
      yield :err_received, { message: args[0] }

    else
      raise NotImplementedError.new(command)
    end
  end

  def start_payload!(info)
    @current_message = info
    @mode = PAYLOAD_MODE
  end

  def process_payload!(&block)
    payload_length = @current_message.fetch(:payload_length)
    return protocol_error!("Payload not followed by CRLF", &block) unless @buffer[payload_length, 2] == CR_LF

    message = @current_message
    payload = @buffer.slice!(0, payload_length)
    @buffer.slice!(0, 2)

    @current_message = nil
    @mode = COMMAND_MODE

    yield :msg_received, message.merge(payload: payload)
  end

  def protocol_error!(message)
    @mode = CLOSED_MODE
    yield :protocol_error, { message: message }
  end

end
