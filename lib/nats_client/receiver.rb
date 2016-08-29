# encoding: us-ascii
# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

class NatsClient::Receiver

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

  OK_RESPONSE = '+OK'.freeze
  ERR_RESPONSE = '-ERR'.freeze

  COMMANDS = {
    INFO_COMMAND => /\A(\{.+\})\z/,
    PING_COMMAND => nil,
    PONG_COMMAND => nil,
    OK_RESPONSE => nil,
    ERR_RESPONSE => /\A'([^']+)'\z/
  }

  COMMAND_REGEX = /\A(#{COMMANDS.keys.map { |c| Regexp.escape(c) }.join('|')})(?:[ \t]+(.+)|)\r\n\z/o

  def initialize(handler)
    @buffer = "".force_encoding(Encoding::ASCII_8BIT)
    @mode = COMMAND_MODE
    @handler = handler
  end

  def closed?
    @mode == CLOSED_MODE
  end

  def close!
    raise "ASDFASDF"
  end

  def <<(bytes)
    case @mode
    when COMMAND_MODE
      if bytes.length + @buffer.length > MAX_COMMAND
        protocol_error!("Command length exceeded #{MAX_COMMAND}")
      else
        @buffer << bytes
        process_command! while @buffer.index(CR_LF)
      end
    when CLOSED_MODE
      raise InvalidModeError.new(@mode.to_s)
    else
      raise "Invalid mode for bytes #{@mode}"
    end
  end

  private

  EMPTY_ARGS = [].freeze

  def parse_args(command, args)
    args_regex = COMMANDS.fetch(command)

    return EMPTY_ARGS if args.nil? && args_regex.nil?

    args_regex.match(args).to_a.drop(1)
  end

  def process_command!
    line = @buffer.slice!(0, @buffer.index(CR_LF) + CR_LF.length)
    return protocol_error!("Invalid command line #{line.inspect}") unless line =~ COMMAND_REGEX

    command = $1

    args = parse_args(command, $2)
    return protocol_error!("Invalid arguments for #{command}: #{args.inspect}") unless args

    case command
    when INFO_COMMAND
      @handler.info_received!(JSON.parse(args[0]))
    when PING_COMMAND
      @handler.ping_received!
    when PONG_COMMAND
      @handler.pong_received!
    when OK_RESPONSE
      @handler.ok_received!
    when ERR_RESPONSE
      @handler.err_received!(args[0])
    else
      raise NotImplementedError.new(command)
    end
  end

  def protocol_error!(message)
    @mode = CLOSED_MODE
    @handler.protocol_error!(message)
  end

end
