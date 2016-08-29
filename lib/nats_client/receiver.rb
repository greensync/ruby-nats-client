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

  COMMAND_REGEX = /\A(PING|PONG)\r\n\z/

  PING_COMMAND = 'PING'.freeze
  PONG_COMMAND = 'PONG'.freeze

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
        process_command! if bytes.index(CR_LF)
      end
    when CLOSED_MODE
      raise InvalidModeError.new(@mode.to_s)
    else
      raise "Invalid mode for bytes #{@mode}"
    end
  end

  private

  def process_command!
    line = @buffer.slice!(0, @buffer.index(CR_LF) + CR_LF.length)
    protocol_error!("Invalid command line #{line.inspect}") unless line =~ COMMAND_REGEX

    command = $1

    case command
    when PING_COMMAND
      @handler.ping_received!
    when PONG_COMMAND
      @handler.pong_received!
    else
      protocol_error!("Invalid command #{command}")
    end
  end

  def protocol_error!(message)
    @mode = CLOSED_MODE
    @handler.protocol_error!(message)
  end

end
