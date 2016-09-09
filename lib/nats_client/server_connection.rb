# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

class NatsClient::ServerConnection

  class ConnectionDead < RuntimeError; end

  def initialize(stream)
    @stream = stream
    @receiver = NatsClient::Receiver.new
    @sender = NatsClient::Sender.new(@stream)
    @sender_mutex = Mutex.new
    @live = Concurrent::AtomicBoolean.new(true)
  end

  def live?
    @live.true?
  end

  %w(pub! sub! multi_sub! unsub! connect! pong!).each do |method_name|
    eval "def #{method_name}(*args); sync_protect { @sender.#{method_name}(*args) }; self; end"
  end

  def batch
    sync_protect { yield @sender }
    self
  end

  # not thread safe
  def parse!(&block)
    dead!($!, &block) unless live?

    bytes = @stream.readpartial(NatsClient::Receiver::MAX_BUFFER)
    @receiver.parse!(bytes, &block)

  rescue IOError, Errno::EPIPE, Errno::ECONNRESET, EOFError
    dead!($!, &block)
  end

  def close!
    @live.make_false

    unless @stream.closed?
      (@stream.shutdown rescue nil) if @stream.respond_to?(:shutdown)
      @stream.close
    end
  end

  def self.empty
    result = new(StringIO.new)
    result.close!
    result
  end

  private

  def dead!(message = "connection dead")
    @live.make_false

    if block_given?
      yield :connection_dead, { message: message }
    else
      raise ConnectionDead.new(message)
    end
  end

  def sync_protect
    dead! unless live?

    @sender_mutex.synchronize { yield }

  rescue IOError, Errno::EPIPE, Errno::ECONNRESET, EOFError
    dead!($!)
  end

end
