# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

class NatsClient::RecordingHandler
  include NatsClient::Handler

  attr_reader :handled

  def initialize
    @handled = []
  end

  def clear
    @handled.clear
  end

  %w(
    info_received!
    msg_started!
    msg_received!
    ping_received!
    pong_received!
    ok_received!
    err_received!
    cleanly_closed!
    protocol_error!
  ).each do |method|
    eval "def #{method}(*args)\nrecord!(:#{method}, args)\nend"
  end

  private

  def record!(message, args)
    @handled << [message, *args]
  end

end
