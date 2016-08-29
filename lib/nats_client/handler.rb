# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

module NatsClient::Handler

  def info_received!
  end

  def msg_started!
  end

  def msg_received!
  end

  def ping_received!
  end

  def pong_received!
  end

  def ok_received!
  end

  def err_received!
  end

  def cleanly_closed!
  end

  # protocol error; close the connection
  def protocol_error!(message)
  end

end
