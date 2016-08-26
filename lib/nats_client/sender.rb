# encoding: us-ascii
# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

class NatsClient::Sender

  def initialize(stream)
    @stream = stream
  end

  CONNECT_SPACE = "CONNECT ".freeze
  CR_LF = "\r\n".freeze

  DEFAULT_CONNECT_OPTIONS = {
    verbose: false,
    pedantic: true,
    lang: "ruby",
    version: NatsClient::VERSION,
    protocol: NatsClient::PROTOCOL_VERSION
  }.freeze

  def connect!(info)
    @stream << CONNECT_SPACE << JSON.generate(DEFAULT_CONNECT_OPTIONS.merge(info)) << CR_LF
  end

  private

end
