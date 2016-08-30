# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

module NatsClient; end

require 'io/wait'

require 'json'

require_relative 'nats_client/version'
require_relative 'nats_client/sender'
require_relative 'nats_client/receiver'

require_relative 'nats_client/connection'
