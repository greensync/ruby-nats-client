# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

require 'simplecov'
SimpleCov.start do
  add_filter '/spec/'
end

require_relative '../lib/nats-client'
