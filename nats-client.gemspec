# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

require_relative 'lib/nats_client/version'

Gem::Specification.new do |s|

  s.name = 'nats-client'
  s.version = NatsClient::VERSION
  s.summary = 'Simple NATS client for Ruby'

  s.author = 'Simon Russell'
  s.email = 'info@greensync.com.au'
  s.homepage = 'http://github.com/greensync/nats-client'

  s.add_dependency 'json', '~> 1'
  s.add_dependency 'concurrent-ruby', '~> 1.0'
  s.add_development_dependency 'rake'
  s.add_development_dependency 'rspec', '~> 3.0'

  s.required_ruby_version = '>= 1.9.3'

  s.files = Dir['lib/**/*.rb'] + ['LICENSE']

end
