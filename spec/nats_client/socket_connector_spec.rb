# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

require 'spec_helper'

describe NatsClient::SocketConnector do

  let(:host) { '127.0.0.1' }
  let(:port) { 26123 }    # let's hope there's nothing running there...

  let(:connector) { NatsClient::SocketConnector.new(host, port) }

  describe "attributes" do
    it "should expose host" do
      expect(connector.host).to eq(host)
    end

    it "should expose port" do
      expect(connector.port).to eq(port)
    end

    it "should be frozen" do
      expect(connector).to be_frozen
    end
  end

  describe "open!" do
    subject { connector.open! }

    context "on success" do
      before do
        @server = TCPServer.new '127.0.0.1', port
      end

      after do
        @server.close
      end

      it "connects via tcp" do
        expect(subject).to be_a(TCPSocket)
        expect(subject.peeraddr).to eq(["AF_INET", 26123, "127.0.0.1", "127.0.0.1"])
        expect(subject.closed?).to be_falsy
      end
    end

    context "on failure" do
      it "returns nil" do
        expect(subject).to be_nil
      end
    end
  end

end
