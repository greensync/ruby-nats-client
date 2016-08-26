# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

require 'spec_helper'

describe NatsClient::Sender do

  let(:stream) { StringIO.new("".force_encoding('ASCII-8BIT')) }
  let(:sender) { NatsClient::Sender.new(stream) }

  describe "#connect!" do
    subject { sender.connect!(info); stream.string }

    context "with no info" do
      let(:info) { {} }

      it "should have defaults" do
        expect(subject).to eq(%{CONNECT {"verbose":false,"pedantic":true,"lang":"ruby","version":"#{NatsClient::VERSION}","protocol":1}\r\n})
      end
    end

    context "with custom info" do
      let(:info) { { name: "billy-bob", verbose: true} }

      it "should merge and override defaults" do
        expect(subject).to eq(%{CONNECT {"verbose":true,"pedantic":true,"lang":"ruby","version":"#{NatsClient::VERSION}","protocol":1,"name":"billy-bob"}\r\n})
      end
    end

  end

end
