# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

require 'spec_helper'

describe NatsClient::Receiver do

  let(:handler) { NatsClient::RecordingHandler.new }
  let(:receiver) { NatsClient::Receiver.new(handler) }

  describe "simple" do

    subject { input.each { |i| receiver << i }; handler.handled }

    context "PING" do
      let(:input) { ["PING\r\n"] }

      it "should trigger a ping" do
        expect(subject).to eq [
          [:ping_received!]
        ]
      end
    end

    context "PONG" do
      let(:input) { ["PONG\r\n"] }

      it "should trigger a pong" do
        expect(subject).to eq [
          [:pong_received!]
        ]
      end
    end

  end

end
