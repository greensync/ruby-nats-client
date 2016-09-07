# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

require 'spec_helper'

describe NatsClient::Sender do

  let(:stream) { StringIO.new("") }
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

  describe "#pub!" do

    let(:topic) { "FISHY.TIMES" }
    let(:payload) { "MY FISHY MESSAGE" }
    let(:options) { {} }

    subject { sender.pub!(topic, payload, options); stream.string }

    context "with no options" do

      let(:options) { {} }

      it "should format correctly" do
        expect(subject).to eq("PUB #{topic} #{payload.bytesize}\r\n#{payload}\r\n")
      end

    end

    context "with reply topic" do

      let(:options) { { reply_to: "REPLY.BOX" } }

      it "should format correctly" do
        expect(subject).to eq("PUB #{topic} REPLY.BOX #{payload.bytesize}\r\n#{payload}\r\n")
      end

    end

    context "with invalid topic" do

      let(:topic) { "INVALID!TOPIC is fun" }

      it "should format correctly" do
        expect { subject }.to raise_error(NatsClient::Sender::InvalidTopicError, topic)
      end

    end

    context "with invalid reply topic" do

      let(:options) { { reply_to: "INVALID!TOPIC is fun" } }

      it "should format correctly" do
        expect { subject }.to raise_error(NatsClient::Sender::InvalidTopicError, "INVALID!TOPIC is fun")
      end

    end

    context "with UTF-8 payload" do

      let(:payload) { "\u{1f4a9}" }

      it "should use the byte size" do
        expect(subject).to eq("PUB #{topic} 4\r\n#{payload}\r\n")
      end

    end

    # context "with ASCII-8BIT payload" do
    #
    #   let(:payload) { "\u{1f4a9}".force_encoding('ascii-8bit') }
    #
    #   it "should use the byte size" do
    #     expect { subject }.to raise_error(NatsClient::Sender::InvalidPayloadEncodingError, payload.encoding.name)
    #   end
    #
    # end

  end

  describe "sub!" do

    let(:topic) { "BOBBY.TABLES" }
    let(:subscription_id) { "XYZ123" }
    let(:options) { {} }

    subject { sender.sub!(topic, subscription_id, options); stream.string }

    context "with no options" do

      let(:options) { {} }

      it "should format correctly" do
        expect(subject).to eq("SUB #{topic} #{subscription_id}\r\n")
      end

    end

    context "with queue group" do

      let(:options) { { queue_group: "QU34" } }

      it "should format correctly" do
        expect(subject).to eq("SUB #{topic} QU34 #{subscription_id}\r\n")
      end

    end

    context "with invalid topic" do

      let(:topic) { "INVALID!TOPIC is fun" }

      it "should format correctly" do
        expect { subject }.to raise_error(NatsClient::Sender::InvalidTopicError, topic)
      end

    end

    context "with invalid subscription_id" do

      let(:subscription_id) { "INVALID!TOPIC is fun" }

      it "should format correctly" do
        expect { subject }.to raise_error(NatsClient::Sender::InvalidNameError, subscription_id)
      end

    end

    context "with invalid queue group" do

      let(:options) { { queue_group: "$$$" } }

      it "should format correctly" do
        expect { subject }.to raise_error(NatsClient::Sender::InvalidNameError, "$$$")
      end

    end

  end

  describe "unsub!" do

    let(:subscription_id) { "ABC456" }
    let(:options) { {} }

    subject { sender.unsub!(subscription_id, options); stream.string }

    context "with no options" do

      it "should format correctly" do
        expect(subject).to eq("UNSUB ABC456\r\n")
      end

    end

    context "with max_msgs" do

      let(:options) { { max_msgs: 345 } }

      it "should format correctly" do
        expect(subject).to eq("UNSUB ABC456 345\r\n")
      end

    end

    context "with invalid subscription id" do

      let(:subscription_id) { '!!!!' }

      it "should raise error" do
        expect { subject }.to raise_error(NatsClient::Sender::InvalidNameError, '!!!!')
      end

    end

    context "with invalid max msgs" do

      let(:options) { { max_msgs: '!!!!' } }

      it "should raise error" do
        expect { subject }.to raise_error(NatsClient::Sender::InvalidNumberError, '!!!!')
      end

    end

  end

  describe "ping!" do
    subject { sender.ping!; stream.string }

    it "should work" do
      expect(subject).to eq("PING\r\n")
    end
  end

  describe "pong!" do
    subject { sender.pong!; stream.string }

    it "should work" do
      expect(subject).to eq("PONG\r\n")
    end
  end

end
