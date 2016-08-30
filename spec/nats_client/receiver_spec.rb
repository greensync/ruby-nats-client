# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

require 'spec_helper'

describe NatsClient::Receiver do

  let(:receiver) { NatsClient::Receiver.new }

  describe "simple" do

    subject do
      events = []

      input.each do |bytes|
        receiver.parse!(bytes) do |event, info|
          events << [event, info]
        end
      end

      events
    end

    {
      [%{INFO {"server_id":"billy bob"}\r\n}] =>
        [[:info_received, {"server_id" => "billy bob"}]],

      ["MSG topic subid 5\r\nhello\r\n"] =>
        [
          [:msg_started, { topic: "topic", subscription_id: "subid", payload_length: 5 }],
          [:msg_received, { topic: "topic", subscription_id: "subid", payload_length: 5, payload: "hello" }]
        ],
      ["MSG", " topic subid 5\r\nhel", "lo\r\n"] =>
        [
          [:msg_started, { topic: "topic", subscription_id: "subid", payload_length: 5 }],
          [:msg_received, { topic: "topic", subscription_id: "subid", payload_length: 5, payload: "hello" }]
        ],
      ["MSG topic subid 5\r\nhello\r\nMSG topic2 subid2 7\r\ngoodbye\r\n"] =>
        [
          [:msg_started, { topic: "topic", subscription_id: "subid", payload_length: 5 }],
          [:msg_received, { topic: "topic", subscription_id: "subid", payload_length: 5, payload: "hello" }],
          [:msg_started, { topic: "topic2", subscription_id: "subid2", payload_length: 7 }],
          [:msg_received, { topic: "topic2", subscription_id: "subid2", payload_length: 7, payload: "goodbye" }]
        ],
      ["MSG topic subid", " 5\r\nhello\r", "\nMSG topic2 subid2 ", "7\r\ngoodbye\r\n"] =>
        [
          [:msg_started, { topic: "topic", subscription_id: "subid", payload_length: 5 }],
          [:msg_received, { topic: "topic", subscription_id: "subid", payload_length: 5, payload: "hello" }],
          [:msg_started, { topic: "topic2", subscription_id: "subid2", payload_length: 7 }],
          [:msg_received, { topic: "topic2", subscription_id: "subid2", payload_length: 7, payload: "goodbye" }]
        ],


      ["PING\r\n"] =>
        [[:ping_received, {}]],
      ["PONG\r\n"] =>
        [[:pong_received, {}]],

      ["+OK\r\n"] =>
        [[:ok_received, {}]],
      ["-ERR 'fishy'\r\n"] =>
        [[:err_received, { message: 'fishy' }]],

      ["PING"] =>
        [],
      ["PO", "NG\r\n"] =>
        [[:pong_received, {}]],
      ["PING\r\nPONG\r\n"] =>
        [
          [:ping_received, {}],
          [:pong_received, {}]
        ],
    }.each do |input, output|
      context "with #{input.inspect}" do
        let(:input) { input }

        it "should convert to #{output.inspect}" do
          expect(subject).to eq output
        end
      end
    end

  end

end
