# Copyright (c) 2016 GreenSync Pty Ltd.  All rights reserved.

require 'spec_helper'

describe NatsClient::PooledConnector do

  let(:connectors) { [] }
  let(:connector) { NatsClient::PooledConnector.new(connectors) }

  describe "open!" do
    subject { connector.open! }

    context "no connectors" do
      let(:connectors) { [] }

      it "returns nil" do
        expect(subject).to be_nil
      end
    end

    context "one working connector" do
      let(:socket) { double("open socket") }
      let(:connectors) { [double(:open! => socket)] }

      it "returns connector result" do
        expect(subject).to eq(socket)
      end
    end

    context "one non-working connector" do
      let(:connectors) { [double(:open! => nil)] }

      it "returns connector result" do
        expect(subject).to be_nil
      end
    end

    context "two connectors, both working" do
      let(:socket) { double("open socket") }
      let(:connectors) { [double(:open! => socket), double(:open! => socket)] }

      it "returns connector result" do
        expect(subject).to eq(socket)
      end
    end

    context "two connectors, none working" do
      let(:connectors) { [double(:open! => nil), double(:open! => nil)] }

      it "returns connector result" do
        expect(subject).to be_nil
      end
    end

    context "two connectors, one working" do
      let(:socket) { double("open socket") }
      let(:connectors) { [double(:open! => socket), double(:open! => nil)] }

      it "returns connector result" do
        expect(subject).to eq(socket)
      end
    end

    context "five connectors, one working" do
      let(:socket) { double("open socket") }
      let(:connectors) { [double(:open! => socket), double(:open! => nil), double(:open! => nil), double(:open! => nil), double(:open! => nil)] }

      it "returns connector result" do
        expect(subject).to eq(socket)
      end
    end

    context "randomized" do
      let(:connectors) do
        (1..5).map do |i|
          double("connector", :open! => double("socket #{i}"))
        end
      end

      it "returns random results" do
        # TODO possibly a better test here.  Can still technically fail!
        expect((1..100).map { connector.open! }.uniq.length).to be > 1
      end
    end

  end

end
