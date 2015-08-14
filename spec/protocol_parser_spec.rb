# coding: binary
require 'stagger/protocol_parser'

describe Stagger::ProtocolParser do
  sample1 = "5,16.hello:\x81\xA9Timestamp\xCEU\xCC\xBC\x05"

  broken_sample1 = "5,16.hello-\x81\xA9Timestamp\xCEU\xCC\xBC\x05"
  broken_sample2 = "x" * 200

  before do
    @commands = []
    @errors = []
    subject.on(:command) { |*a| @commands << a }
    subject.on(:error) { |*a| @errors << a }
  end

  it "works" do
    subject.feed(sample1)
    expect(@commands).to eq([
      ["hello", {"Timestamp"=>1439480837}]
    ])
    expect(@errors).to eq([])
    expect(subject.state).to eq(:init)
    expect(subject.buffer).to eq("")
  end

  it "works twice" do
    subject.feed(sample1)
    subject.feed(sample1)
    expect(@commands).to eq([
      ["hello", {"Timestamp"=>1439480837}],
      ["hello", {"Timestamp"=>1439480837}]
    ])
    expect(@errors).to eq([])
    expect(subject.state).to eq(:init)
    expect(subject.buffer).to eq("")
  end

  it "can be fed byte by byte" do
    sample1.each_char do |c|
      expect(@commands).to eq([])
      subject.feed(c)
    end

    expect(subject.buffer).to eq("")
    expect(@commands).to eq([
      ["hello", {"Timestamp"=>1439480837}]
    ])
    expect(@errors).to eq([])
    expect(subject.state).to eq(:init)
    expect(subject.buffer).to eq("")
  end

  it "complains if the separator is wrong" do
    subject.feed(broken_sample1)
    expect(@commands).to eq([])
    expect(@errors).to eq([["Invalid separator - != :"]])
  end

  it "complains if it doesn't find the init in the first 200 bytes of data" do
    subject.feed(broken_sample2)
    expect(@commands).to eq([])
    expect(@errors).to eq([])
    subject.feed("x")
    expect(@commands).to eq([])
    expect(@errors).to eq([["Init not found in the first 200 bytes"]])
  end
end
