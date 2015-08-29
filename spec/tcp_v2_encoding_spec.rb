# coding: binary
require 'stagger/tcp_v2_encoding'

describe Stagger::TCPv2Encoding do
	data = [:report_all, 'Timestamp' => 1337]
	sample = Stagger::TCPv2Encoding.encode(*data)

	it 'can decode what it encoded' do
		method = :'report_all'
		params = {}
		buffer = subject.encode(method, params)
		b2, m2, p2 = subject.decode(buffer)

		expect(b2).to eq("")
		expect(m2).to eq(method)
		expect(p2).to eq(params)
	end

	describe "encoding" do
		it "complains if it doesn't know the method" do
			expect{ subject.encode(:woot, {})}.to raise_error(ArgumentError)
		end
	end

	describe "decoding" do
		it 'complains if the magic header is wrong' do
			sample1 = sample.dup
			sample1[1] = "z"

			expect{ subject.decode(sample1) }.to raise_error(Stagger::EncodingError)
		end

		it "complains if it doesn't know the method" do
			sample1 = sample.dup
			sample1[3] = "\xFF"
			expect{ subject.decode(sample1) }.to raise_error(ArgumentError)
		end

		it "doesn't happen if the message is smaller than the header" do
			sample1 = sample.byteslice(0, 6)
			expect(subject.decode(sample1)).to eq([sample1, nil, nil])
		end

		it "doesn't happen if the message is smaller than the body" do
			sample1 = sample.byteslice(0..-2)
			expect(subject.decode(sample1)).to eq([sample1, nil, nil])
		end

		it "returns the remaining data after decoding" do
			sample1 = sample + "WOOT"
			expect(subject.decode(sample1)).to eq(["WOOT", :report_all, {'Timestamp' => 1337}])
		end
	end
end