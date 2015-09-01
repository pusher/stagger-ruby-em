# -*- coding: binary -*-
require 'msgpack'

module Stagger
  class EncodingError < StandardError; end
  class TCPv2EncodingImpl
    MAGIC_BYTE1 = "\x83"
    MAGIC_BYTE2 = "\x84"
    MAGIC_VERSION = "\x00"
    MAGIC_HEADER = [MAGIC_BYTE1, MAGIC_BYTE2, MAGIC_VERSION].join

    def initialize(method_to_byte)
      @method_to_byte = method_to_byte.freeze
      @byte_to_method = method_to_byte.each_with_object({}) do |(method, byte), hash|
        hash[byte] = method
      end.freeze
      freeze
    end

    def decode(buffer) #=> new_buffer, method, params
      return nil, nil, nil if buffer.nil?
      size = buffer.bytesize
      return buffer, nil, nil if size < 8
      body_size = buffer.byteslice(4, 4).unpack('N').first
      return buffer, nil, nil if size < (8 + body_size)

      if !buffer.start_with?(MAGIC_HEADER)
        fail(EncodingError, "Unknown header #{buffer.byteslice(0, 3).bytes.inspect}")
      end

      method = to_method(buffer.byteslice(3))
      params = MessagePack.unpack buffer.byteslice(8, body_size)

      return buffer.byteslice(8+body_size..-1), method, params
    end

    def encode(method, params) #=> binary string
      params = MessagePack.pack(params)
      method = to_byte(method)
      MAGIC_HEADER + method + [params.bytesize].pack('N') + params
    end

    private

    def to_byte(method)
      @method_to_byte[method] ||
        fail(ArgumentError, "Unknown method #{method}")
    end

    def to_method(byte)
      @byte_to_method[byte] ||
        fail(ArgumentError, "Unknown byte #{byte.inspect} for method")
    end
  end

  TCPv2Encoding = TCPv2EncodingImpl.new(
    :'pair:ping' => "\x28",
    :'pair:pong' => "\x29",
    :'report_all' => "\x30",
    :'register_process' => "\x41",
    :'stats_partial' => "\x42",
    :'stats_complete' => "\x43",
  )
end
