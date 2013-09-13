require 'pair'
require 'msgpack'

module Stagger
  # Adds MessagePack serialization on top of Pair protocol
  class Protocol
    include EventEmitter

    def initialize(reg_addr = "tcp://127.0.0.1:5867")
      @reg_addr = reg_addr
      @pair = register()
    end

    def register
      reg = Pair::Registration.new(Stagger.zmq, @reg_addr, 13)
      pair = reg.register_client
      pair.on(:connected) { emit(:connected) }
      pair.on(:disconnected) { emit(:disconnected) }
      pair.on(:message, &method(:command))
      return pair
    end

    def shutdown
      @pair.shutdown
    end

    def command(method, msgpack_params)
      params = if !msgpack_params.empty?
        MessagePack.unpack(msgpack_params)
      else
        {}
      end

      emit(:command, method, params)
    end

    def send_message(message, body)
      @pair.send(message, MessagePack.pack(body))
    end
  end
end
