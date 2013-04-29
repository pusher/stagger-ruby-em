module Pair
  class Registration
    def initialize(zmq, reg_addr, timeout = 60)
      @zmq, @reg_addr, @timeout = zmq, reg_addr, timeout
    end

    def register_client(metadata = "")
      pair = @zmq.socket(ZMQ::PAIR)
      pair.setsockopt(ZMQ::LINGER, 0)
      pair.bind("tcp://127.0.0.1:*")
      addr = pair.getsockopt(ZMQ::LAST_ENDPOINT).chomp("\0")

      registration = @zmq.socket(ZMQ::PUSH)
      registration.setsockopt(ZMQ::LINGER, 0)
      registration.connect(@reg_addr)

      registration.send_msg(addr, metadata)

      return Client.new(pair, @timeout).tap { |c|
        c.on(:connected) {
          # Wait for the client to connect before disconnecting otherwise the
          # registration will never arrive
          registration.disconnect(@reg_addr)
        }
      }
    end

    def bind(&block)
      registration = @zmq.socket(ZMQ::PULL)
      registration.bind(@reg_addr)

      registration.on(:message) { |m|
        client_addr = m.copy_out_string

        pair = @zmq.socket(ZMQ::PAIR)
        pair.setsockopt(ZMQ::LINGER, 0)
        pair.connect(client_addr)

        yield Client.new(pair, @timeout)
      }
    end
  end
end
