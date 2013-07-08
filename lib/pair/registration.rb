module Pair
  class Registration
    def initialize(zmq, reg_addr, timeout = 60)
      @zmq, @reg_addr, @timeout = zmq, reg_addr, timeout

      @registration = @zmq.socket(ZMQ::PUSH)
      @registration.setsockopt(ZMQ::LINGER, 0)
      @registration.connect(@reg_addr)
    end

    def register_client(metadata = "")
      pair = @zmq.socket(ZMQ::PAIR)
      pair.setsockopt(ZMQ::LINGER, 0)
      pair.bind("tcp://127.0.0.1:*")
      addr = pair.getsockopt(ZMQ::LAST_ENDPOINT).chomp("\0")

      return Client.new(addr, pair, @timeout).tap { |c|
        register(c, metadata)
        c.on(:disconnected) {
          puts "Disconnected, register called"
          register(c, metadata)
        }
      }
    end

    def register(client, metadata)
      @registration.send_msg(client.addr, metadata)
    end

    def bind(&block)
      registration = @zmq.socket(ZMQ::PULL)
      registration.bind(@reg_addr)

      registration.on(:message) { |m|
        client_addr = m.copy_out_string
        m.close

        pair = @zmq.socket(ZMQ::PAIR)
        pair.setsockopt(ZMQ::LINGER, 0)
        pair.connect(client_addr)

        yield Client.new(client_addr, pair, @timeout)
      }
    end
  end
end
