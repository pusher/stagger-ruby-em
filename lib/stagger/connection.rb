module Stagger
  # Manages the connection and encoding/decoding of messages to stagger.
  #
  # TODO: ping/pong
  # TODO: hello
  class Connection < EventMachine::Connection
    include EventEmitter

    def initialize(host, port)
      @host = host
      @port = port
    end

    # Encodes and writes data to the socket
    def send_command(method, body = "")
      params = MessagePack.pack(body)
      send_data("%d,%d.%s:%s", message.bytesize, params.bytesize, method, params)
    end

    # Once shutdown is called there is no way back
    def shutdown
      @should_close = true
      close_connection(false)
    end

    #protected

    # Called by EM after the connection is created
    def post_init(*a)
      p [:post_init, *a]
    end

    # Called by EM after the connection is established
    def connection_completed
      @parser = ProtocolParser.new
      @parser.on(:command) do |command|
        emit(:command, command)
      end
      @parser.on(:error) do |reason|
        # TODO: log error
        p [:error, reason]
        close_connection
      end

      emit(:connected, peername: get_peername)
    end

    # Called by EM when the other side has sent some data and it arrived
    def receive_data(data)
      super
      @parser.feed(data)
    end

    def unbind
      emit(:disconnected, error: error?)
      return if @should_close
      # TODO: reconnect attempts
      EM.add_timer(5) { reconnect(@host, @port) }
    end

  end
end
