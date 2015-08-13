module Stagger
  # Manages the connection and encoding/decoding of messages to stagger.
  #
  # TODO: ping/pong
  # TODO: hello
  class Connection < EventMachine::Connection
    include EventEmitter

    def initialize(host, port, retry_after_s = 5)
      @host = host
      @port = port
      @retry_after_s = retry_after_s
    end

    # Encodes and writes data to the socket
    def send_command(method, params = {})
      params = MessagePack.pack(params)
      send_data("%d,%d.%s:%s" % [method.bytesize, params.bytesize, method, params])
    end

    # Once shutdown is called there is no way back
    def shutdown
      @should_close = true
      close_connection(false)
    end

    #protected

    # Called by EM after the connection is established
    def connection_completed
      # Disconnect after 30 seconds if no activity
      self.comm_inactivity_timeout = 30

      @parser = ProtocolParser.new

      @parser.on(:command) do |method, params|
        emit(:command, method, params)
      end

      @parser.on(:error) do |reason|
        emit(:error, reason)
        # Reset everything if there is an error
        close_connection
      end

      emit(:connected)
    end

    # Called by EM when the other side has sent some data and it arrived
    def receive_data(data)
      @parser.feed(data)
    end

    # Called by EM when the connection is closed
    def unbind
      emit(:disconnected, error: error?)
      return if @should_close

      # Always try to connect
      EM.add_timer(@retry_after_s) { reconnect(@host, @port) }
    end
  end
end
