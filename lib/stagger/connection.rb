module Stagger
  # Manages the connection and encoding/decoding of messages to stagger.
  #
  # TODO: ping/pong
  # TODO: hello
  class Connection < EventMachine::Connection
    include EventEmitter

    def initialize(host, port, retry_after_s = 5, encoding = TCPv2Encoding)
      @host = host
      @port = port
      @retry_after_s = retry_after_s
      @encoding = encoding
    end

    # Encodes and writes data to the socket
    def send_command(method, params = {})
      send_data @encoding.encode(method, params)
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

      @buffer = ""

      emit(:connected)
    end

    # Called by EM when the other side has sent some data and it arrived
    def receive_data(data)
      @buffer, method, params = @encoding.decode(@buffer + data)

      emit(:command, method, params) if method
    rescue => ex
      emit(:error, ex)
      # Reset everything if there is an error
      close_connection
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
