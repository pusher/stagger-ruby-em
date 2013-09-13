module Pair
  class Client
    attr_reader :pair, :addr

    include EM::ZeroMQ::EventEmitter

    def initialize(addr, pair, timeout = 60)
      @addr, @pair, @timeout = addr, pair, timeout
      @connected = false

      ping

      @pair.on(:message) { |part1, part2|
        @connected ||= begin
          emit(:connected)
          setup_activity_check
          true
        end

        reset_activity

        case (method = part1.copy_out_string)
        when "pair:ping", "ping"
          pong
        when "pair:pong", "pong" # NOOP
        when "pair:shutdown"
          terminate(0)
        else
          # TODO
          emit(:message, method, part2.copy_out_string)
        end
        part1.close
        part2.close
      }
    end

    def terminate(time_since_activity)
      @activity_check.cancel if @activity_check
      @connected = false
      emit(:disconnected, time_since_activity)
    end

    def send(method, body = nil)
      @pair.send_msg(method, body)
    end

    private

    def ping
      send("pair:ping")
    end

    def pong
      send("pair:pong")
    end

    def reset_activity
      @activity_at = Time.now
    end

    def setup_activity_check
      @activity_check = EM::PeriodicTimer.new(@timeout) {
        now = Time.now
        since_activity = now - @activity_at
        if since_activity > 3 * @timeout
          terminate(since_activity)
        elsif since_activity > @timeout
          ping
        end
      }
    end
  end
end
