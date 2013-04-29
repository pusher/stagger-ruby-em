module Pair
  class Client
    attr_reader :pair

    include EM::ZeroMQ::EventEmitter

    def initialize(pair, timeout = 60)
      @pair, @timeout = pair, timeout
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
        when "ping"
          pong
        when "pong" # NOOP
        else
          # TODO
          emit(:message, method, part2.copy_out_string)
        end
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
      send("ping")
    end

    def pong
      send("pong")
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
