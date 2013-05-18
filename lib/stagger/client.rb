module Stagger
  class Client
    # Only 279 google results for "port 5867" :)
    def initialize(reg_address = "tcp://127.0.0.1:5867")
      register(reg_address)

      @count_callbacks = {}
      @value_callbacks = {}

      reset_data
    end

    def register_count(name, &block)
      raise "Already registered #{name}" if @count_callbacks[name]
      @count_callbacks[name.to_sym] = block
    end

    def register_value(name, &block)
      raise "Already registered #{name}" if @value_callbacks[name]
      @value_callbacks[name.to_sym] = block
    end

    def incr(name, count = 1)
      if @connected
        @counters[name.to_sym] += block_given? ? yield : count
      end
    end

    def value(name, value = nil, weight = 1)
      if @connected
        if block_given?
          v, w = yield
          @values[name.to_sym].add(v, w || 1) if v
        else
          @values[name.to_sym].add(value, weight) if value
        end
      end
    end

    private

    def reset_data
      @counters = Hash.new { |h,k| h[k] = 0 }
      @values = Hash.new { |h,k| h[k] = Distribution.new }
    end

    def register(reg_address)
      @zmq_client = Protocol.new(reg_address)
      @zmq_client.on(:command, &method(:command))
      @zmq_client.on(:connected) {
        @connected = true
      }
      @zmq_client.on(:disconnected) {
        puts "Connection to client lost, reregistering"
        @connected = false
        # Reset data when disconnected so that old (potentially ancient) data
        # isn't sent on reconnect, which would be confusing default behaviour
        # TODO: Maybe make this behaviour configurable?
        reset_data
        register(reg_address)
      }
    end

    def run_callbacks
      @count_callbacks.each do |name, cb|
        c = cb.call
        incr(name, c) if c
      end

      @value_callbacks.each do |name, cb|
        vw = *cb.call
        value(name, *vw) if vw
      end
    end

    def command(method, params)
      case method
      when "report_all"
        run_callbacks

        body = {
          Timestamp: params["Timestamp"],
          Counts: @counters.map do |name, count|
            {Name: name, Count: count.to_f}
          end,
          Dists: @values.map do |name, vd|
            {Name: name, Dist: vd.to_a.map(&:to_f)}
          end,
        }

        @zmq_client.send_message("stats_complete", body)

        reset_data
      else
        p ["Unknown command", method]
      end
    end
  end
end
