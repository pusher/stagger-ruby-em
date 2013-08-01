module Stagger
  class Aggregator
    def initialize(zmq_client)
      @zmq_client = zmq_client
      reset_all
    end

    def reset_all
      @deltas = Hash.new  { |h,k| h[k] = Delta.new }
      reset_data
    end

    def reset_data
      @counters = Hash.new { |h,k| h[k] = 0 }
      @values = Hash.new { |h,k| h[k] = Distribution.new }
    end

    def incr(name, count = 1)
      c = block_given? ? yield : count
      if c && c > 0
        @counters[name.to_sym] += c
      end
    end

    def value(name, value, weight = 1)
      @values[name.to_sym].add(value.to_f, weight) if value
    end

    def delta(name, value)
      incr(name, @deltas[name].delta(value))
    end
    alias :delta_incr :delta

    def delta_value(name, value, weight = 1)
      value(name, @deltas[name].delta(value), weight)
    end

    def report(ts, options)
      method = options[:complete] ? "stats_complete" : "stats_partial"

      body = {
        Timestamp: ts,
        Counts: @counters.map do |name, count|
          {Name: name, Count: count.to_f}
        end,
        Dists: @values.map do |name, vd|
          {Name: name, Dist: vd.to_a.map(&:to_f)}
        end,
      }

      @zmq_client.send_message(method, body)

      reset_data()
    end
  end
end
