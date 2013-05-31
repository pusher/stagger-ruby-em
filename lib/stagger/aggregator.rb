module Stagger
  class Aggregator
    def initialize(zmq_client)
      @zmq_client = zmq_client
      @deltas = Hash.new
      reset_data
    end

    def reset_data()
      @counters = Hash.new { |h,k| h[k] = 0 }
      @values = Hash.new { |h,k| h[k] = Distribution.new }
    end

    def delta(name, value, weight = 1)
      if value
        @values[name.to_sym].add(value.to_f - @deltas[name.to_sym], weight) if @deltas[name.to_sym]
        @deltas[name.to_sym] = value.to_f
      end
    end

    def incr(name, count = 1)
      @counters[name.to_sym] += block_given? ? yield : count
    end

    def value(name, value, weight = 1)
      @values[name.to_sym].add(value.to_f, weight) if value
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
    end
  end
end
