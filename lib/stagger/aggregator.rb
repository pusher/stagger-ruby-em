module Stagger
  class Aggregator
    def initialize(conn)
      @conn = conn
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

    def incr(key, count = 1)
      c = block_given? ? yield : count
      if c && c > 0
        @counters[key] += c
      end
    end

    def value(key, value, weight = 1)
      @values[key].add(value.to_f, weight) if value
    end

    def delta(key, value)
      incr(key, @deltas[key].delta(value))
    end
    alias :delta_incr :delta

    def delta_value(key, value, weight = 1)
      value(key, @deltas[key].delta(value), weight)
    end

    def report(ts, options)
      method = options[:complete] ? "stats_complete" : "stats_partial"

      body = {
        Timestamp: ts,
        Counts: @counters.map do |key, count|
          {Name: key, Count: count.to_f}
        end,
        Dists: @values.map do |key, vd|
          {Name: key, Dist: vd.to_a.map(&:to_f)}
        end,
      }

      @conn.send_command(method, body)

      reset_data()
    end
  end
end
