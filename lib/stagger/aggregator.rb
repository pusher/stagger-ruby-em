require 'stagger/tags'

module Stagger
  class Aggregator
    include Tags

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

    ## Metrics

    def incr(key, count = 1, tags = {})
      c = block_given? ? yield : count
      if c && c > 0
        @counters[to_key(key, tags)] += c
      end
    end

    def value(key, value, weight = 1, tags = {})
      @values[to_key(key, tags)].add(value.to_f, weight) if value
    end

    def delta(key, value, tags = {})
      incr(key, @deltas[to_key(key, tags)].delta(value))
    end
    alias :delta_incr :delta

    def delta_value(key, value, weight = 1, tags = {})
      value(key, @deltas[to_key(key, tags)].delta(value), weight)
    end

    ## Reporting

    def report(ts, options)
      method = options[:complete] ? :stats_complete : :stats_partial

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
