module Stagger
  class Client
    # Only 279 google results for "port 5867" :)
    def initialize(reg_address = "tcp://127.0.0.1:5867", zmq_context = Stagger.zmq)
      register(reg_address, zmq_context)

      @count_callbacks = {}
      @value_callbacks = {}
      @delta_callbacks = {}
      @callbacks = []

      @aggregator = Aggregator.new(@zmq_client)
    end

    # This should be called before the process exits. It explicity notifies
    # stagger that the connection is shutting down, rather than relying on
    # ping-pong messages to do the same (therefore stagger knows sooner).
    def shutdown
      @zmq_client.shutdown
    end

    def register_count(name, tags={}, &block)
      k = key(name, tags)
      raise "Already registered #{k}" if @count_callbacks[k]
      @count_callbacks[k] = block
    end

    def register_value(name, tags={}, &block)
      k = key(name, tags)
      raise "Already registered #{k}" if @value_callbacks[k]
      @value_callbacks[k] = block
    end

    def register_delta(name, tags={}, &block)
      k = key(name, tags)
      raise "Already registered #{k}" if @delta_callbacks[k]
      @delta_callbacks[k] = block
    end

    def register_cb(&block)
      @callbacks << [block, Aggregator.new(@zmq_client)]
    end

    def incr(name, count = 1, tags = {})
      if @connected
        @aggregator.incr(key(name, tags), block_given? ? yield : count)
      end
    end

    def value(name, value = nil, weight = 1, tags = {})
      if @connected
        if block_given?
          vw = yield
          @aggregator.value(key(name, tags), *vw)
        else
          @aggregator.value(key(name, tags), value, weight)
        end
      end
    end

    def delta(name, value = nil, tags = {})
      if @connected
        value = yield if block_given?
        @aggregator.delta(key(name, tags), value)
      end
    end

    private

    def key(name, tags)
      return name.to_sym if tags.empty?
      # Make sure the keys and values are strings and ordered
      tags = tags.each_with_object({}) do |(k, v), hash|
        hash[k.to_s] = v.to_s
      end
      "#{name},#{tags.sort.map{|pair| pair.join('=')}.join(',')}"
    end

    def reset_all
      @aggregator.reset_all
      @callbacks.each { |cb, agg| agg.reset_all }
    end

    def register(reg_address, zmq_context)
      @zmq_client = Protocol.new(reg_address, zmq_context)
      @zmq_client.on(:command, &method(:on_command))
      @zmq_client.on(:connected) {
        @connected = true
      }
      @zmq_client.on(:disconnected) {
        @connected = false
        # Reset data when disconnected so that old (potentially ancient) data
        # isn't sent on reconnect, which would be confusing default behaviour
        # TODO: Maybe make this behaviour configurable?
        reset_all
      }
    end

    def run_and_report_sync(ts, aggregator_options)
      @count_callbacks.each do |name, cb|
        c = cb.call
        @aggregator.incr(name, c) if c
      end

      @value_callbacks.each do |name, cb|
        vw = cb.call
        @aggregator.value(name, *vw) if vw
      end

      @delta_callbacks.each do |name, cb|
        v = cb.call
        self.delta(name, v) if v
      end

      @aggregator.report(ts, aggregator_options)
    end

    def run_and_report_async(ts)
      EM::Iterator.new(@callbacks, 10).each(
        lambda { |(cb, aggregator), iter|
          maybe_df = cb.call(aggregator)
          if maybe_df.kind_of?(EM::Deferrable)
            maybe_df.callback {
              aggregator.report(ts, complete: false)
              iter.next
            }.errback {
              iter.next
            }
          else
            aggregator.report(ts, complete: false)
            iter.next
          end
        },
        lambda {
          Aggregator.new(@zmq_client).report(ts, complete: true)
        }
      )
    end

    def on_command(method, params)
      case method
      when "report_all"
        ts = params["Timestamp"]
        if @callbacks.any?
          run_and_report_sync(ts, complete: false)
          run_and_report_async(ts)
        else
          run_and_report_sync(ts, complete: true)
        end
      else
        p ["Unknown command", method]
      end
    end
  end
end
