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

    def register_count(name, &block)
      raise "Already registered #{name}" if @count_callbacks[name]
      @count_callbacks[name.to_sym] = block
    end

    def register_value(name, &block)
      raise "Already registered #{name}" if @value_callbacks[name]
      @value_callbacks[name.to_sym] = block
    end

    def register_delta(name, &block)
      @delta_callbacks[name.to_sym] = block
    end

    def register_cb(&block)
      @callbacks << [block, Aggregator.new(@zmq_client)]
    end

    def incr(name, count = 1)
      if @connected
        @aggregator.incr(name, block_given? ? yield : count)
      end
    end

    def value(name, value = nil, weight = 1)
      if @connected
        if block_given?
          vw = yield
          @aggregator.value(name, *vw)
        else
          @aggregator.value(name, value, weight)
        end
      end
    end

    def delta(name, value = nil)
      if @connected
        if block_given?
          @aggregator.delta(name, yield)
        else
          @aggregator.delta(name, value)
        end
      end
    end

    private

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
