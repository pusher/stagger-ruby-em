module Stagger
  class Client
    # Only 279 google results for "port 5867" :)
    def initialize(reg_address = "tcp://127.0.0.1:5867")
      register(reg_address)

      @count_callbacks = {}
      @value_callbacks = {}
      @callbacks = []

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

    def register_cb(&block)
      @callbacks << block
    end

    def incr(name, count = 1)
      if @connected
        @aggregator.incr(name, block_given? ? yield : count)
      end
    end

    def value(name, value = nil, weight = 1)
      if @connected
        if block_given?
          v, w = yield
          @aggregator.value(name, v, w)
        else
          @aggregator.value(name, value, weight)
        end
      end
    end

    private

    def reset_data
      @aggregator = Aggregator.new(@zmq_client)
    end

    def register(reg_address)
      @zmq_client = Protocol.new(reg_address)
      @zmq_client.on(:command, &method(:command))
      @zmq_client.on(:connected) {
        @connected = true
      }
      @zmq_client.on(:disconnected) {
        @connected = false
        # Reset data when disconnected so that old (potentially ancient) data
        # isn't sent on reconnect, which would be confusing default behaviour
        # TODO: Maybe make this behaviour configurable?
        reset_data
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

      @aggregator.report(ts, aggregator_options)
      reset_data
    end

    def run_and_report_async(ts)
      EM::Iterator.new(@callbacks, 10).each(
        lambda { |cb, iter|
          aggregator = Aggregator.new(@zmq_client)
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

    def command(method, params)
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
