require 'stagger/tags'

module Stagger
  class Client
    include Tags

    attr_accessor :logger
    def initialize(host = '127.0.0.1', port = 5865, logger = Logger.new(STDOUT), process_info = {pid: Process.pid, cmd: $0})
      @count_callbacks = {}
      @value_callbacks = {}
      @delta_callbacks = {}
      @callbacks = []
      @logger = logger
      @process_info = sanitize_process_info(process_info)
      setup_connection(host, port)

      @aggregator = Aggregator.new(@conn)
    end

    # This should be called before the process exits. It explicity notifies
    # stagger that the connection is shutting down, rather than relying on
    # ping-pong messages to do the same (therefore stagger knows sooner).
    def shutdown
      @conn.shutdown
    end

    def register_count(name, tags={}, &block)
      k = to_key(name, tags)
      raise "Already registered #{k}" if @count_callbacks[k]
      @count_callbacks[k] = block
    end

    def register_value(name, tags={}, &block)
      k = to_key(name, tags)
      raise "Already registered #{k}" if @value_callbacks[k]
      @value_callbacks[k] = block
    end

    def register_delta(name, tags={}, &block)
      k = to_key(name, tags)
      raise "Already registered #{k}" if @delta_callbacks[k]
      @delta_callbacks[k] = block
    end

    def register_cb(&block)
      @callbacks << [block, Aggregator.new(@conn)]
    end

    def incr(name, count = 1, tags = {})
      if @connected
        @aggregator.incr(name, block_given? ? yield : count, tags)
      end
    end

    def value(name, value = nil, weight = 1, tags = {})
      if @connected
        if block_given?
          value2, weight2, tags2 = yield
          value = value2 if value2
          weight = weight2 if weight2
          tags = tags2 if tags2
        end
        @aggregator.value(name, value, weight, tags)
      end
    end

    def delta(name, value = nil, tags = {})
      if @connected
        value = yield if block_given?
        @aggregator.delta(name, value, tags)
      end
    end

    private

    def reset_all
      @aggregator.reset_all
      @callbacks.each { |cb, agg| agg.reset_all }
    end

    def setup_connection(host, port)
      @conn = EM.connect(host, port, Connection, host, port)
      @conn.on(:command, &method(:on_command))
      @conn.on(:connected) {
        @logger.info("stagger connected to #{host}:#{port}")
        @connected = true
        send_register_process(@process_info)
      }
      @conn.on(:disconnected) {
        @logger.info("stagger disconnected from #{host}:#{port}")
        @connected = false
        # Reset data when disconnected so that old (potentially ancient) data
        # isn't sent on reconnect, which would be confusing default behaviour
        # TODO: Maybe make this behaviour configurable?
        reset_all
      }
      @conn.on(:error) { |reason|
        @logger.error("stagger connection error: #{reason}")
      }
      @conn
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
            }.errback { |reason|
              @logger.error("stagger reporting error: #{reason}")
              iter.next
            }
          else
            aggregator.report(ts, complete: false)
            iter.next
          end
        },
        lambda {
          Aggregator.new(@conn).report(ts, complete: true)
        }
      )
    end

    def on_command(method, params)
      case method
      when :report_all
        ts = params["Timestamp"]
        if @callbacks.any?
          run_and_report_sync(ts, complete: false)
          run_and_report_async(ts)
        else
          run_and_report_sync(ts, complete: true)
        end
      else
        raise ArgumentError, "Unknown command #{method}"
      end
    end

    private

    # Makes sure all the process info tags and values are strings
    def sanitize_process_info(tags={})
      return nil if tags.empty?

      tags.each_with_object({}) do |(k, v), hash|
        hash[to_utf8(k)] = to_utf8(v)
      end
    end

    def send_register_process(process_info)
      return if process_info.nil?
      @conn.send_command(:register_process, {Tags: process_info})
    end

    # Avoid sending binary data with MessagePack. The format has evolved and
    # there are some compatiblity issues.
    #
    # Favor losing characters during transcoding over raising exceptions.
    def to_utf8(str)
      str.to_s.encode('utf-8', invalid: :replace, undef: :replace)
    end
  end
end
