require 'msgpack'

require 'stagger/event_emitter'

module Stagger
  # A little state-machien that emits commands
  class ProtocolParser
    include EventEmitter

    attr_reader :buffer, :state

    def initialize(max_size = 4 * 1024 * 1024)
      @max_size = max_size
      @buffer = "".encode(Encoding::BINARY)
      reset_state
    end

    def feed(data)
      return :error if state == :error

      @buffer += data.encode(Encoding::BINARY)

      if @buffer.bytesize > @max_size
        return fail("Too much data (%s bytes)" % [@buffer.bytesize])
      end

      catch(:abort) do
        begin
          loop do
            case @state
            when :init
              md = /\A(\d+),(\d+)\./.match(@buffer)
              if !md
                if @buffer.bytesize > 200
                  fail!("Init not found in the first 200 bytes")
                else
                  need_data!
                end
              end

              @buffer.slice!(0, md.end(2) + 1)
              @sizes = md.captures.map(&:to_i)
              transition_to(:method)
            when :method
              need_data! if @buffer.bytesize < @sizes.first
              @method = @buffer.slice!(0, @sizes.first)
              transition_to(:body)
            when :body
              need_data! if @buffer.bytesize < @sizes[1] + 1
              if @buffer[0] != ":"
                fail!("Invalid separator %s != :" % @buffer[0])
              end

              params = @buffer.slice!(0, @sizes[1]+1)
              params = MessagePack.unpack(params[1..-1])
              emit(:command, @method, params)

              # Reset state for next loop
              reset_state
            else
              fail!("Unexpected state %s" % @state)
            end
          end
        rescue => ex
          fail!(ex)
        end
      end
    end

    def reset_state
      # Resets all intermediary variables
      @sizes = @method = nil
      transition_to(:init)
    end

    protected

    def fail(reason)
      emit(:error, reason)
      transition_to(:error)
    end

    def fail!(reason)
      fail(reason)
      throw(:abort, :error)
    end

    def need_data!
      throw(:abort, @state)
    end

    def transition_to(state)
      @state = state
    end
  end
end
