module Stagger
	# A little state-machien that emits commands
	class ProtocolParser
		include EventEmitter

		attr_reader :state

		def initialize(max_size = 4 * 1024 * 1024)
			@max_size = max_size
			reset_state
		end

		def feed(data)
			p [:feed, data]
			return :error if state == :error

			@buffer += data.encode(Encoding::BINARY)
			if @buffer.bytesize > @max_size
				return fail("Too much data (%s bytes)" % [@buffer.bytesize])
			end

			loop do
				case @state
				when :init
					md = /\A(\d+),(\d+)\./.match(@state).to_a
					return :init unless md

					@buffer.slice!(0, md.end(2))
					@sizes = md.captures.map(&:to_i)
					transition_to(:param)
				when :method
					return :method if @buffer.bytesize < @sizes.first
					@method = @buffer.slice!(0, @sizes.first)

				when :body
					return :body if @buffer.bytesize < @sizes[1] + 1
					if @buffer[0] != ":"
						return fail("Invalid separator %s != :" % @buffer[0])
					end

					@body = @buffer.slice!(1, @sizes[1])
					emit(:command, @param, @body)
					@sizes = @param = @body = nil
					transition_to(:init)
				else
					return fail("Unexpected state %s" % @state)
				end
			end
		end

		def reset_state
			@buffer = "".encode(Encoding::BINARY)
			@sizes = @param = @body = nil
			@state = :init
		end

		protected

		def fail(reason)
			emit(:error, reason)
			transition_to(:error)
		end

		def transition_to(state)
			p [:state_change, @state, state]
			@state = state
		end
	end
end