require 'em-zeromq'

module Pair
  @reg_addr = "tcp://127.0.0.1:5867"
  @timeout = 1

  class << self
    attr_writer :zmq, :reg_addr, :timeout

    def default
      @default ||= begin
        @zmq ||= EM::ZeroMQ::Context.new(1)
        Registration.new(@zmq, @reg_addr, @timeout)
      end
    end
  end
end

require 'pair/client'
require 'pair/registration'
