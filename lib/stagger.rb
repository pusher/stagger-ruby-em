require 'em-zeromq'

module Stagger
  class << self
    def default
      @default ||= Client.new
    end

    def zmq
      @ctx ||= EM::ZeroMQ::Context.new(1)
    end
  end
end

require 'stagger/event_emitter'
require 'stagger/protocol'
require 'stagger/aggregator'
require 'stagger/delta'
require 'stagger/client'
require 'stagger/distribution'
