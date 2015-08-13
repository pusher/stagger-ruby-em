module Stagger
  def self.default
    @default ||= Client.new
  end
end

require 'eventmachine'
require 'msgpack'

require 'stagger/event_emitter'
require 'stagger/protocol_parser'
require 'stagger/connection'

require 'stagger/aggregator'
require 'stagger/delta'
require 'stagger/distribution'

require 'stagger/client'
