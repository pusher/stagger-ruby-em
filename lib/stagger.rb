module Stagger
  def self.default
    @default ||= Client.new
  end
end

require 'eventmachine'
require 'msgpack'

require 'logger'

require 'stagger/tags'
require 'stagger/event_emitter'
require 'stagger/tcp_v2_encoding'
require 'stagger/connection'

require 'stagger/aggregator'
require 'stagger/delta'
require 'stagger/distribution'

require 'stagger/client'
