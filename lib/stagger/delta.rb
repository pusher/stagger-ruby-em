module Stagger
  class Delta
    attr_reader :value

    def initialize
      @value = nil
    end

    def delta(value)
      if @value && (delta = value - @value) >= 0
        @value = value
        return delta
      else
        @value = value
        return nil
      end
    end
  end
end
