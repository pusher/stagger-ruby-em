module Stagger
  class Delta
    attr_reader :value

    def initialize
      @value = nil
    end

    def delta(value)
      if @value
        @value, old_value = value, @value
        return value - old_value
      else
        @value = value
        return nil
      end
    end
  end
end
