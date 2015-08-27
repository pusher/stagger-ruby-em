module Stagger
  module Tags
    # Transforms a name + tags to a key.
    def to_key(name, tags={})
      return name.to_sym if tags.nil? || tags.empty?
      # Make sure the keys and values are strings and ordered
      tags = tags.each_with_object({}) do |(k, v), hash|
        hash[k.to_s] = v.to_s
      end
      "#{name},#{tags.sort.map{|pair| pair.join('=')}.join(',')}"
    end
  end
end
