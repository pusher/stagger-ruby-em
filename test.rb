#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
require 'bundler/setup'
require 'stagger'

EM.error_handler do |err|
  p [:error, err, *(err.backtrace rescue nil)]
end

EM.run {
  (1..20).each do |i|
    Stagger.default.register_value(:"app.connections", account_id: i%3, app_id: i) {
      Random.rand
    }
  end
  pause = true
  Stagger.default.register_cb { |agg|
    if pause
      puts "Long tick"
      sleep 21
      puts "done"
      pause = false
    end
    agg.incr(:"app.woot", 1, account_id: rand(100))
  }

  puts "Waiting..."
}
