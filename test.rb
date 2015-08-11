#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-
require 'bundler/setup'
require 'stagger'
EM.run {
  (1..20).each do |i|
    Stagger.default.register_value(:"app.connections", account_id: i%3, app_id: i) {
      puts "fetching data for app.connections#{i}"
      Random.rand
    }
  end

  puts "Waiting..."
}
