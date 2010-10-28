# -*- encoding: utf-8 -*-
require File.expand_path("../lib/theman/version", __FILE__)

Gem::Specification.new do |s|
  s.name        = "theman"
  s.version     = Theman::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Rufus Post"]
  s.email       = ["rufuspost@gmail.com"]
  s.homepage    = "http://github.com/mynameisrufus/theman"
  s.summary     = "PostgreSQL AR temporary table generator using PostgreSQL COPY"
  s.description = "FasterCSV is great and all but when you get to 100mb files it takes a while and you may only be looking for certain records that match some criteria, enter theman"

  s.required_rubygems_version = ">= 1.3.6"
  s.rubyforge_project         = "theman"

  s.add_development_dependency "bundler", ">= 1.0.0"
  s.add_development_dependency "rspec", ">= 2.0.0"
  s.add_development_dependency "activerecord", ">= 3.0.0"
  s.add_development_dependency "pg"
  
  s.add_runtime_dependency "activerecord"
  s.add_runtime_dependency "pg"

  s.files        = `git ls-files`.split("\n")
  s.executables  = `git ls-files`.split("\n").map{|f| f =~ /^bin\/(.*)/ ? $1 : nil}.compact
  s.require_path = 'lib'
end
