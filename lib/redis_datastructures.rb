$:.push File.join(File.dirname(__FILE__), '..', 'vendor', 'redis-rb', 'lib')

require 'redis'
require 'rubygems'
require 'activesupport'

require File.join(File.dirname(__FILE__), 'partitioned_table', 'base')
require File.join(File.dirname(__FILE__), 'indexed_table', 'base')
