require File.join(File.dirname(__FILE__), 'benchmark_helper')

@redis = Redis.new

begin
  puts "Inserting 10,000 keys"
  puts Benchmark.measure {
    10000.times do |i|
      @redis["testkey_#{i}"] = i.to_s
    end
  }
  
  puts "Inserting 10,000 keys into index"
  puts Benchmark.measure {
    10000.times do |i|
      @redis.push_tail("testlist", i.to_s)
    end
  }
  
  puts "Pulling 10,000 values using sort"
  puts Benchmark.measure {
    @redis.sort("testlist", :get => "testkey_*")
  }
ensure
  @redis.keys('test*').each{|k| @redis.delete k}
end