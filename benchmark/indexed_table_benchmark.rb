require File.join(File.dirname(__FILE__), 'benchmark_helper')

class ConversionRates < IndexedTable::Base
  name "benchmark_conversion_rates"
  fields :datasets, :versions, :conversions, :hours, :totals, :successes
  indices :datasets, :hours
end

hours = (1..720).to_a
datasets = ['purchases', 'gifts', 'comments']
versions = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
conversions = ['\\N', 'installs', 'clicks']

begin
  puts "-" * 50
  puts "Loading 50,000 rows"
  puts Benchmark.measure {
    50000.times do |i|
      ConversionRates << [
        datasets[i % 3], 
        versions[i % 7], 
        conversions[i % 3], 
        hours[i % 720], 
        (rand * 100).round, 
        100 + (rand * 200).round
      ]
    end
  }

  puts "-" * 50
  puts "Finding all purchases"
  puts Benchmark.measure {
    puts "Length: " + ConversionRates.find(:datasets => 'purchases').length.to_s
  }

  puts "-" * 50
  puts "Finding all purchases during hour 100"
  puts Benchmark.measure {
    puts "Length: " + ConversionRates.find(:datasets => 'purchases', :hours => 100).length.to_s
  }

  puts "-" * 50
  puts "Finding 336 hours of data"
  puts Benchmark.measure {
    results = []
    1.upto(336) do |hour|
      results += ConversionRates.find(:hours => hour)
    end
    puts "Length: " + results.length.to_s
  }
ensure
  redis = Redis.new
  redis.keys('benchmark*').each {|k| redis.delete k }
end