require File.join(File.dirname(__FILE__), 'spec_helper')

class ConversionRates < PartitionedTable::Base
  name "test_conversion_rates"
  fields :accounts, :applications, :datasets, 
          :versions, :hours, :totals, :successes
  partitions :accounts, :applications, :datasets, :hours
end

describe ConversionRates do
  after(:each) do
    redis = Redis.new
    redis.keys('test*').each{|k| redis.delete(k)}
  end
  
  it "should have the corect name" do
    ConversionRates.name.should == 'test_conversion_rates'
  end
  
  it "should have the correct fields" do
    ConversionRates.fields.should == [
      :accounts, :applications, :datasets, 
      :versions, :hours, :totals, :successes
    ]
  end
  
  it "should have the correct partitions" do
    ConversionRates.partitions.should == [
      :accounts, :applications, :datasets, :hours
    ]
  end
  
  it "should be able to append a row" do
    ConversionRates << ['test', 'test', 'notifications', 'A.1', 1, 1, 1]
  end
  
  it "should be able to load multiple rows" do
    ConversionRates.load(
      [
        ['test', 'test', 'notifications', 'A.1', 1, 1, 1], 
        ['test', 'test', 'invites', 'A.1', 1, 1, 1]
      ]
    )
  end
  
  it "should be able to retrieve a value" do
    ConversionRates << ['test', 'test', 'notifications', 'A.1', 1, 1, 1]
    ConversionRates.find(:datasets => 'notifications').length.should == 1
  end
end