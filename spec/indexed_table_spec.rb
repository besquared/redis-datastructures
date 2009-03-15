require File.join(File.dirname(__FILE__), 'spec_helper')

class ConversionRates < IndexedTable::Base
  name "test_conversion_rates"
  fields :datasets, :hours, :counts
  indices :datasets, :hours
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
    ConversionRates.fields.should == [:datasets, :hours, :counts]
  end
  
  it "should have the correct indices" do
    ConversionRates.indices.should == [:datasets, :hours]
  end
  
  it "should be able to append a row" do
    ConversionRates << ['purchases', 1, 1]
  end
  
  it "should be able to retrieve a value" do
    ConversionRates << ['purchases', 1, 1]
    ConversionRates.find(:datasets => 'purchases').length.should == 1
  end
end