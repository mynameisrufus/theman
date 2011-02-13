require 'spec_helper'

describe Theman::Object do
  before do
    csv   = File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec', 'fixtures', 'temp_one.csv'))
    conn  = ActiveRecord::Base.connection.raw_connection
    
    agent = Theman::Agency.new(conn, csv) do |agent|
      agent.table do |t|
        t.date :col_date
        t.string :col_two
      end
    end

    @model = Theman::Object.new(agent.table_name, ActiveRecord::Base)
  end

  it "should superclass active record" do
    @model.superclass.should == ActiveRecord::Base 
  end

  it "should have connection" do
    @model.connection.class.should == ActiveRecord::ConnectionAdapters::PostgreSQLAdapter
  end

  it "should have a table name" do
    @model.table_name.should match /agent[0-9]{10}/
  end

  it "should have an ispect method" do
    @model.inspect.should match /Agent/
  end

  it "should count" do
    @model.count.should == 4
  end
end
