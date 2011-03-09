require 'spec_helper'

describe Theman::Agency, "sed chomp" do
  before do
    conn  = ActiveRecord::Base.connection.raw_connection
    csv   = File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec', 'fixtures', 'temp_two.csv'))
    
    agent = ::Theman::Agency.new conn, csv do |agent|
      agent.chop 15
    end

    @model = Theman::Object.new(agent.table_name, ActiveRecord::Base)
  end
  
  it "should have all the records from the csv" do
    @model.count.should == 5
  end
end

describe Theman::Agency, "data types" do
  before do
    conn  = ActiveRecord::Base.connection.raw_connection
    csv   = File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec', 'fixtures', 'temp_one.csv'))
    @agent = ::Theman::Agency.new conn, csv do |agent|
      agent.nulls /"N"/, /"UNKNOWN"/, /""/
      agent.table do |t|
        t.date :col_date
        t.boolean :col_four
        t.float :col_five
      end
    end
    @model = Theman::Object.new(@agent.table_name, ActiveRecord::Base)
  end

  it "should create date col" do
    @model.first.col_date.class.should == Date
  end

  it "should create boolean col" do
    @model.where(:col_four => true).count.should == 2
  end

  it "should create float col" do
    @model.where("col_five > 10.0").count.should == 2
  end

  it "should have an array of nulls" do
    @agent.nulls_to_sed.should == ["-e 's/\"N\"//g'", "-e 's/\"UNKNOWN\"//g'", "-e 's/\"\"//g'"]
  end
  
  it "should have nulls not strings" do
    @model.where(:col_two => nil).count.should == 2
    @model.where(:col_three => nil).count.should == 2
  end
end

describe Theman::Agency, "european date styles" do
  before do
    conn  = ActiveRecord::Base.connection.raw_connection
    csv   = File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec', 'fixtures', 'temp_three.csv'))
    agent = ::Theman::Agency.new conn, csv do |smith|
      smith.datestyle 'European'
      smith.table do |t|
        t.date :col_date
      end
    end
    @model = Theman::Object.new(agent.table_name, ActiveRecord::Base)
  end
  
  it "should have correct date" do
    date = @model.first.col_date
    date.day.should == 25
    date.month.should == 12
  end
end

describe Theman::Agency, "US date styles" do
  before do
    conn  = ActiveRecord::Base.connection.raw_connection
    csv   = File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec', 'fixtures', 'temp_four.csv'))
    agent = ::Theman::Agency.new conn, csv do |smith|
      smith.datestyle 'US'
      smith.table do |t|
        t.date :col_date
      end
    end
    @model = Theman::Object.new(agent.table_name, ActiveRecord::Base)
  end
  
  it "should have correct date" do
    date = @model.first.col_date
    date.day.should == 25
    date.month.should == 12
  end
end

describe Theman::Agency, "ISO date styles" do
  before do
    conn  = ActiveRecord::Base.connection.raw_connection
    csv   = File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec', 'fixtures', 'temp_five.csv'))
    agent = ::Theman::Agency.new conn, csv do |smith|
      smith.datestyle 'ISO'
      smith.table do |t|
        t.date :col_date
      end
    end
    @model = Theman::Object.new(agent.table_name, ActiveRecord::Base)
  end
  
  it "should have correct date" do
    date = @model.first.col_date
    date.day.should == 25
    date.month.should == 12
  end
end

describe Theman::Agency, "procedural" do
  before do
    @conn  = ActiveRecord::Base.connection.raw_connection
    @csv   = File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec', 'fixtures', 'temp_two.csv'))
  end

  it "should be able to be called procedural" do
    smith = ::Theman::Agency.new @conn, @csv
    smith.datestyle "European"
    smith.chop 15
    smith.nulls /"XXXX"/
    
    smith.table do |t|
      t.date :date
    end

    smith.create!
    
    model = Theman::Object.new(smith.table_name, ActiveRecord::Base)
    model.first.date.class.should == Date
    model.first.org_code.class.should == NilClass
    model.count.should == 5
  end
end

describe Theman::Agency, "create table" do
  before do
    conn  = ActiveRecord::Base.connection.raw_connection
    csv   = File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec', 'fixtures', 'temp_one.csv'))
    agent = ::Theman::Agency.new conn, csv do |agent|
      agent.nulls /"N"/, /"UNKNOWN"/, /""/
      agent.table do |t|
        t.string :col_two, :limit => 50
      end
    end
    @model = Theman::Object.new(agent.table_name, ActiveRecord::Base)
  end

  it "should have" do
    @model.first.col_two.should == "some \\text\\"
  end
end

describe Theman::Agency, "add primary key" do
  before do
    conn  = ActiveRecord::Base.connection.raw_connection
    csv   = File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec', 'fixtures', 'temp_one.csv'))
    agent = ::Theman::Agency.new conn, csv
    agent.create!
    agent.add_primary_key!
    @model = Theman::Object.new(agent.table_name, ActiveRecord::Base)
  end

  it "should have serial primary key" do
    @model.first.id.should == 1
  end
end

describe Theman::Agency, "delimiters" do
  before do
    conn  = ActiveRecord::Base.connection.raw_connection
    csv   = File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec', 'fixtures', 'temp_six.txt'))
    agent = ::Theman::Agency.new conn, csv do |agent|
      agent.delimiter "|"
    end
    @model = Theman::Object.new(agent.table_name, ActiveRecord::Base)
  end

  it "should have imported pipe delimited txt file" do
    @model.count.should == 4
  end
end

describe Theman::Agency, "no headers" do
  before do
    conn  = ActiveRecord::Base.connection.raw_connection
    csv   = File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec', 'fixtures', 'temp_seven.csv'))
    agent = Theman::Agency.new conn, csv, :headers => false do |agent|
      agent.nulls /"N"/, /"UNKNOWN"/, /""/
      agent.table do |t|
        t.date :col_date
        t.string :col_two
        t.string :col_three
        t.boolean :col_four
        t.float :col_five
      end
    end
    @model = Theman::Object.new(agent.table_name, ActiveRecord::Base)
  end

  it "should import csv without headers" do
    @model.count.should == 4
  end
end

describe Theman::Agency, "with advanced sed" do
  before do
    conn  = ActiveRecord::Base.connection.raw_connection
    csv   = File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec', 'fixtures', 'temp_eight.csv'))
    agent = Theman::Agency.new conn, csv, :headers => false do |agent|
      agent.seds '-e \'s/[A-Z][A-Z]$/\"&\"/\' -e \'s/^[0-9|-]*/&/\'', '\'s/,\(.*\),/,\"\1\",/\''
      agent.table do |t|
        t.string :col_one
        t.string :col_two
        t.string :col_three
      end
    end
    @model = Theman::Object.new(agent.table_name, ActiveRecord::Base)
  end

  it "should import csv without headers" do
    @model.count.should == 249
  end
end

describe Theman::Agency, "data types" do
  it "should be able to run in transaction" do
    conn  = ActiveRecord::Base.connection.raw_connection
    csv   = File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec', 'fixtures', 'temp_three.csv'))
  
    agent = Theman::Agency.new conn, csv, :on_commit => :drop

    agent.transaction do
      agent.create!

      model = Theman::Object.new(agent.table_name, ActiveRecord::Base)
      model.count.should == 4
    end
  end
end

describe Theman::Agency, "data types" do
  before do
    @conn = ActiveRecord::Base.connection.raw_connection
    @csv  = File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec', 'fixtures', 'temp_one.csv'))
  end
  
  it "should raise an error if the columns are wrong" do
    agent = ::Theman::Agency.new @conn, @csv
    agent.table do |t|
      t.date :column_not_in_csv
    end
    lambda{ @agent.create! }.should raise_error
  end
end
