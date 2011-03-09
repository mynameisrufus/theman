require 'spec_helper'

describe Theman::Agency::Columns, "data types" do
  before do
    @columns = Theman::Agency::Columns.new(ActiveRecord::Base.connection.raw_connection)
  end

  it "should return string col with limit" do
    @columns.string :col_one
    @columns.string :col_two, :limit => 50
    @columns.to_sql.should == "\"col_one\" character varying(255), \"col_two\" character varying(50)"
  end

  it "should NOT NULL" do
    @columns.string :col_one, :limit => 20, :null => false
    @columns.to_sql.should == "\"col_one\" character varying(20) NOT NULL"
  end

  it "should default" do
    @columns.string :col_one, :limit => 50, :default => "'sam the man'"
    @columns.to_sql.should == "\"col_one\" character varying(50) DEFAULT 'sam the man'"
  end

  it "should return a text" do
    @columns.text :col_one
    @columns.to_sql.should == "\"col_one\" text"
  end

  it "should return int type with scale" do
    @columns.integer :col_one
    @columns.integer :col_two, :limit => 1
    @columns.integer :col_three, :limit => 5
    @columns.to_sql.should == "\"col_one\" integer, \"col_two\" smallint, \"col_three\" bigint"
  end

  it "should return a float type with precision" do
    @columns.float :col_one
    @columns.to_sql.should == "\"col_one\" double precision"
  end

  it "should return a decimal with precision" do
    @columns.decimal :col_one
    @columns.to_sql.should == "\"col_one\" double precision"
  end

  it "should return a binary" do
    @columns.binary :col_one
    @columns.to_sql.should == "\"col_one\" oid"
  end

  it "should return a datetime" do
    @columns.datetime :col_one
    @columns.to_sql.should == "\"col_one\" timestamp without time zone"
  end

  it "should return a timestamp" do
    @columns.timestamp :col_one
    @columns.to_sql.should == "\"col_one\" timestamp without time zone"
  end

  it "should return a time" do
    @columns.time :col_one
    @columns.to_sql.should == "\"col_one\" time without time zone"
  end

  it "should return a date" do
    @columns.date :col_one
    @columns.to_sql.should == "\"col_one\" date"
  end

  it "should return a boolean" do
    @columns.boolean :col_one
    @columns.to_sql.should == "\"col_one\" boolean"
  end
end

describe Theman::Agency::Columns, "data types" do
  before do
    @columns = Theman::Agency::Columns.new(ActiveRecord::Base.connection.raw_connection)
  end

  it "should have include? method" do
    @columns.boolean :col_one
    @columns.include?(:col_one).should be_true
    @columns.include?(:col_two).should be_false
  end
end
