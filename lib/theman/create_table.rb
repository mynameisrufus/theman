module Theman
  class CreateTable
    def initialize(columns, base, table_name, temporary = false)
      @table_name = table_name
      @base       = base
      @columns    = columns
      @temporary  = temporary
    end

    def to_sql
      td = ActiveRecord::ConnectionAdapters::TableDefinition.new(@base)
      @columns.each do |col|
        td.column *col
      end
      <<-SQL
        CREATE TEMPORARY TABLE #{@table_name} (
          #{td.to_sql}
        );
      SQL
    end
    

  end
end
