module Theman
  class Agency
    class Table
      def initialize(name, columns, temporary = false)
        @name       = name
        @columns    = columns
        @temporary  = temporary
      end

      def to_sql #:nodoc
        <<-SQL
          CREATE TEMPORARY TABLE #{@name} (
            #{@columns}
          );
        SQL
      end
    end
  end
end
