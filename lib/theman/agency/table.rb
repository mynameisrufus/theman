module Theman
  class Agency
    class Table
      def initialize(name, columns, temporary = nil, on_commit = nil)
        @name       = name
        @columns    = columns
        @temporary  = temporary
        @on_commit  = on_commit
      end

      def to_sql(sql = []) #:nodoc
        sql << ["CREATE"]
        sql << "TEMPORARY TABLE" unless @temporary == false
        sql << @name
        sql << "(#{@columns})"
        unless @on_commit.nil?
          sql << "ON COMMIT"
          sql << @on_commit.to_s.upcase.gsub(/_/," ")
        end
        sql.join(" ")
      end
    end
  end
end
