module Theman
  class Agency
    attr_reader :columns, :table_name, :connection

    def initialize(conn, stream, options = {}, &block)
      @stream       = stream
      @connection   = conn
      @options      = options

      @table_name         = sprintf "agent%010d", rand(100000000)
      @columns            = Columns.new(conn)
      @stream_columns_set = false

      if block_given?
        yield self
        create!
      end
    end
    
    def create_stream_columns #:nodoc
      @stream_columns_set = true
      headers.split(delimiter_regexp).each do |column|
        @columns.string column
      end
    end

    def headers #:nodoc
      File.open(@stream, "r"){ |infile| infile.gets }
    end
    
    # create default columns from stream and replace selected
    # columns with custom data types from block
    def table(&block)
      create_stream_columns
      yield @columns
    end
    
    # the location of the data to be sent to Postgres via STDIN (requires a header row)
    def stream(arg)
      @stream = arg
    end
    
    # datestyle of date columns
    def datestyle(arg)
      @datestyle = arg
    end
    
    # values in stream to replace with NULL
    def nulls(*args)
      @nulls = args
    end

    # custom seds to parse stream with
    def seds(*args)
      @seds = args
    end

    # delimter used in stream - comma is the default
    def delimiter(arg)
      @delimiter = arg
    end
    
    def psql_copy(psql = []) #:nodoc
      psql << "COPY #{table_name} FROM STDIN WITH"
      psql << "DELIMITER '#{@delimiter}'" unless @delimiter.nil?
      psql << "CSV HEADER"
      psql
    end

    def psql_command(psql = []) #:nodoc
      psql << "SET DATESTYLE TO #{@datestyle}" unless @datestyle.nil?
      psql << psql_copy.join(" ")
      psql
    end

    def sed_command(sed = []) #:nodoc
      sed << nulls_to_sed unless @nulls.nil?
      sed << @seds unless @seds.nil?
      sed
    end

    def nulls_to_sed #:nodoc
      @nulls.map do |regex|
        "-e 's/#{regex.source}//g'"
      end
    end

    def delimiter_regexp #:nodoc
      Regexp.new(@delimiter.nil? ? "," : "\\#{@delimiter}")
    end
    
    # Postgress COPY command using STDIN with CSV HEADER
    # - reads chunks of 8192 bytes to save memory
    # System command for IO subprocesses are piped to 
    # take advantage of multi cores
    def create!
      create_stream_columns unless @stream_columns_set
      connection.exec Table.new(table_name, @columns.to_sql, @options[:temporary]).to_sql
      pipe_it
    end
    
    # adds a serial column called agents_pkey and sets as primary key
    def add_primary_key!
      connection.exec "ALTER TABLE #{table_name} ADD COLUMN agents_pkey serial PRIMARY KEY;"
    end
    
    # analyzes the table for efficent query contstruction on tables larger than ~1000 tuples
    def analyze!
      connection.exec "ANALYZE #{table_name};";
    end
    
    def system_command #:nodoc
      unless sed_command.empty?
        "cat #{@stream} | sed #{sed_command.join(" | sed ")}" 
      else
        "cat #{@stream}"
      end
    end

    def pipe_it(l = "") #:nodoc
      connection.exec psql_command.join("; ")
      f = IO.popen(system_command)
      begin
        while f.read(8192, l)
          connection.put_copy_data l
        end
      rescue EOFError
        f.close
      end
      connection.put_copy_end
    end
    
    #def dump(file = File.join(File.direname(__FILE__),"#{instance.table_name}.csv"))
    #  psql = []
    #  psql << "COPY #{instance.table_name} TO STDOUT"
    #  psql << "WITH DELIMITER '#{@delimiter}'" unless @delimiter.nil?
    #  con.query psql.join(' ')
    #end
  end
end
