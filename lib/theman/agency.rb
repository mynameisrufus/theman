module Theman
  class Agency
    attr_reader :columns, :table_name, :connection
    # create a new agent object - if a block is passed create! is called
    #
    # ==== Parameters
    # * +conn+ - A database connection from the <tt>PGconn</tt> class 
    #   or <tt>ActiveRecord::Base.connection.raw_connection</tt> which 
    #   is the same class.
    # * +stream+ - path to the data file.
    # * +options+ - Additional options are <tt>:temporary</tt>, 
    #   <tt>:on_commit</tt> and <tt>:headers</tt>
    #
    # ==== Examples
    #   # Update all customers with the given attributes
    #   conn  = PGconn.open(:dbname => 'test')
    #   agent = Theman::Agency.new(conn, 'sample.csv')
    #   agent.create!
    #   res = conn.exec("SELECT count(*) FROM #{agent.table_name}")
    #   res.getvalue(0,0)
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
    
    # create a transaction block for use with :on_commit => :drop
    def transaction(&block)
      connection.exec "BEGIN;"
      yield
      connection.exec "COMMIT;"
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
      create_stream_columns unless @options[:headers] == false
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
      psql << "CSV"
      psql << "HEADER" unless @options[:headers] == false
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
      @delimiter_regexp ||= Regexp.new(@delimiter.nil? ? "," : "\\#{@delimiter}")
    end
    
    # Postgress COPY command using STDIN
    # - reads chunks of 8192 bytes to save memory
    # System command for IO subprocesses are piped to 
    # take advantage of multi cores
    def create!
      unless @stream_columns_set || @options[:headers] == false
        create_stream_columns
      end
      connection.exec Table.new(table_name, @columns.to_sql, @options[:temporary], @options[:on_commit]).to_sql
      pipe_it
    end
    
    # adds a serial column called id and sets as primary key
    # if your data allready has a column called id the column will be called agents_pkey
    def add_primary_key!
      name = @columns.include?(:id) ? "agents_pkey" : "id"
      connection.exec "ALTER TABLE #{table_name} ADD COLUMN #{name} serial PRIMARY KEY;"
    end
    
    # analyzes the table for efficent query contstruction on tables larger than ~1000 tuples
    def analyze!
      connection.exec "ANALYZE #{table_name};"
    end

    # explicitly drop table
    def drop!
      connection.exec "DROP TABLE #{table_name};"
      @table_name = nil
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
  end
end
