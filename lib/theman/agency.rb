module Theman
  class Agency
    attr_reader :instance, :column_names

    def initialize(stream = nil, parent = ::ActiveRecord::Base, options = {})
      @options = options
      @stream  = stream

      agent_id = sprintf "agent%010d", rand(100000000)
      @column_names = {}
      @instance = Class.new(parent) do
        instance_eval <<-EOV, __FILE__, __LINE__ + 1
          set_table_name "#{agent_id}"
          def table_name
            "#{agent_id}"
          end
          def inspect
            "Agent (#{agent_id})"
          end
        EOV
      end

      yield self if block_given?
      return unless stream
      create_table
      pipe_it
      if @options[:primary_key]
        add_primary_key
      end
    end

    def table
      yield self if block_given?
    end

    # columnn data type methods
    %w( string text integer float decimal datetime timestamp time date binary boolean ).each do |column_type|
      class_eval <<-EOV, __FILE__, __LINE__ + 1
        def #{column_type}(column_name, *args)
          column(column_name, '#{column_type}', *args)
        end
      EOV
    end

    # overides the default string type column
    def column(column_name, column_type, *args)
      @column_names.merge! column_name.to_sym => [column_name, column_type, *args]
    end

    def stream(arg)
      @stream = arg
    end

    def datestyle(arg)
      @datestyle = arg
    end

    def nulls(*args)
      @nulls = args
    end

    def seds(*args)
      @seds = args
    end

    def delimiter(arg)
      @delimiter = arg
    end
    
    def symbolize(name)
      name.gsub(/ /,"_").gsub(/\W/, "").downcase.to_sym
    end
    
    def psql_copy(psql = [])
      psql << "COPY #{@instance.table_name} FROM STDIN WITH"
      psql << "DELIMITER '#{@delimiter}'" unless @delimiter.nil?
      psql << "CSV HEADER"
      psql
    end

    def psql_command(psql = [])
      psql << "SET DATESTYLE TO #{@datestyle}" unless @datestyle.nil?
      psql << psql_copy.join(" ")
      psql
    end

    def sed_command(sed = [])
      sed << nulls_to_sed unless @nulls.nil?
      sed << @seds unless @seds.nil?
      sed
    end

    def nulls_to_sed
      @nulls.map do |regex|
        "-e 's/#{regex.source}//g'"
      end
    end

    # creates a delimiter regular expresion
    def delimiter_regexp
      Regexp.new(@delimiter.nil? ? "," : "\\#{@delimiter}")
    end
    
    def raw
      instance.connection.raw_connection
    end

    # read the first line from the stream to create a table with
    def create_table
      cols = []
      headers.split(delimiter_regexp).each do |col|
        column_name = symbolize(col)
        if c = @column_names.fetch(column_name, nil)
          cols << c
        else
          cols << [column_name, :string]
        end
      end
      table = CreateTable.new(cols, instance.connection, instance.table_name, @options[:temporary])
      raw.query table.to_sql
    end

    def headers
      File.open(@stream, "r"){ |infile| infile.gets }
    end
    
    # system command for IO subprocesses, commands are piped to 
    # take advantage of multi cores
    def system_command
      unless sed_command.empty?
        "cat #{@stream} | sed #{sed_command.join(" | sed ")}" 
      else
        "cat #{@stream}"
      end
    end

    # addition of a primary key after the data has been piped to 
    # the table
    def add_primary_key
      raw.query "ALTER TABLE #{instance.table_name} ADD COLUMN agents_pkey serial PRIMARY KEY; ANALYZE #{instance.table_name}";
    end

    # use postgress COPY command using STDIN with CSV HEADER
    # reads chunks of 8192 bytes to save memory
    def pipe_it(l = "")
      raise "table does not exist" unless instance.table_exists?
      raw.query psql_command.join("; ")
      f = IO.popen(system_command)
      begin
        while f.read(8192, l)
          raw.put_copy_data l
        end
      rescue EOFError
        f.close
      end
      raw.put_copy_end
    end
  end
end
