module Theman
  class Agency
    attr_reader :instance, :column_names, :custom_sed_commands

    def initialize(stream = nil, parent = ::ActiveRecord::Base, options = {})
      # source of the data
      @options = options
      @stream  = stream

      # create a new class that extends an active record model
      # use instance_parent(klass) if not ActiveRecord::Base
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

      # if stream given table will be created
      # other wise create_table and pipe_it will need to called
      # proceduraly
      return unless stream
      if block_given?
        yield self
      end
      create_table
      pipe_it
    end

    def table
      yield self if block_given?
    end

    # overide ActiveRecord column types to be used in a block
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

    def stream(path)
      @stream = path
    end

    def datestyle(local)
      @psql_datestyle = local
    end

    def nulls(*args)
      @null_sed_commands = args
    end

    def seds(*args)
      @custom_sed_commands = args
    end
    
    def symbolize(name)
      name.gsub(/ /,"_").gsub(/\W/, "").downcase.to_sym
    end
    
    def psql_datestyle
      "SET DATESTYLE TO #{@psql_datestyle}"
    end

    def psql_copy
      "COPY #{@instance.table_name} FROM STDIN WITH CSV HEADER"
    end

    def psql_command(psql = [])
      psql << psql_datestyle unless @psql_datestyle.nil?
      raise unless @instance.table_exists?
      psql << psql_copy
      psql
    end

    def sed_command(sed= [])
      sed << null_sed_commands unless @null_sed_commands.nil?
      sed << custom_sed_commands unless @custom_sed_commands.nil?
      sed
    end

    def null_sed_commands
      @null_sed_commands.map do |regex|
        "-e 's/#{regex.source}//g'"
      end
    end

    def create_table
      f = File.open(@stream, 'r')
      instance.connection.create_table(instance.table_name, :temporary => (@options[:temporary] || true), :id => false) do |t|
        f.each_line do |line|
          line.split(/,/).each do |col|
            column_name = symbolize(col)
            if custom = @column_names.fetch(column_name, nil)
              t.column(*custom) 
            else
              t.string column_name
            end
          end
          break
        end
      end
    end
    
    def system_command
      unless sed_command.empty?
        "sed #{sed_command.join(" ")} #{@stream}" 
      else
        "cat #{@stream}" 
      end
    end

    # use postgress COPY command using STDIN with CSV HEADER
    # reads chunks of 8192 bytes to save memory
    def pipe_it(l = "")
      raw = instance.connection.raw_connection
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
