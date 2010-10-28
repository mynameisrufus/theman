module Theman
  class Agency
    attr_reader :instance, :column_names, :null_replacements, :sed_commands

    attr_writer :stream
    
    def initialize(stream = nil, parent = ::ActiveRecord::Base)
      # source of the data
      @stream = stream

      # create a new class that extends an active record model
      # use instance_parent(klass) if not ActiveRecord::Base
      cabinet_id = "c#{10.times.map{rand(9)}.join}" 
      @column_names = {}
      @instance = Class.new(parent) do
        instance_eval <<-EOV, __FILE__, __LINE__ + 1
          set_table_name "#{cabinet_id}"
          def table_name
            "#{cabinet_id}"
          end
          def inspect
            "Agent (#{cabinet_id})"
          end
        EOV
      end

      # if stream given table will be created
      # other wise create_table and pipe_it will need to called
      # proceduraly
      if stream
        if block_given?
          yield self
        end
        create_table
        pipe_it
      end
    end

    def table
      yield self if block_given?
    end

    # overide ActiveRecord column types to be used in a block
    %w( string text integer float decimal datetime timestamp time date binary boolean ).each do |column_type|
      class_eval <<-EOV, __FILE__, __LINE__ + 1
        def #{column_type}(*args)
          column(args[0], '#{column_type}', args[1].nil? ? {} : args[1])
        end
      EOV
    end

    # overides the default string type column
    def column(name, type, options)
      @column_names.merge! name.to_sym => [name, type, options]
    end

    def create_table
      f = File.open(@stream, 'r')
      instance.connection.create_table(instance.table_name, :temporary => true, :id => false) do |t|
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
    
    # use postgress COPY command using STDIN with CSV HEADER
    # reads chunks of 8192 bytes to save memory
    def pipe_it(l = "")
      raw = instance.connection.raw_connection
      raw.query "COPY #{instance.table_name} FROM STDIN WITH CSV HEADER"
      command = "cat #{@stream} #{seds_join}"
      f = IO.popen(command)
      begin
        while f.read(8192, l)
          raw.put_copy_data l
        end
      rescue EOFError
        f.close
      end
      raw.put_copy_end
    end

    def nulls(*args)
      @null_replacements = args
    end

    def seds(*args)
      @sed_commands = args
    end
    
    def symbolize(name)
      name.gsub(/ /,"_").gsub(/\W/, "").downcase.to_sym
    end
    
    # join together the sed commands to apply to stream
    def seds_join(commands = [])
      unless null_replacements.nil?
        commands << "| sed #{nulls_to_sed.join(" ")}"
      end
      unless sed_commands.nil?
        commands << "| sed #{sed_commands.join("| sed ")}"
      end
      commands.join(" ")
    end

    def nulls_to_sed
      @null_replacements.map do |null|
        "-e 's/#{null.source}//g'"
      end
    end
  end
end
