module Theman
  class Object
    # create a new basic model object
    # ==== Parameters
    # * +table_name+ - the name of the table created by Theman::Agency
    # * +parent+ - optional parent object for the new basic model object
    #   usually ActiveRecord::Base
    # * +conn+ - optional pg connection
    # ==== Example
    #    my_model = Theman::Object.new(agent.table_name, ActiveRecord::Base)
    def self.new(table_name, parent = ::Object, conn = nil)
      Class.new(parent) do
        unless conn.nil?
          @@connection = conn
        end
        instance_eval <<-EOV, __FILE__, __LINE__ + 1
          set_table_name "#{table_name}"
          
          def table_name
            "#{table_name}"
          end
          
          def inspect
            "Agent (#{table_name})"
          end
        EOV
      end
    end
  end
end
