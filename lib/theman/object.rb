module Theman
  class Object
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
