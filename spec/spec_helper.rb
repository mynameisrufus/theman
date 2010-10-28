$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

require 'active_record'
require 'fileutils'
require 'logger'
require 'rspec'
require 'rspec/autorun'
require 'theman'

ActiveRecord::Base.configurations = YAML.load_file(File.join("spec", "database.yml"))
FileUtils.mkdir_p "#{Dir.pwd}/log"
logfile = "#{Dir.pwd}/log/database.log"
ActiveRecord::Base.logger = Logger.new(File.open(logfile, 'w'))
ActiveRecord::Base.establish_connection(ActiveRecord::Base.configurations.fetch('test'))

