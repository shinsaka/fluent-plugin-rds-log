require 'fluent/input'
require 'mysql2'

class Fluent::Plugin::Rds_LogInput < Fluent::Plugin::Input
  Fluent::Plugin.register_input("rds_log", self)

  # Define `router` method to support v0.10.57 or earlier
  unless method_defined?(:router)
    define_method("router") { Fluent::Engine }
  end

  config_param :tag,      :string
  config_param :host,     :string,  :default => nil
  config_param :port,     :integer, :default => 3306
  config_param :username, :string,  :default => nil
  config_param :password, :string,  :default => nil, :secret => true
  config_param :log_type, :string,  :default => nil
  config_param :refresh_interval, :integer, :default => 30
  config_param :auto_reconnect, :bool, :default => true
  config_param :add_host, :bool, :default => false
  config_param :where, :string, :default => nil

  def initialize
    super
  end

  def configure(conf)
    super
    if @log_type.nil?
      $log.error "fluent-plugin-rds-log: missing parameter log_type is {slow_log|general_log}"
    end
  end

  def start
    super
    @watcher = Thread.new(&method(:watch))
  end

  def shutdown
    super
    @watcher.terminate
    @watcher.join
  end

  private

  def connect(host)
    begin
      client = Mysql2::Client.new({
        :host => host,
        :port => @port,
        :username => @username,
        :password => @password,
        :reconnect => @auto_reconnect,
        :database => 'mysql'
      })
      return client
    rescue
      $log.error "fluent-plugin-rds-log: cannot connect RDS [#{host}]"
    end
    return nil
  end

  def watch
    while true
      @host.split(',').each do |host|
        output(host)
      end
      sleep @refresh_interval
    end
  end

  def output(host)
    client = connect(host)
    return if client.nil?
    output_log_data = query(client)
    return if output_log_data.nil?
    output_log_data.each do |row|
      row.delete_if{|key,value| value == ''}
      row['host'] = host if @add_host
      router.emit(tag, Fluent::Engine.now, row)
    end
    client.close
  end

  def query(client)
    begin
      client.query("CALL mysql.rds_rotate_#{@log_type}")
      sql = "SELECT * FROM mysql.#{@log_type}_backup"
      unless @where.nil?
        sql += " WHERE #{@where}"
      end
      output_log_data = client.query(sql, :cast => false)
    rescue Exception => e
      $log.error "fluent-plugin-rds-log: ERROR Occurred!"
      $log.error "#{e.message}\n#{e.backtrace.join("\n")}"
      return nil
    end
  end
end
