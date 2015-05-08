class Fluent::Rds_LogInput < Fluent::Input
  Fluent::Plugin.register_input("rds_log", self)

  config_param :tag,      :string
  config_param :host,     :string,  :default => nil
  config_param :port,     :integer, :default => 3306
  config_param :username, :string,  :default => nil
  config_param :password, :string,  :default => nil
  config_param :log_type, :string,  :default => nil
  config_param :refresh_interval, :integer, :default => 30
  config_param :auto_reconnect, :bool, :default => true
  config_param :add_host, :bool, :default => false

   def initialize
    super
    require 'mysql2'
  end

  def configure(conf)
    super
    if @log_type.empty?
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
  def connect_all
    @clients = {}
    @host.split(',').each do |host|
      @clients[host] = connect(host)
    end
  end

  def connect(host)
    begin
      $log.info "fluent-plugin-rds-log: connecting RDS [#{host}]"
      client = Mysql2::Client.new({
        :host => host,
        :port => @port,
        :username => @username,
        :password => @password,
        :reconnect => @auto_reconnect,
        :database => 'mysql'
      })
      $log.info "fluent-plugin-rds-log: connected RDS [#{host}]"
      return client
    rescue
      $log.error "fluent-plugin-rds-log: cannot connect RDS [#{host}]"
    end
  end

  def watch
    connect_all
    while true
      sleep @refresh_interval
      @host.split(',').each do |host|
        output(host)
      end
    end
  end

  def output(host)
    client = @clients[host]
    if client.nil?
      client = connect(host)
      if client.nil?
        return
      end
    end

    client.query("CALL mysql.rds_rotate_#{@log_type}")

    output_log_data = client.query("SELECT * FROM mysql.#{@log_type}_backup", :cast => false)
    output_log_data.each do |row|
      row.delete_if{|key,value| value == ''}
      row['host'] = host if @add_host
      Fluent::Engine.emit(tag, Fluent::Engine.now, row)
    end
  end
end
