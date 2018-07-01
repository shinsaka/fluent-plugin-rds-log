require_relative '../helper'

class Rds_LogInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  DEFAULT_CONFIG = {
    host: 'endpoint.abcdefghijkl.ap-northeast-1.rds.amazonaws.com',
    username: 'testuser',
    password: 'testpass',
    refresh_interval: 30,
    auto_reconnect: true,
    tag: 'rds-general-log',
    add_host: true,
    where: 'argument',
  }


  def parse_config(conf = {})
    ''.tap{|s| conf.each { |k, v| s << "#{k} #{v}\n" } }
  end

  def create_driver(conf = DEFAULT_CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::Plugin::Rds_LogInput).configure(parse_config conf)
  end

  def iam_info_url
    'http://169.254.169.254/latest/meta-data/iam/security-credentials/'
  end

  def use_iam_role
    stub_request(:get, iam_info_url)
      .to_return(status: [200, 'OK'], body: "hostname")
    stub_request(:get, "#{iam_info_url}hostname")
      .to_return(status: [200, 'OK'],
                 body: {
                   "AccessKeyId" => "dummy",
                   "SecretAccessKey" => "secret",
                   "Token" => "token"
                 }.to_json)
  end

  def test_configure
    use_iam_role
    d = create_driver
    assert_equal 'endpoint.abcdefghijkl.ap-northeast-1.rds.amazonaws.com', d.instance.host
    assert_equal 'testuser', d.instance.username
    assert_equal 'testpass', d.instance.password
    assert_equal 30, d.instance.refresh_interval
    assert_equal true, d.instance.auto_reconnect
    assert_equal 'rds-general-log', d.instance.tag
    assert_equal true, d.instance.add_host
    assert_equal 'argument', d.instance.where
  end
end
