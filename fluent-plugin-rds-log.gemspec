# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |gem|
  gem.name          = "fluent-plugin-rds-log"
  gem.version       = "0.1.4"
  gem.authors       = ["shinsaka"]
  gem.email         = ["shinx1265@gmail.com"]
  gem.description   = "Amazon RDS slow_log and general_log input plugin for Fluent event collector"
  gem.homepage      = "https://github.com/shinsaka/fluent-plugin-rds-log"
  gem.summary       = gem.description
  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.add_dependency "fluentd", "~> 0.10.30"
  gem.add_dependency "mysql2",  "~> 0.3.11"
  gem.add_development_dependency "rake", ">= 10.0.4"
end
