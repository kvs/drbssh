# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require 'rake'

Gem::Specification.new do |s|
	s.name        = "drbssh"
	s.version     = "0.5.2"
	s.platform    = Gem::Platform::RUBY
	s.authors     = %w(kvs)
	s.email       = %w(kvs@binarysolutions.dk)
	s.homepage    = "https://github.com/kvs/drbssh"
	s.summary     = "An SSH protocol driver for DRb"
	s.description = %q{Allows DRb to create and use an SSH-connection.}

	s.files            = `git ls-files`.split("\n")
	s.test_files       = `git ls-files -- {test,spec,features}/*`.split("\n")
	s.executables      = `git ls-files -- bin/*`.split("\n").map { |f| File.basename(f) }
	s.require_paths    = ["lib"]

	s.add_development_dependency "rspec", ["~> 2.8.0"]
	s.add_development_dependency "guard-rspec"
	s.add_development_dependency "vagrant"
	s.add_development_dependency "vagrant-proxyssh"
end
