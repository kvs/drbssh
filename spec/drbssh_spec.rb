# -*- mode: ruby; tab-width: 4; indent-tabs-mode: t -*-

require 'drbssh'

describe DRb::DRbSSHProtocol do
	it "responds to drbssh:// URIs" do
		described_class.uri_option("drbssh://localhost/ruby", {}).should eq [ "drbssh://localhost/ruby", nil ]
		described_class.uri_option("drbssh://localhost", {}).should eq [ "drbssh://localhost/", nil ]
		described_class.uri_option("drbssh://", {}).first.should match(/^drbssh:\/\//)

		expect { described_class.uri_option("druby://localhost/ruby", {}) }.to raise_exception DRb::DRbBadScheme
	end

	it "starts and stops when requested" do
		expect { DRb.current_server }.to raise_exception DRb::DRbServerNotFound
		DRb.start_service("drbssh://localhost")
		DRb.current_server.instance_variable_get("@protocol").should be_an_instance_of DRb::DRbSSHServer
		DRb.stop_service
		expect { DRb.current_server }.to raise_exception DRb::DRbServerNotFound
	end

	it "disallows running two drbssh-servers" do
		DRb.start_service("drbssh://localhost")
		expect { DRb.start_service("drbssh://localhost2") }.to raise_exception DRb::DRbConnError
	end

	it "creates DRbObjects with a URI pointed to itself" do
		DRb.start_service("drbssh://localhost")
		DRb::DRbObject.new({}.extend(DRbUndumped)).__drburi.should eq 'drbssh://localhost'
		DRb.stop_service
	end

	it "connects to a remote Ruby" do
		DRb.start_service("drbssh://localhost")
		drb = DRbObject.new_with_uri("drbssh://vagrant-drbssh/ruby")
		drb.should be_an_instance_of DRb::DRbObject
		drb.__drburi.should eq "drbssh://vagrant-drbssh/ruby"

		drb.eval("1+1").should eq 2
		drb.eval("`hostname`").should eq "vagrant-ubuntu-oneiric\n"
		drb.eval("{ foo: :bar }.extend DRb::DRbUndumped").should be_an_instance_of DRb::DRbObject
		DRb.stop_service
	end

	it "allows two-way communication" do
		DRb.start_service("drbssh://localhost")

		drb = DRbObject.new_with_uri("drbssh://vagrant-drbssh/ruby")

		remote_hash = drb.eval("@a = {}.extend(DRb::DRbUndumped)")

		# remote server gets started in main thread, with binding as front object.
		# server spawns thread to run in, and another thread to run client-connection in.
		# client connects, sends message,

		# Set local Proc in remote hash, re-fetch it and call it. Works without two-way comms
		remote_hash["a"] = lambda { return 1 }
		drb.eval("@a['a']").call.should eq 1

		# Call same Proc in a remote context. This means establishing a connection to the local side, since
		# the Proc is really a DRbObject on the remote side.
		drb.eval("@a['a'].call").should eq 1

		DRb.stop_service
	end

	it "reconnects when connection has been terminated" do
		DRb.start_service("drbssh://localhost")
		drb = DRbObject.new_with_uri("drbssh://vagrant-drbssh/ruby")
		drb.eval("`hostname`").should eq "vagrant-ubuntu-oneiric\n"
		pid = drb.eval('$$')

		expect { drb.eval('Kernel.exit! 0') }.to raise_exception IOError

		drb.eval('$$').should_not eq pid # should create new connection
	end
end
