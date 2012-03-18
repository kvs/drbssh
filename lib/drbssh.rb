# -*- mode: ruby; tab-width: 4; indent-tabs-mode: t -*-

require 'drb'

# DRb protocol handler for using a persistent SSH connection to a remote server. Creates a duplex
# connection so DRbUndumped objects can contact the initiating machine back through the same link.
#
# Contains all needed classes and no external dependencies, since this file will be read and sent
# to the remote end, to bootstrap its protocol support without requiring anything but Ruby
# installed.

module DRb

	class DRbSSHProtocol
		def self.server; @server; end

		# Open a client connection to the server at +uri+, using configuration +config+, and return it.
		def self.open(uri, config)
			raise DRbServerNotFound, "need to run a DRbSSH server first" if @server.nil? or @server.closed?

			DRb.thread['DRbSSHLocalClient'] || DRbSSHRemoteClient.new(uri, @server)
		end

		# Open a server listening at +uri+, using configuration +config+, and return it.
		def self.open_server(uri, config)
			@server = nil if !@server.nil? and @server.closed?

			raise DRbConnError, "server already running with different uri" if !@server.nil? and @server.uri != uri

			@server ||= DRbSSHServer.new(uri, config)
		end

		# Parse +uri+ into a [uri, option] pair.
		def self.uri_option(uri, config)
			host, path = split_uri(uri)
			[ "drbssh://#{host}/#{path}", nil ]
		end

		# Split URI into component pairs
		def self.split_uri(uri)
			if uri.match('^drbssh://([^/?]+)(?:/(.+))?$')
				[ $1, $2 ]
			else
				raise DRbBadScheme,uri unless uri =~ /^drbssh:/
				raise DRbBadURI, "can't parse uri: " + uri
			end
		end
	end
	DRbProtocol.add_protocol(DRbSSHProtocol)

	class DRbSSHClient
		attr_reader :receiveq
		attr_reader :sendq
		attr_reader :read_fd
		attr_reader :write_fd

		def initialize(server)
			@receiveq, @sendq = Queue.new, Queue.new
			@write_fd.sync = true

			server.client_queue.push(self)
		end

		def send_request(ref, msg_id, arg, b)
			@sendq.push(['req', [ref, msg_id, arg, b]])
		end

		def recv_reply
			@receiveq.pop
		end

		def alive?
			!self.read_fd.closed? && !self.write_fd.closed?
		end
	end

	# Class for connecting to a remote object.
	class DRbSSHRemoteClient < DRbSSHClient
		# Create an SSH-connection to +uri+, and spawn a server, so client has something to talk to
		def initialize(uri, server)
			# child-to-parent, parent-to-child
			ctp_rd, ctp_wr = IO.pipe
			ptc_rd, ptc_wr = IO.pipe

			host, cmd = DRbSSHProtocol.split_uri(uri)

			# Read the source-code for this file, and add bits for initialising the remote side.
			self_code = <<-EOT
				#{File.read(__FILE__)};
				DRb.start_service("#{uri}", binding)
				DRb.thread['DRbSSHLocalClient'] = DRb::DRbSSHLocalClient.new(DRb::DRbSSHProtocol.server)
				DRb.thread.join
			EOT

			if fork.nil?
				# child
				ctp_rd.close
				ptc_wr.close

				$stdin.reopen(ptc_rd)
				$stdout.reopen(ctp_wr)

				# Open a connection to a remote Ruby, and assume we can read stuff from STDIN, which will be written by
				# the parent.
				cmd = ['ruby'] if cmd.nil? or cmd.empty?
				exec("ssh", "-oBatchMode=yes", "-T", host, "exec", *cmd, '-rzlib', '-e', "\"eval STDIN.read(#{self_code.bytesize})\"")
				exit
			else
				# parent
				ctp_wr.close
				ptc_rd.close

				# Pump initial code into the remote Ruby-process, so a full two-way DRb-session can be established.
				ptc_wr.write(self_code)

				@read_fd, @write_fd = ctp_rd, ptc_wr

				super(server)
			end
		end

		def close
			self.read_fd.close
			self.write_fd.close
		end
	end

	# Class for connecting to a remote object.
	class DRbSSHLocalClient < DRbSSHClient
		# Create an SSH-connection to +uri+, and spawn a server, so client has something to talk to
		def initialize(server)
			@read_fd, @write_fd = $stdin, $stdout

			super(server)
		end

		def close
			Kernel.exit 0
		end
	end

	# Server running on local side
	class DRbSSHServer
		attr_reader :uri
		attr_reader :client_queue

		def initialize(uri, config)
			@uri = uri
			@config = config
			@client_queue = Queue.new
			@clients = []
		end

		def accept
			client = @client_queue.pop
			@clients << DRbSSHServerConn.new(uri, @config, client)
			@clients.last
		end

		def close
			@clients.map(&:close)
			@clients = nil
		end

		def closed?
			@clients.nil?
		end
	end

	# Per-connection class
	class DRbSSHServerConn
		attr_reader :uri

		def initialize(uri, config, client)
			@uri = uri
			@client = client
			@srv_requestq = Queue.new

			msg = DRbMessage.new(config)

			# Read-thread
			Thread.new do
				# read from client's in-fd, and delegate to #recv_request or client.recv_reply
				begin
					loop do
						type = msg.load(client.read_fd)
						if type == 'req'
							@srv_requestq.push(msg.recv_request(client.read_fd))
						else
							client.receiveq.push(msg.recv_reply(client.read_fd))
						end
					end
				rescue
					client.close
				end
			end

			# Write-thread
			Thread.new do
				begin
					loop do
						type, data = client.sendq.pop

						client.write_fd.write(msg.dump(type))

						if type == 'req'
							msg.send_request(client.write_fd, *data)
						else
							msg.send_reply(client.write_fd, *data)
						end
					end
				rescue
					client.close
				end
			end
		end

		# Close the FDs on the RemoteClient associated with this connection.
		def close
			@client.close
		end

		# Wait for a request to appear on the request-queue
		def recv_request
			@srv_requestq.pop
		end

		# Queue client-reply
		def send_reply(succ, result)
			@client.sendq.push(['rep', [succ, result]])
		end
	end
end
