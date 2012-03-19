# -*- mode: ruby; tab-width: 4; indent-tabs-mode: t -*-

require 'drb'
require 'socket'
require 'timeout'

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
			host ||= Socket.gethostname
			[ "drbssh://#{host}/#{path}", nil ]
		end

		# Split URI into component pairs
		def self.split_uri(uri)
			if uri.match('^drbssh://([^/?]+)?/?(?:(.+))?$')
				[ $1, $2 ]
			else
				raise DRbBadScheme,uri unless uri =~ /^drbssh:/
				raise DRbBadURI, "can't parse uri: " + uri
			end
		end
	end
	DRbProtocol.add_protocol(DRbSSHProtocol)


	# Base class for the DRbSSH clients.
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
			reply = @receiveq.pop
			if reply.is_a? Exception
				self.close
				raise reply
			else
				reply
			end
		end

		def alive?
			!self.read_fd.closed? && !self.write_fd.closed?
		end
	end


	# DRbSSH remote client - does the heavy lifting of SSH'ing to a remote server and bootstrapping
	# its Ruby interpreter for DRb communication.
	class DRbSSHRemoteClient < DRbSSHClient
		# Create an SSH-connection to +uri+, and spawn a server, so client has something to talk to
		def initialize(uri, server)
			# child-to-parent, parent-to-child
			ctp_rd, ctp_wr = IO.pipe
			ptc_rd, ptc_wr = IO.pipe

			host, cmd = DRbSSHProtocol.split_uri(uri)

			# Read the source-code for this file, and add bits for initialising the remote side.
			# Since DRbSSHServer is doing all the filedescriptor read/writing, we need to start
			# a DRbSSHLocalClient immediately.
			self_code = <<-EOT
				#{File.read(__FILE__)};
				DRb.start_service("#{uri}", binding)
				DRb.thread['DRbSSHLocalClient'] = DRb::DRbSSHLocalClient.new(DRb::DRbSSHProtocol.server)
				DRb.thread.join
			EOT

			# Fork to create an SSH child-process
			if fork.nil?
				# In child - cleanup filehandles, reopen stdin/stdout, and exec ssh to connect to remote

				ctp_rd.close
				ptc_wr.close

				$stdin.reopen(ptc_rd)
				$stdout.reopen(ctp_wr)

				cmd = ['ruby'] if cmd.nil? or cmd.empty?

				# exec Ruby on remote end, and read bootstrap code.
				exec("ssh", "-oBatchMode=yes", "-T", host, "exec", *cmd, '-e', "\"eval STDIN.read(#{self_code.bytesize})\"")
				exit
			else
				# In parent - cleanup filehandles, write bootstrap-code, and hand over to superclass.

				ctp_wr.close
				ptc_rd.close

				# Bootstrap remote Ruby.
				ptc_wr.write(self_code)

				@read_fd, @write_fd = ctp_rd, ptc_wr

				super(server)
			end
		end

		# Close client.
		def close
			# Closing the filedescriptors should trigger an IOError in the server-thread
			# waiting, which makes it close the client attached.
			self.read_fd.close unless self.read_fd.closed?
			self.write_fd.close unless self.write_fd.closed?
		end
	end


	# DRbSSH client used to contact local objects - used on remote side.
	class DRbSSHLocalClient < DRbSSHClient
		# Use stdin/stdout to talk with the local side of an SSH-connection.
		def initialize(server)
			@read_fd, @write_fd = $stdin, $stdout

			super(server)
		end

		# Kill Ruby if client is asked to close.
		def close
			Kernel.exit 0
		end
	end


	# Common DRb protocol server for DRbSSH. Waits on incoming clients on a thread-safe `Queue`,
	# and spawns a new connection handler for each.
	class DRbSSHServer
		attr_reader :uri
		attr_reader :client_queue

		# Create new server.
		def initialize(uri, config)
			@uri = uri
			@config = config
			@client_queue = Queue.new
			@clients = []
		end

		# Wait for clients to register themselves on the client_queue.
		def accept
			client = @client_queue.pop
			@clients << DRbSSHServerConn.new(uri, @config, client)
			@clients.last
		end

		# Close server by closing all clients.
		def close
			@clients.map(&:close)
			@clients = nil
		end

		# Server is closed if +close+ has been called earlier.
		def closed?
			@clients.nil?
		end
	end


	# DRbSSH protocol server per-connection class. Handles client-to-client communications
	# by utilizing thread-safe Queue's for the input and output, and by adding a small
	# 'rep' or 'req' packet before sending, so we can have two-way DRb duplexed over a
	# single pair of filedescriptors.
	class DRbSSHServerConn
		attr_reader :uri

		# Create a new server-connection for the specified +client+.
		def initialize(uri, config, client)
			@uri = uri
			@client = client
			@srv_requestq = Queue.new

			msg = DRbMessage.new(config)

			# Read-thread
			Thread.new do
				# Read from client, and delegate request/reply to the correct place.
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
					client.receiveq.push($!)
					@srv_requestq.push($!)
				end
			end

			# Write-thread
			Thread.new do
				# Wait for outgoing data on send queue, and add header-packet before
				# writing.
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
					client.receiveq.push($!)
					@srv_requestq.push($!)
				end
			end
		end

		# Delegate shutdown to client.
		def close
			return unless @client.alive?

			Timeout::timeout(15) do
				sleep 0.1 until @client.sendq.empty?
			end rescue nil

			@client.close
		end

		# Wait for a request to appear on the request-queue
		def recv_request
			reply = @srv_requestq.pop
			if reply.is_a? Exception
				self.close
				raise reply
			else
				reply
			end
		end

		# Queue client-reply
		def send_reply(succ, result)
			@client.sendq.push(['rep', [succ, result]])
		end
	end
end
