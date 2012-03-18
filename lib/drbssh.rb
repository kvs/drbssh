# -*- mode: ruby; tab-width: 4; indent-tabs-mode: t -*-

require 'drb'

# Open an SSH connection to a remote server, and run a DRb server there, too. Make them communicate through
# stdin/stdout, and make the connection two-way while we're at it.
module DRb
	class DRbSSHProtocol
		# Open a client connection to the server at +uri+, using configuration +config+, and return it.
		def self.open(uri, config)
			if @server.nil?
				raise DRbServerNotFound, "need to run a DRbSSH server first"
			else
				_, _, option = self.split_uri(uri)
				if option == 'local'
					DRbSSHLocalClient.new(uri, config, @server)
				else
					DRbSSHRemoteClient.new(uri, config, @server)
				end
			end
		end

		# Open a server listening at +uri+, using configuration +config+, and return it.
		def self.open_server(uri, config)
			# Ensure just one DRbSSH-server is active, since more doesn't make sense.
			if @server.nil? or @server.closed?
				_, _, option = self.split_uri(uri)
				if option == 'local'
					@server = DRbSSHLocalServer.new(uri, config)
				else
					@server = DRbSSHRemoteServer.new(uri, config)
				end
			else
				if @server.uri != uri
					raise DRbConnError, "server already running with different uri"
				else
					@server
				end
			end
		end

		# Parse +uri+ into a [uri, option] pair.
		def self.uri_option(uri, config)
			host, path, option = self.split_uri(uri)
			[ "drbssh://#{host}/#{path}", option ]
		end

		# Split URI into component pairs
		def self.split_uri(uri)
			if uri.match('^drbssh://([^/?]+)(?:/([^?]+))?(?:\?(.+))?$')
				[ $1, $2, $3 ]
			else
				raise DRbBadScheme,uri unless uri =~ /^drbssh:/
				raise DRbBadURI, "can't parse uri: " + uri
			end
		end
	end
	DRbProtocol.add_protocol(DRbSSHProtocol)

	# Class for connecting to a remote object.
	class DRbSSHRemoteClient
		# Create an SSH-connection to +uri+, and spawn a server, so client has something to talk to
		def initialize(uri, config, server)
			@uri = uri
			@config = config
			@server = server

			# child-to-parent, parent-to-child
			ctp_rd, ctp_wr = IO.pipe
			ptc_rd, ptc_wr = IO.pipe

			host, cmd, _ = DRbSSHProtocol.split_uri(uri)

			# Read the source-code for this file, and add bits for initialising a remote client.
			self_code = File.read(__FILE__)
			self_code += "\nDRb.start_service('#{uri}', binding); DRb.thread.join\n"

			@child_pid = fork

			if @child_pid.nil?
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

				ptc_wr.sync = true

				# Pump initial code into the remote Ruby-process, so a full two-way DRb-session can be established.
				ptc_wr.write(self_code)

				@receiveq = Queue.new
				@sendq = Queue.new

				@server.client_queue.push({ receiveq: @receiveq, sendq: @sendq, read_fd: ctp_rd, write_fd: ptc_wr })
			end
		end

		def send_request(ref, msg_id, arg, b)
			@sendq.push(['req', [ref, msg_id, arg, b]])
		end

		def recv_reply
			@receiveq.pop
		end

		def alive?
			true # FIXME
		end

		def close
			Process.kill("QUIT", @child_pid)
		end
	end

	# Server running on local side
	class DRbSSHLocalServer
		attr_reader :uri
		attr_reader :client_queue

		def initialize(uri, config)
			@uri = uri
			@config = config
			@client_queue = Queue.new
			@closed = false
		end

		def accept
			client = @client_queue.pop
			DRbSSHLocalServerConn.new(uri, @config, client)
		end

		def close
			# FIXME: shutdown all RemoteClients
			@closed = true
		end
		def closed?; @closed; end
	end

	class DRbSSHLocalServerConn
		attr_reader :uri

		def initialize(uri, config, client)
			@uri = uri
			@client = client
			@srv_requestq = Queue.new

			msg = DRbMessage.new(config)

			# Read-thread
			Thread.new do
				# read from client's in-fd, and delegate to #recv_request or client.recv_reply
				loop do
					type = msg.load(client[:read_fd])
					if type == 'req'
						@srv_requestq.push(msg.recv_request(client[:read_fd]))
					else
						client[:receiveq].push(msg.recv_reply(client[:read_fd]))
					end
				end
			end

			# Write-thread
			Thread.new do
				loop do
					type, data = client[:sendq].pop

					client[:write_fd].write(msg.dump(type))

					if type == 'req'
						msg.send_request(client[:write_fd], *data)
					else
						msg.send_reply(client[:write_fd], *data)
					end
				end
			end
		end

		def close
			true # FIXME: close fds?
		end

		# Receive
		def recv_request
			@srv_requestq.pop
		end

		# Sends reply back on $
		def send_reply(succ, result)
			@client[:sendq].push(['rep', [succ, result]])
		end
	end

	# Class for connecting to a ?local URI - used on remote side to talk to local side.
	class DRbSSHLocalClient
		# The local client should relay its information back through the +server+, since it can't establish its
		# own connection
		def initialize(uri, config, server)
			@uri = uri
			@config = config
			@server = server
			@msg = DRbMessage.new(config)
		end

		def send_request(ref, msg_id, arg, b)
			@server.cl_sendq.push(['req', [ref, msg_id, arg, b]])
		end

		def recv_reply
			@server.cl_recvq.pop
		end

		def alive?
			true # FIXME: check if @server is alive
		end

		# Nothing to do - we simply stop piggy-backing.
		def close
			true
		end
	end

	# Server running on remote side
	class DRbSSHRemoteServer
		attr_reader :uri
		attr_reader :cl_sendq
		attr_reader :cl_recvq

		def initialize(uri, config)
			@uri = uri
			msg = DRbMessage.new(config)
			@srv_requestq = Queue.new
			@cl_sendq = Queue.new
			@cl_recvq = Queue.new

			$stdout.sync = true

			# Read-thread
			Thread.new do
				loop do
					type = msg.load($stdin)
					if type == 'req'
						@srv_requestq.push(msg.recv_request($stdin))
					else
						@cl_recvq.push(msg.recv_reply($stdin))
					end
				end
			end

			# Write-thread
			Thread.new do
				loop do
					type, data = @cl_sendq.pop

					$stdout.write(msg.dump(type))

					if type == 'req'
						msg.send_request($stdout, *data)
					else
						msg.send_reply($stdout, *data)
					end
				end
			end
		end

		# Accept incoming connection once, and sleep after that.
		# DRb creates a thread on client connection, and thus returns to waiting on #accept immediately.
		def accept
			if @accepted
				sleep 60 while true
			else
				@accepted = self
			end
		end

		# Handles both closure of client and server.
		def close
			[$stdin, $stdout].each { |fd| fd.close }
			Kernel.exit 0
		end

		# Receives a request on $stdin
		def recv_request
			@srv_requestq.pop
		end

		# Sends reply back on $stdout
		def send_reply(succ, result)
			@cl_sendq.push(['rep', [succ, result]])
		end
	end
end
