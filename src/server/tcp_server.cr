require "socket"
require "log"
require "./connection"
require "../storage/database_manager"
require "../commands/command_handler"

module Redis
  class Server
    MAX_CONNECTIONS = 10_000
    DEFAULT_HOST    = "0.0.0.0"
    DEFAULT_PORT    = 6379

    @server : TCPServer?
    @running : Bool = true
    @active_connections : Atomic(Int32)
    @connection_pool : Channel(TCPSocket)?
    @db_manager : DatabaseManager
    @handler : CommandHandler
    @logger : Log

    def initialize(@host = DEFAULT_HOST, @port = DEFAULT_PORT)
      @active_connections = Atomic(Int32).new(0)
      @db_manager = DatabaseManager.new
      @handler = CommandHandler.new(@db_manager)
      @logger = Log.for("facet.server")
    end

    def start : Nil
      @server = TCPServer.new(@host, @port, backlog: 4096)
      @connection_pool = Channel(TCPSocket).new(MAX_CONNECTIONS)

      puts "Redis server listening on #{@host}:#{@port}"

      accept_loop
      run_server
    end

    def stop : Nil
      @running = false
      @server.try &.close
    end

    private def accept_loop : Nil
      server = @server
      pool = @connection_pool
      return unless server && pool

      spawn(name: "acceptor") do
        while @running
          if client = server.accept?
            if @active_connections.get >= MAX_CONNECTIONS
              @logger.warn { "Max connections reached (#{MAX_CONNECTIONS}), rejecting client" }
              client.close
              next
            end

            @active_connections.add(1)
            pool.send(client)
          end
        end
      end
    end

    private def run_server : Nil
      num_workers = (ENV["FACET_WORKERS"]? || "4").to_i

      num_workers.times do |i|
        spawn(name: "worker-#{i}") do
          worker_loop
        end
      end
    end

    private def worker_loop : Nil
      pool = @connection_pool
      return unless pool

      while @running
        socket = pool.receive?
        next unless socket
        tcp_socket = socket.as(TCPSocket)

        spawn(name: "handler-#{Fiber.current.object_id}") do
          begin
            client = Connection.new(tcp_socket)
            begin
              handle_client(client)
            ensure
              client.close rescue nil
            end
          rescue ex
            @logger.error(exception: ex) { "Error during connection setup" }
          ensure
            @active_connections.sub(1)
          end
        end
        Fiber.yield # Allow spawned handlers to run
      end
    end

    private def handle_client(client : Connection) : Nil
      parser = RespParser.new(client.socket)

      loop do
        break if client.closed?

        begin
          value = parser.parse
          return unless value

          process_command(value, client)
        rescue ex : ParseError
          @logger.debug { "Parse error from #{client.remote_address}: #{ex.message}" }
          begin
            client.send_error(ex.message) unless client.closed?
          rescue
            # Client disconnected, ignore
          end
          break
        rescue ex : IO::Error
          # Client disconnected, silently close
          break
        rescue ex : ConnectionError
          # Write failed, client disconnected
          break
        rescue ex : Exception
          @logger.error(exception: ex) { "Internal error handling client #{client.remote_address}" }
          begin
            client.send_error("Internal error") unless client.closed?
          rescue
            # Ignore write errors
          end
          break
        end
      end
    end

    private def process_command(value : RespValue, client : Connection) : Nil
      return unless value.is_a?(Array(RespValue))
      commands = value.as(Array(RespValue))
      return if commands.empty?
      @handler.execute(commands, client)
    end
  end
end
