require "socket"
require "../protocol/resp_parser"
require "../protocol/resp_serializer"
require "../storage/database_manager"

module Redis
  class Connection
    getter socket : TCPSocket
    getter remote_address : String
    getter connected_at : Time
    property current_db_id : DatabaseId = 0

    # Transaction state
    property? in_transaction : Bool = false
    getter queued_commands : Array(Array(RespValue))

    # WATCH state: maps (db_id, key) to version at watch time
    getter watched_keys : Hash({DatabaseId, Bytes}, Int64)

    def initialize(@socket : TCPSocket)
      @remote_address = begin
        socket.remote_address.to_s
      rescue
        "unknown"
      end
      @connected_at = Time.utc
      @current_db_id = 0
      @in_transaction = false
      @queued_commands = [] of Array(RespValue)
      @watched_keys = Hash({DatabaseId, Bytes}, Int64).new
    end

    # Protected constructor for testing - allows subclasses to bypass socket setup
    protected def initialize(@socket : TCPSocket, @remote_address : String, @connected_at : Time)
      @current_db_id = 0
      @in_transaction = false
      @queued_commands = [] of Array(RespValue)
      @watched_keys = Hash({DatabaseId, Bytes}, Int64).new
    end

    # Transaction management
    def start_transaction : Nil
      @in_transaction = true
      @queued_commands.clear
    end

    def queue_command(commands : Array(RespValue)) : Nil
      @queued_commands << commands
    end

    def discard_transaction : Nil
      @in_transaction = false
      @queued_commands.clear
      @watched_keys.clear
    end

    def finish_transaction : Array(Array(RespValue))
      commands = @queued_commands.dup
      @in_transaction = false
      @queued_commands.clear
      @watched_keys.clear
      commands
    end

    # WATCH/UNWATCH management
    def watch_key(db_id : DatabaseId, key : Bytes, version : Int64) : Nil
      @watched_keys[{db_id, key}] = version
    end

    def unwatch_all : Nil
      @watched_keys.clear
    end

    def has_watched_keys? : Bool
      !@watched_keys.empty?
    end

    def write(data : Bytes) : Nil
      @socket.write(data)
    rescue ex : IO::Error
      raise ConnectionError.new("Write failed: #{ex.message}")
    end

    def write_string(str : String) : Nil
      write(str.to_slice)
    end

    def read_line : String?
      @socket.gets
    end

    def read_bytes(count : Int32) : Bytes?
      buffer = Bytes.new(count)
      @socket.read_fully(buffer)
      buffer
    rescue ex : IO::Error
      nil
    end

    def flush : Nil
      @socket.flush
    end

    def close : Nil
      @socket.close
    end

    def closed? : Bool
      @socket.closed?
    end

    def send_response(value : RespValue) : Nil
      write(RespSerializer.serialize(value))
      flush
    end

    def send_simple_string(str : String) : Nil
      send_response(str)
    end

    def send_error(message : String?) : Nil
      write("-ERR #{message || "unknown error"}\r\n".to_slice)
      flush
    end

    def send_ok : Nil
      send_simple_string("OK")
    end

    def send_nil : Nil
      send_response(nil)
    end

    def send_integer(num : Int64) : Nil
      send_response(num)
    end

    def send_bulk_string(bytes : Bytes?) : Nil
      send_response(bytes)
    end

    def send_array(arr : Array(RespValue)) : Nil
      send_response(arr)
    end
  end

  # Wrapper connection that captures responses instead of sending them
  # Used by EXEC to collect results from queued commands
  class CapturingConnection < Connection
    property captured_response : RespValue = nil
    @real_client : Connection

    def initialize(@real_client : Connection)
      super(@real_client.socket, @real_client.remote_address, @real_client.connected_at)
      @current_db_id = @real_client.current_db_id
      @captured_response = nil
    end

    def send_response(value : RespValue) : Nil
      @captured_response = value
    end

    def send_simple_string(str : String) : Nil
      @captured_response = str
    end

    def send_error(message : String?) : Nil
      # Store error as a special format
      @captured_response = "-ERR #{message || "unknown error"}".to_slice
    end

    def send_ok : Nil
      @captured_response = "OK"
    end

    def send_nil : Nil
      @captured_response = nil
    end

    def send_integer(num : Int64) : Nil
      @captured_response = num
    end

    def send_bulk_string(bytes : Bytes?) : Nil
      @captured_response = bytes
    end

    def send_array(arr : Array(RespValue)) : Nil
      @captured_response = arr
    end
  end

  class ConnectionError < Exception
  end
end
