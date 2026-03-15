require "spectator"
require "../src/lib"

# Helper to create Bytes from string
def b(str : String) : Bytes
  str.to_slice
end

# Helper to create IO::Memory with RESP data
def resp_io(data : String) : IO::Memory
  IO::Memory.new(data)
end

# Test connection that captures responses for verification
class TestConnection < Redis::Connection
  getter responses : Array(Redis::RespValue)
  getter errors : Array(String)
  @output : IO::Memory
  @@test_socket : TCPSocket?
  @@test_server : TCPServer?

  def self.setup_test_socket : TCPSocket
    # Reuse socket if already created and not closed
    if socket = @@test_socket
      return socket unless socket.closed?
    end

    # Create a long-lived server for all tests
    server = @@test_server ||= TCPServer.new("127.0.0.1", 0)
    port = server.local_address.port

    # Accept connection in background
    spawn do
      if server.accept?
        # Keep the server-side connection open
        spawn { sleep 1.hour } # Keep fiber alive
      end
    end
    Fiber.yield

    socket = TCPSocket.new("127.0.0.1", port)
    @@test_socket = socket
    socket
  end

  def initialize
    @output = IO::Memory.new
    socket = TestConnection.setup_test_socket
    super(socket, "test:0", Time.utc)
    @responses = [] of Redis::RespValue
    @errors = [] of String
  end

  def send_response(value : Redis::RespValue) : Nil
    @responses << value
  end

  def send_simple_string(str : String) : Nil
    @responses << str
  end

  def send_error(message : String?) : Nil
    @errors << (message || "unknown error")
  end

  def send_ok : Nil
    send_simple_string("OK")
  end

  def send_nil : Nil
    @responses << nil
  end

  def send_integer(num : Int64) : Nil
    @responses << num
  end

  def send_bulk_string(bytes : Bytes?) : Nil
    @responses << bytes
  end

  def send_array(arr : Array(Redis::RespValue)) : Nil
    @responses << arr
  end

  def send_bytes_array(arr : Array(Bytes)) : Nil
    result = Array(Redis::RespValue).new(arr.size)
    arr.each { |value| result << value.as(Redis::RespValue) }
    @responses << result
  end

  def send_cursor_bytes_array(cursor : Bytes, items : Array(Bytes)) : Nil
    result = Array(Redis::RespValue).new(2)
    result << cursor.as(Redis::RespValue)

    item_arr = Array(Redis::RespValue).new(items.size)
    items.each { |value| item_arr << value.as(Redis::RespValue) }
    result << item_arr.as(Redis::RespValue)

    @responses << result
  end

  def last_response : Redis::RespValue
    @responses.last
  end

  def last_error : String
    @errors.last
  end

  def clear : Nil
    @responses.clear
    @errors.clear
  end
end
