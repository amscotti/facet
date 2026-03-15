require "../spec_helper"

Spectator.describe Redis::Connection do
  let(socket) { TCPServer.new("127.0.0.1", 0) }
  let(port) { socket.local_address.port }

  after_each do
    socket.close rescue nil
  end

  describe "#initialize" do
    it "initializes with a TCPSocket" do
      server = TCPServer.new("127.0.0.1", 0)
      spawn { server.accept? }
      client = TCPSocket.new("127.0.0.1", server.local_address.port)

      conn = Redis::Connection.new(client)

      expect(conn.socket).to eq(client)
      expect(conn.remote_address).to contain("127.0.0.1")
      expect(conn.connected_at).to be_a(Time)
      expect(conn.current_db_id).to eq(0)
      expect(conn.in_transaction?).to be_false
      expect(conn.queued_commands).to be_empty

      client.close
      server.close
    end

    it "captures remote address even on error" do
      client_socket = TCPSocket.new("127.0.0.1", port)
      # Close socket immediately to simulate error condition
      client_socket.remote_address rescue nil

      # The connection should still initialize with "unknown" address
      expect { Redis::Connection.new(client_socket) }.not_to raise_error

      client_socket.close
    end
  end

  describe "#current_db_id" do
    it "defaults to 0" do
      conn = TestConnection.new
      expect(conn.current_db_id).to eq(0)
    end

    it "can be changed" do
      conn = TestConnection.new
      conn.current_db_id = 5
      expect(conn.current_db_id).to eq(5)
    end
  end

  describe "transaction management" do
    describe "#start_transaction" do
      it "sets in_transaction to true" do
        conn = TestConnection.new
        expect(conn.in_transaction?).to be_false

        conn.start_transaction

        expect(conn.in_transaction?).to be_true
      end

      it "clears queued commands" do
        conn = TestConnection.new
        cmd = [b("SET"), b("key1"), b("val1")] of Redis::RespValue
        conn.queue_command(cmd)
        expect(conn.queued_commands.size).to eq(1)

        conn.start_transaction

        expect(conn.queued_commands).to be_empty
      end
    end

    describe "#queue_command" do
      it "adds command to queue" do
        conn = TestConnection.new
        cmd = [b("SET"), b("key1"), b("val1")] of Redis::RespValue

        conn.queue_command(cmd)

        expect(conn.queued_commands).to contain(cmd)
      end

      it "can queue multiple commands" do
        conn = TestConnection.new
        cmd1 = [b("SET"), b("key1"), b("val1")] of Redis::RespValue
        cmd2 = [b("GET"), b("key1")] of Redis::RespValue

        conn.queue_command(cmd1)
        conn.queue_command(cmd2)

        expect(conn.queued_commands.size).to eq(2)
      end
    end

    describe "#discard_transaction" do
      it "sets in_transaction to false" do
        conn = TestConnection.new
        conn.start_transaction
        expect(conn.in_transaction?).to be_true

        conn.discard_transaction

        expect(conn.in_transaction?).to be_false
      end

      it "clears queued commands" do
        conn = TestConnection.new
        conn.start_transaction
        cmd = [b("SET"), b("key1"), b("val1")] of Redis::RespValue
        conn.queue_command(cmd)
        expect(conn.queued_commands.size).to eq(1)

        conn.discard_transaction

        expect(conn.queued_commands).to be_empty
      end
    end

    describe "#finish_transaction" do
      it "returns queued commands" do
        conn = TestConnection.new
        conn.start_transaction
        cmd1 = [b("SET"), b("key1"), b("val1")] of Redis::RespValue
        cmd2 = [b("GET"), b("key1")] of Redis::RespValue
        conn.queue_command(cmd1)
        conn.queue_command(cmd2)

        result = conn.finish_transaction

        expect(result.size).to eq(2)
        expect(result).to contain(cmd1)
        expect(result).to contain(cmd2)
      end

      it "sets in_transaction to false" do
        conn = TestConnection.new
        conn.start_transaction
        expect(conn.in_transaction?).to be_true

        conn.finish_transaction

        expect(conn.in_transaction?).to be_false
      end

      it "clears queued commands" do
        conn = TestConnection.new
        conn.start_transaction
        cmd = [b("SET"), b("key1"), b("val1")] of Redis::RespValue
        conn.queue_command(cmd)
        conn.finish_transaction

        expect(conn.queued_commands).to be_empty
      end
    end
  end

  describe "response methods" do
    let(conn) { TestConnection.new }

    describe "#send_ok" do
      it "sends OK string" do
        conn.send_ok
        expect(conn.last_response).to eq("OK")
      end
    end

    describe "#send_simple_string" do
      it "sends string as response" do
        conn.send_simple_string("Hello")
        expect(conn.last_response).to eq("Hello")
      end
    end

    describe "#send_error" do
      it "sends error message" do
        conn.send_error("Something went wrong")
        expect(conn.last_error).to eq("Something went wrong")
      end

      it "sends default error message if nil" do
        conn.send_error(nil)
        expect(conn.last_error).to eq("unknown error")
      end
    end

    describe "#send_nil" do
      it "sends nil" do
        conn.send_nil
        expect(conn.last_response).to be_nil
      end
    end

    describe "#send_integer" do
      it "sends integer" do
        conn.send_integer(42_i64)
        expect(conn.last_response).to eq(42_i64)
      end
    end

    describe "#send_bulk_string" do
      it "sends bytes" do
        conn.send_bulk_string(b("hello"))
        expect(conn.last_response).to eq(b("hello"))
      end

      it "sends nil" do
        conn.send_bulk_string(nil)
        expect(conn.last_response).to be_nil
      end
    end

    describe "#send_array" do
      it "sends array of RespValue" do
        arr = [b("key1"), b("key2"), b("key3")] of Redis::RespValue
        conn.send_array(arr)
        expect(conn.last_response).to eq(arr)
      end

      it "sends empty array" do
        arr = [] of Redis::RespValue
        conn.send_array(arr)
        expect(conn.last_response).to eq(arr)
      end
    end
  end

  describe "#closed?" do
    it "returns false for open connection" do
      conn = TestConnection.new
      expect(conn.closed?).to be_false
    end

    it "returns true for closed connection" do
      conn = TestConnection.new
      conn.close
      expect(conn.closed?).to be_true
    end
  end
end

Spectator.describe Redis::CapturingConnection do
  describe "#initialize" do
    it "wraps a real connection" do
      real_conn = TestConnection.new
      cap_conn = Redis::CapturingConnection.new(real_conn)

      expect(cap_conn.socket).to eq(real_conn.socket)
      expect(cap_conn.remote_address).to eq(real_conn.remote_address)
      expect(cap_conn.connected_at).to eq(real_conn.connected_at)
      expect(cap_conn.current_db_id).to eq(real_conn.current_db_id)
    end
  end

  describe "response capturing" do
    let(real_conn) { TestConnection.new }
    let(cap_conn) { Redis::CapturingConnection.new(real_conn) }

    describe "#captured_response" do
      it "captures send_response" do
        cap_conn.send_response("test")
        expect(cap_conn.captured_response).to eq("test")
      end

      it "captures send_simple_string" do
        cap_conn.send_simple_string("OK")
        expect(cap_conn.captured_response).to eq("OK")
      end

      it "captures send_error" do
        cap_conn.send_error("Error message")
        response = cap_conn.captured_response
        expect(response).to be_a(Redis::RespError)
        expect(response.as(Redis::RespError).message).to eq("Error message")
      end

      it "captures send_ok" do
        cap_conn.send_ok
        expect(cap_conn.captured_response).to eq("OK")
      end

      it "captures send_nil" do
        cap_conn.send_nil
        expect(cap_conn.captured_response).to be_nil
      end

      it "captures send_integer" do
        cap_conn.send_integer(123_i64)
        expect(cap_conn.captured_response).to eq(123_i64)
      end

      it "captures send_bulk_string" do
        cap_conn.send_bulk_string(b("data"))
        expect(cap_conn.captured_response).to eq(b("data"))
      end

      it "captures send_array" do
        arr = [b("a"), b("b")] of Redis::RespValue
        cap_conn.send_array(arr)
        expect(cap_conn.captured_response).to eq(arr)
      end
    end
  end
end
