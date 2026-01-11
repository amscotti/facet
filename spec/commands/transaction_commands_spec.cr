require "../spec_helper"

Spectator.describe "Transaction Commands" do
  let(db_manager) { Redis::DatabaseManager.new }
  let(db) { db_manager.get_or_create(0) }
  let(handler) { Redis::CommandHandler.new(db_manager) }
  let(conn) { TestConnection.new }

  def cmd(*args) : Array(Redis::RespValue)
    result = [] of Redis::RespValue
    args.each { |arg| result << b(arg) }
    result
  end

  describe "MULTI" do
    it "starts transaction mode" do
      handler.execute(cmd("MULTI"), conn)
      expect(conn.last_response).to eq("OK")
      expect(conn.in_transaction?).to be_true
    end

    it "returns error if already in transaction" do
      handler.execute(cmd("MULTI"), conn)
      conn.clear
      handler.execute(cmd("MULTI"), conn)
      expect(conn.errors.size).to eq(1)
      expect(conn.last_error).to contain("nested")
    end
  end

  describe "EXEC" do
    it "executes queued commands and returns array of results" do
      handler.execute(cmd("MULTI"), conn)
      conn.clear
      handler.execute(cmd("SET", "x", "1"), conn)
      expect(conn.last_response).to eq("QUEUED")
      handler.execute(cmd("INCR", "x"), conn)
      expect(conn.last_response).to eq("QUEUED")
      handler.execute(cmd("GET", "x"), conn)
      expect(conn.last_response).to eq("QUEUED")
      conn.clear

      handler.execute(cmd("EXEC"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))

      arr = result.as(Array)
      expect(arr.size).to eq(3)
      expect(arr[0]).to eq("OK")   # SET response
      expect(arr[1]).to eq(2_i64)  # INCR response
      expect(arr[2]).to eq(b("2")) # GET response
    end

    it "returns error if not in transaction" do
      handler.execute(cmd("EXEC"), conn)
      expect(conn.errors.size).to eq(1)
      expect(conn.last_error).to contain("without MULTI")
    end

    it "exits transaction mode after execution" do
      handler.execute(cmd("MULTI"), conn)
      handler.execute(cmd("SET", "x", "1"), conn)
      handler.execute(cmd("EXEC"), conn)

      expect(conn.in_transaction?).to be_false
    end

    it "executes empty transaction" do
      handler.execute(cmd("MULTI"), conn)
      conn.clear
      handler.execute(cmd("EXEC"), conn)

      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to eq(0)
    end
  end

  describe "DISCARD" do
    it "cancels transaction and clears queue" do
      handler.execute(cmd("MULTI"), conn)
      handler.execute(cmd("SET", "x", "1"), conn)
      handler.execute(cmd("SET", "y", "2"), conn)
      conn.clear

      handler.execute(cmd("DISCARD"), conn)
      expect(conn.last_response).to eq("OK")
      expect(conn.in_transaction?).to be_false

      # Keys should not be set
      handler.execute(cmd("GET", "x"), conn)
      expect(conn.last_response).to be_nil
    end

    it "returns error if not in transaction" do
      handler.execute(cmd("DISCARD"), conn)
      expect(conn.errors.size).to eq(1)
      expect(conn.last_error).to contain("without MULTI")
    end
  end

  describe "Command queueing" do
    it "queues commands instead of executing during transaction" do
      handler.execute(cmd("MULTI"), conn)
      conn.clear

      handler.execute(cmd("SET", "key", "value"), conn)
      expect(conn.last_response).to eq("QUEUED")

      # Key should not exist yet
      expect(db.exists?(b("key"))).to be_false
    end

    it "returns QUEUED for each queued command" do
      handler.execute(cmd("MULTI"), conn)
      conn.clear

      handler.execute(cmd("SET", "a", "1"), conn)
      expect(conn.last_response).to eq("QUEUED")

      handler.execute(cmd("SET", "b", "2"), conn)
      expect(conn.last_response).to eq("QUEUED")

      handler.execute(cmd("GET", "a"), conn)
      expect(conn.last_response).to eq("QUEUED")
    end

    it "handles multiple data types in transaction" do
      handler.execute(cmd("MULTI"), conn)
      handler.execute(cmd("SET", "string", "value"), conn)
      handler.execute(cmd("LPUSH", "list", "item"), conn)
      handler.execute(cmd("HSET", "hash", "field", "value"), conn)
      handler.execute(cmd("SADD", "set", "member"), conn)
      handler.execute(cmd("ZADD", "zset", "1", "member"), conn)
      conn.clear

      handler.execute(cmd("EXEC"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(5)
    end
  end

  describe "Transaction isolation" do
    it "changes are visible after EXEC" do
      handler.execute(cmd("MULTI"), conn)
      handler.execute(cmd("SET", "key", "value"), conn)
      handler.execute(cmd("EXEC"), conn)

      expect(db.get(b("key"))).to eq(b("value"))
    end

    it "changes are not visible before EXEC" do
      handler.execute(cmd("MULTI"), conn)
      handler.execute(cmd("SET", "key", "value"), conn)

      expect(db.get(b("key"))).to be_nil

      handler.execute(cmd("EXEC"), conn)
      expect(db.get(b("key"))).to eq(b("value"))
    end
  end

  describe "WATCH" do
    it "returns OK when watching keys" do
      handler.execute(cmd("WATCH", "key1", "key2"), conn)
      expect(conn.last_response).to eq("OK")
    end

    it "allows transaction to complete when watched key is unchanged" do
      db.set(b("counter"), b("10"))

      handler.execute(cmd("WATCH", "counter"), conn)
      handler.execute(cmd("MULTI"), conn)
      handler.execute(cmd("INCR", "counter"), conn)
      conn.clear
      handler.execute(cmd("EXEC"), conn)

      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array)[0]).to eq(11_i64)
      expect(db.get(b("counter"))).to eq(b("11"))
    end

    it "aborts transaction when watched key is modified" do
      db.set(b("counter"), b("10"))

      handler.execute(cmd("WATCH", "counter"), conn)

      # Simulate another client modifying the key
      db.set(b("counter"), b("999"))

      handler.execute(cmd("MULTI"), conn)
      handler.execute(cmd("INCR", "counter"), conn)
      conn.clear
      handler.execute(cmd("EXEC"), conn)

      # EXEC returns nil when WATCH fails
      expect(conn.last_response).to be_nil

      # The original modification should remain
      expect(db.get(b("counter"))).to eq(b("999"))
    end

    it "aborts transaction when any watched key is modified" do
      db.set(b("key1"), b("value1"))
      db.set(b("key2"), b("value2"))

      handler.execute(cmd("WATCH", "key1", "key2"), conn)

      # Modify only key2
      db.set(b("key2"), b("modified"))

      handler.execute(cmd("MULTI"), conn)
      handler.execute(cmd("SET", "key1", "new_value"), conn)
      conn.clear
      handler.execute(cmd("EXEC"), conn)

      # EXEC should return nil
      expect(conn.last_response).to be_nil

      # key1 should not have been modified
      expect(db.get(b("key1"))).to eq(b("value1"))
    end

    it "returns error when called inside MULTI" do
      handler.execute(cmd("MULTI"), conn)
      conn.clear
      handler.execute(cmd("WATCH", "key"), conn)
      expect(conn.errors.size).to eq(1)
      expect(conn.last_error).to contain("inside MULTI")
    end

    it "can watch non-existent keys" do
      handler.execute(cmd("WATCH", "nonexistent"), conn)
      expect(conn.last_response).to eq("OK")

      # Create the key - this should cause WATCH to fail
      db.set(b("nonexistent"), b("value"))

      handler.execute(cmd("MULTI"), conn)
      handler.execute(cmd("GET", "nonexistent"), conn)
      conn.clear
      handler.execute(cmd("EXEC"), conn)

      # Transaction should be aborted
      expect(conn.last_response).to be_nil
    end

    it "clears watched keys after EXEC" do
      db.set(b("key"), b("value"))

      handler.execute(cmd("WATCH", "key"), conn)
      handler.execute(cmd("MULTI"), conn)
      handler.execute(cmd("GET", "key"), conn)
      handler.execute(cmd("EXEC"), conn)

      # Modify key after EXEC
      db.set(b("key"), b("modified"))

      # New transaction should succeed since watches were cleared
      handler.execute(cmd("MULTI"), conn)
      handler.execute(cmd("GET", "key"), conn)
      conn.clear
      handler.execute(cmd("EXEC"), conn)

      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
    end

    it "clears watched keys after DISCARD" do
      db.set(b("key"), b("value"))

      handler.execute(cmd("WATCH", "key"), conn)
      handler.execute(cmd("MULTI"), conn)
      handler.execute(cmd("DISCARD"), conn)

      # Modify key after DISCARD
      db.set(b("key"), b("modified"))

      # New transaction should succeed since watches were cleared
      handler.execute(cmd("MULTI"), conn)
      handler.execute(cmd("GET", "key"), conn)
      conn.clear
      handler.execute(cmd("EXEC"), conn)

      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
    end
  end

  describe "UNWATCH" do
    it "returns OK" do
      handler.execute(cmd("UNWATCH"), conn)
      expect(conn.last_response).to eq("OK")
    end

    it "clears watched keys" do
      db.set(b("key"), b("value"))

      handler.execute(cmd("WATCH", "key"), conn)

      # Modify the key
      db.set(b("key"), b("modified"))

      # Unwatch the key
      handler.execute(cmd("UNWATCH"), conn)

      # Now transaction should succeed despite key modification
      handler.execute(cmd("MULTI"), conn)
      handler.execute(cmd("GET", "key"), conn)
      conn.clear
      handler.execute(cmd("EXEC"), conn)

      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array)[0]).to eq(b("modified"))
    end

    it "can be called without prior WATCH" do
      handler.execute(cmd("UNWATCH"), conn)
      expect(conn.last_response).to eq("OK")
    end
  end
end
