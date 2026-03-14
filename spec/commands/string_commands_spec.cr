require "../spec_helper"

Spectator.describe "String Commands" do
  let(db_manager) { Redis::DatabaseManager.new }
  let(db) { db_manager.get_or_create(0) }
  let(handler) { Redis::CommandHandler.new(db_manager) }
  let(conn) { TestConnection.new }

  def cmd(*args) : Array(Redis::RespValue)
    result = [] of Redis::RespValue
    args.each { |arg| result << b(arg) }
    result
  end

  describe "PING" do
    it "returns PONG" do
      handler.execute(cmd("PING"), conn)
      expect(conn.last_response).to eq("PONG")
    end

    it "returns argument if provided" do
      handler.execute(cmd("PING", "hello"), conn)
      expect(conn.last_response).to eq(b("hello"))
    end
  end

  describe "ECHO" do
    it "returns the message" do
      handler.execute(cmd("ECHO", "Hello World"), conn)
      expect(conn.last_response).to eq(b("Hello World"))
    end

    it "returns error without argument" do
      handler.execute(cmd("ECHO"), conn)
      expect(conn.errors.size).to eq(1)
    end
  end

  describe "SET and GET" do
    it "sets and gets a value" do
      handler.execute(cmd("SET", "mykey", "myvalue"), conn)
      expect(conn.last_response).to eq("OK")

      conn.clear
      handler.execute(cmd("GET", "mykey"), conn)
      expect(conn.last_response).to eq(b("myvalue"))
    end

    it "returns nil for non-existing key" do
      handler.execute(cmd("GET", "nonexistent"), conn)
      expect(conn.last_response).to be_nil
    end

    it "supports NX option (only if not exists)" do
      handler.execute(cmd("SET", "mykey", "value1"), conn)
      conn.clear
      handler.execute(cmd("SET", "mykey", "value2", "NX"), conn)
      expect(conn.last_response).to be_nil # NX failed

      handler.execute(cmd("GET", "mykey"), conn)
      expect(conn.last_response).to eq(b("value1")) # Unchanged
    end

    it "supports XX option (only if exists)" do
      handler.execute(cmd("SET", "mykey", "value1", "XX"), conn)
      expect(conn.last_response).to be_nil # XX failed - key doesn't exist

      handler.execute(cmd("SET", "mykey", "value1"), conn)
      conn.clear
      handler.execute(cmd("SET", "mykey", "value2", "XX"), conn)
      expect(conn.last_response).to eq("OK")
    end

    it "supports EX option (seconds)" do
      handler.execute(cmd("SET", "mykey", "value", "EX", "100"), conn)
      expect(conn.last_response).to eq("OK")
      expect(db.exists?(b("mykey"))).to be_true
    end

    it "supports PX option (milliseconds)" do
      handler.execute(cmd("SET", "mykey", "value", "PX", "100000"), conn)
      expect(conn.last_response).to eq("OK")
    end
  end

  describe "DEL" do
    it "deletes single key" do
      db.set(b("key1"), b("value1"))
      handler.execute(cmd("DEL", "key1"), conn)
      expect(conn.last_response).to eq(1_i64)
      expect(db.exists?(b("key1"))).to be_false
    end

    it "deletes multiple keys" do
      db.set(b("key1"), b("value1"))
      db.set(b("key2"), b("value2"))
      handler.execute(cmd("DEL", "key1", "key2"), conn)
      expect(conn.last_response).to eq(2_i64)
    end

    it "returns 0 for non-existing keys" do
      handler.execute(cmd("DEL", "nonexistent"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "EXISTS" do
    it "returns 1 for existing key" do
      db.set(b("key1"), b("value1"))
      handler.execute(cmd("EXISTS", "key1"), conn)
      expect(conn.last_response).to eq(1_i64)
    end

    it "returns 0 for non-existing key" do
      handler.execute(cmd("EXISTS", "nonexistent"), conn)
      expect(conn.last_response).to eq(0_i64)
    end

    it "counts multiple keys" do
      db.set(b("key1"), b("value1"))
      db.set(b("key2"), b("value2"))
      handler.execute(cmd("EXISTS", "key1", "key2", "key3"), conn)
      expect(conn.last_response).to eq(2_i64)
    end
  end

  describe "TYPE" do
    it "returns 'none' for non-existing key" do
      handler.execute(cmd("TYPE", "nonexistent"), conn)
      expect(conn.last_response).to eq("none")
    end

    it "returns 'string' for string key" do
      db.set(b("key"), b("value"))
      handler.execute(cmd("TYPE", "key"), conn)
      expect(conn.last_response).to eq("string")
    end
  end

  describe "APPEND" do
    it "appends to existing value" do
      db.set(b("key"), b("Hello"))
      handler.execute(cmd("APPEND", "key", " World"), conn)
      expect(conn.last_response).to eq(11_i64)
    end

    it "creates key if not exists" do
      handler.execute(cmd("APPEND", "key", "value"), conn)
      expect(conn.last_response).to eq(5_i64)
    end
  end

  describe "STRLEN" do
    it "returns string length" do
      db.set(b("key"), b("hello"))
      handler.execute(cmd("STRLEN", "key"), conn)
      expect(conn.last_response).to eq(5_i64)
    end

    it "returns 0 for non-existing key" do
      handler.execute(cmd("STRLEN", "nonexistent"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "INCR" do
    it "increments value by 1" do
      db.set(b("counter"), b("10"))
      handler.execute(cmd("INCR", "counter"), conn)
      expect(conn.last_response).to eq(11_i64)
    end

    it "creates key with value 1 if not exists" do
      handler.execute(cmd("INCR", "counter"), conn)
      expect(conn.last_response).to eq(1_i64)
    end
  end

  describe "INCRBY" do
    it "increments by specified amount" do
      db.set(b("counter"), b("10"))
      handler.execute(cmd("INCRBY", "counter", "5"), conn)
      expect(conn.last_response).to eq(15_i64)
    end

    it "returns error on overflow" do
      db.set(b("counter"), Int64::MAX.to_s.to_slice)
      handler.execute(cmd("INCRBY", "counter", "1"), conn)
      expect(conn.last_error).to contain("overflow")
    end

    it "returns error on underflow" do
      db.set(b("counter"), Int64::MIN.to_s.to_slice)
      handler.execute(cmd("INCRBY", "counter", "-1"), conn)
      expect(conn.last_error).to contain("overflow")
    end
  end

  describe "INCR overflow" do
    it "returns error when INCR would overflow" do
      db.set(b("counter"), Int64::MAX.to_s.to_slice)
      handler.execute(cmd("INCR", "counter"), conn)
      expect(conn.last_error).to contain("overflow")
    end
  end

  describe "DECR overflow" do
    it "returns error when DECR would underflow" do
      db.set(b("counter"), Int64::MIN.to_s.to_slice)
      handler.execute(cmd("DECR", "counter"), conn)
      expect(conn.last_error).to contain("overflow")
    end
  end

  describe "DECRBY overflow" do
    it "returns error on underflow" do
      db.set(b("counter"), Int64::MIN.to_s.to_slice)
      handler.execute(cmd("DECRBY", "counter", "1"), conn)
      expect(conn.last_error).to contain("overflow")
    end
  end

  describe "INCRBYFLOAT" do
    it "increments by float amount" do
      db.set(b("counter"), b("10.5"))
      handler.execute(cmd("INCRBYFLOAT", "counter", "0.5"), conn)
      result = conn.last_response
      expect(result).to be_a(Bytes)
      expect(String.new(result.as(Bytes))).to eq("11.0")
    end
  end

  describe "DECR" do
    it "decrements value by 1" do
      db.set(b("counter"), b("10"))
      handler.execute(cmd("DECR", "counter"), conn)
      expect(conn.last_response).to eq(9_i64)
    end
  end

  describe "DECRBY" do
    it "decrements by specified amount" do
      db.set(b("counter"), b("10"))
      handler.execute(cmd("DECRBY", "counter", "3"), conn)
      expect(conn.last_response).to eq(7_i64)
    end
  end

  describe "GETRANGE" do
    it "returns substring" do
      db.set(b("key"), b("Hello World"))
      handler.execute(cmd("GETRANGE", "key", "0", "4"), conn)
      expect(conn.last_response).to eq(b("Hello"))
    end

    it "supports negative indices" do
      db.set(b("key"), b("Hello World"))
      handler.execute(cmd("GETRANGE", "key", "-5", "-1"), conn)
      expect(conn.last_response).to eq(b("World"))
    end
  end

  describe "SETRANGE" do
    it "overwrites at offset" do
      db.set(b("key"), b("Hello World"))
      handler.execute(cmd("SETRANGE", "key", "6", "Redis"), conn)
      expect(conn.last_response).to eq(11_i64)
      expect(db.get(b("key"))).to eq(b("Hello Redis"))
    end
  end

  describe "MGET" do
    it "gets multiple values" do
      db.set(b("key1"), b("value1"))
      db.set(b("key2"), b("value2"))
      handler.execute(cmd("MGET", "key1", "key2", "key3"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      arr = result.as(Array)
      expect(arr[0]).to eq(b("value1"))
      expect(arr[1]).to eq(b("value2"))
      expect(arr[2]).to be_nil
    end
  end

  describe "MSET" do
    it "sets multiple key-value pairs" do
      handler.execute(cmd("MSET", "key1", "value1", "key2", "value2"), conn)
      expect(conn.last_response).to eq("OK")
      expect(db.get(b("key1"))).to eq(b("value1"))
      expect(db.get(b("key2"))).to eq(b("value2"))
    end
  end

  describe "MSETNX" do
    it "sets all if none exist" do
      handler.execute(cmd("MSETNX", "key1", "value1", "key2", "value2"), conn)
      expect(conn.last_response).to eq(1_i64)
    end

    it "sets none if any exist" do
      db.set(b("key1"), b("existing"))
      handler.execute(cmd("MSETNX", "key1", "value1", "key2", "value2"), conn)
      expect(conn.last_response).to eq(0_i64)
      expect(db.get(b("key2"))).to be_nil
    end
  end

  describe "SETNX" do
    it "sets if not exists" do
      handler.execute(cmd("SETNX", "key", "value"), conn)
      expect(conn.last_response).to eq(1_i64)
    end

    it "does not set if exists" do
      db.set(b("key"), b("existing"))
      handler.execute(cmd("SETNX", "key", "value"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "SETEX" do
    it "sets value with expiry in seconds" do
      handler.execute(cmd("SETEX", "key", "100", "value"), conn)
      expect(conn.last_response).to eq("OK")
      expect(db.exists?(b("key"))).to be_true
    end
  end

  describe "PSETEX" do
    it "sets value with expiry in milliseconds" do
      handler.execute(cmd("PSETEX", "key", "100000", "value"), conn)
      expect(conn.last_response).to eq("OK")
      expect(db.exists?(b("key"))).to be_true
    end
  end

  describe "GETEX" do
    it "returns the value and updates expiry" do
      db.set(b("key"), b("value"))

      handler.execute(cmd("GETEX", "key", "EX", "100"), conn)
      expect(conn.last_response).to eq(b("value"))
      expect(db.ttl(b("key"))).to be >= 95_i64
    end

    it "returns a syntax error for extra arguments" do
      db.set(b("key"), b("value"))

      handler.execute(cmd("GETEX", "key", "EX", "10", "junk"), conn)
      expect(conn.last_error).to contain("syntax error")
    end

    it "returns an error for non-positive EX values" do
      db.set(b("key"), b("value"))

      handler.execute(cmd("GETEX", "key", "EX", "0"), conn)
      expect(conn.last_error).to contain("invalid expire time")
    end
  end

  describe "SET option validation" do
    it "rejects KEEPTTL combined with EX" do
      db.set(b("key"), b("value"), Time.utc.to_unix_ms + 10_000)

      handler.execute(cmd("SET", "key", "new", "EX", "10", "KEEPTTL"), conn)
      expect(conn.last_error).to contain("syntax error")
    end

    it "rejects non-positive EX values" do
      handler.execute(cmd("SET", "key", "value", "EX", "0"), conn)
      expect(conn.last_error).to contain("invalid expire time")
    end
  end

  describe "GETDEL" do
    it "gets and deletes value" do
      db.set(b("key"), b("value"))
      handler.execute(cmd("GETDEL", "key"), conn)
      expect(conn.last_response).to eq(b("value"))
      expect(db.exists?(b("key"))).to be_false
    end

    it "returns nil for non-existing key" do
      handler.execute(cmd("GETDEL", "nonexistent"), conn)
      expect(conn.last_response).to be_nil
    end
  end

  describe "GETSET" do
    it "sets new value and returns old value" do
      db.set(b("key"), b("old"))
      handler.execute(cmd("GETSET", "key", "new"), conn)
      expect(conn.last_response).to eq(b("old"))
      expect(db.get(b("key"))).to eq(b("new"))
    end

    it "returns nil if key didn't exist" do
      handler.execute(cmd("GETSET", "key", "value"), conn)
      expect(conn.last_response).to be_nil
      expect(db.get(b("key"))).to eq(b("value"))
    end
  end
end
