require "../spec_helper"

Spectator.describe "Server Commands" do
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

    it "returns argument when provided" do
      handler.execute(cmd("PING", "hello"), conn)
      expect(conn.last_response).to eq(b("hello"))
    end
  end

  describe "ECHO" do
    it "echoes the message" do
      handler.execute(cmd("ECHO", "Hello"), conn)
      expect(conn.last_response).to eq(b("Hello"))
    end

    it "returns error without argument" do
      handler.execute(cmd("ECHO"), conn)
      expect(conn.errors.size).to eq(1)
    end
  end

  describe "COMMAND" do
    it "returns OK" do
      handler.execute(cmd("COMMAND"), conn)
      expect(conn.last_response).to eq("OK")
    end
  end

  describe "DBSIZE" do
    it "returns 0 for empty database" do
      handler.execute(cmd("DBSIZE"), conn)
      expect(conn.last_response).to eq(0_i64)
    end

    it "returns correct count" do
      db.set(b("key1"), b("value1"))
      db.set(b("key2"), b("value2"))
      handler.execute(cmd("DBSIZE"), conn)
      expect(conn.last_response).to eq(2_i64)
    end

    it "does not count expired keys" do
      db.set(b("expired"), b("value"), Time.utc.to_unix_ms - 1)
      db.set(b("active"), b("value"))

      handler.execute(cmd("DBSIZE"), conn)
      expect(conn.last_response).to eq(1_i64)
    end
  end

  describe "CONFIG" do
    it "returns supported config values" do
      handler.execute(cmd("CONFIG", "GET", "appendonly"), conn)
      expect(conn.last_response).to eq([b("appendonly"), b("no")] of Redis::RespValue)
    end

    it "supports glob patterns" do
      handler.execute(cmd("CONFIG", "GET", "a*"), conn)
      result = conn.last_response.as(Array)
      expect(result).to contain(b("appendonly"))
      expect(result).to contain(b("no"))
    end

    it "returns an empty array for unknown config keys" do
      handler.execute(cmd("CONFIG", "GET", "nomatch"), conn)
      expect(conn.last_response).to eq([] of Redis::RespValue)
    end
  end

  describe "FLUSHDB" do
    it "clears all keys" do
      db.set(b("key1"), b("value1"))
      db.set(b("key2"), b("value2"))
      handler.execute(cmd("FLUSHDB"), conn)
      expect(conn.last_response).to eq("OK")
      expect(db.size).to eq(0)
    end
  end

  describe "KEYS" do
    before_each do
      db.set(b("key1"), b("value1"))
      db.set(b("key2"), b("value2"))
      db.set(b("other"), b("value3"))
    end

    it "returns all keys with *" do
      handler.execute(cmd("KEYS", "*"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to eq(3)
    end

    it "does not include expired keys" do
      db.set(b("expired"), b("value"), Time.utc.to_unix_ms - 1)

      handler.execute(cmd("KEYS", "*"), conn)
      result = conn.last_response.as(Array)
      expect(result).not_to contain(b("expired"))
    end

    it "filters keys with pattern" do
      handler.execute(cmd("KEYS", "key*"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to eq(2)
    end

    it "supports character classes and escaped specials" do
      db.set(b("hello"), b("1"))
      db.set(b("hallo"), b("1"))
      db.set(b("h?llo"), b("1"))

      handler.execute(cmd("KEYS", "h[ae]llo"), conn)
      result = conn.last_response.as(Array)
      expect(result).to contain(b("hello"))
      expect(result).to contain(b("hallo"))
      expect(result).not_to contain(b("h?llo"))

      handler.execute(cmd("KEYS", "h\\?llo"), conn)
      escaped = conn.last_response.as(Array)
      expect(escaped).to eq([b("h?llo")] of Redis::RespValue)
    end

    it "returns empty array when no matches" do
      handler.execute(cmd("KEYS", "nomatch*"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to eq(0)
    end
  end

  describe "INFO" do
    it "returns server information" do
      handler.execute(cmd("INFO"), conn)
      result = conn.last_response
      expect(result).to be_a(Bytes)
      info = String.new(result.as(Bytes))
      expect(info).to contain("# Server")
      expect(info).to contain("facet_version")
      expect(info).to contain("uptime_in_seconds")
    end

    it "returns specific section when requested" do
      handler.execute(cmd("INFO", "server"), conn)
      result = conn.last_response
      expect(result).to be_a(Bytes)
      info = String.new(result.as(Bytes))
      expect(info).to contain("# Server")
      expect(info).to contain("facet_version")
    end

    it "returns keyspace section" do
      db.set(b("key1"), b("value1"))
      handler.execute(cmd("INFO", "keyspace"), conn)
      result = conn.last_response
      expect(result).to be_a(Bytes)
      info = String.new(result.as(Bytes))
      expect(info).to contain("# Keyspace")
      expect(info).to contain("db0:keys=")
    end

    it "returns memory section" do
      handler.execute(cmd("INFO", "memory"), conn)
      result = conn.last_response
      expect(result).to be_a(Bytes)
      info = String.new(result.as(Bytes))
      expect(info).to contain("# Memory")
      expect(info).to contain("used_memory")
    end

    it "returns stats section" do
      handler.execute(cmd("INFO", "stats"), conn)
      result = conn.last_response
      expect(result).to be_a(Bytes)
      info = String.new(result.as(Bytes))
      expect(info).to contain("# Stats")
      expect(info).to contain("total_keys")
      expect(info).to contain("total_databases")
    end
  end

  describe "TIME" do
    it "returns server time as array" do
      handler.execute(cmd("TIME"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      arr = result.as(Array)
      expect(arr.size).to eq(2)

      # First element is unix timestamp in seconds
      seconds = String.new(arr[0].as(Bytes))
      expect(seconds.to_i64?).not_to be_nil

      # Second element is microseconds
      microseconds = String.new(arr[1].as(Bytes))
      expect(microseconds.to_i64?).not_to be_nil
    end

    it "returns reasonable timestamp" do
      handler.execute(cmd("TIME"), conn)
      result = conn.last_response.as(Array)
      seconds = String.new(result[0].as(Bytes)).to_i64

      # Should be after 2020 (1577836800)
      expect(seconds).to be > 1_577_836_800_i64
    end
  end

  describe "Unknown commands" do
    it "returns error for unknown command" do
      handler.execute(cmd("UNKNOWNCOMMAND"), conn)
      expect(conn.errors.size).to eq(1)
      expect(conn.last_error).to contain("unknown command")
    end
  end
end
