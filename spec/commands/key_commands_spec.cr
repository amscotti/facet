require "../spec_helper"

Spectator.describe "Key Commands" do
  let(db_manager) { Redis::DatabaseManager.new }
  let(db) { db_manager.get_or_create(0) }
  let(handler) { Redis::CommandHandler.new(db_manager) }
  let(conn) { TestConnection.new }

  def cmd(*args) : Array(Redis::RespValue)
    result = [] of Redis::RespValue
    args.each { |arg| result << b(arg) }
    result
  end

  describe "EXPIRE" do
    it "sets TTL on existing key and returns 1" do
      db.set(b("mykey"), b("myvalue"))
      handler.execute(cmd("EXPIRE", "mykey", "100"), conn)
      expect(conn.last_response).to eq(1_i64)
    end

    it "returns 0 for non-existing key" do
      handler.execute(cmd("EXPIRE", "nonexistent", "100"), conn)
      expect(conn.last_response).to eq(0_i64)
    end

    it "updates existing TTL" do
      db.set(b("mykey"), b("myvalue"))
      handler.execute(cmd("EXPIRE", "mykey", "100"), conn)
      conn.clear
      handler.execute(cmd("EXPIRE", "mykey", "200"), conn)
      expect(conn.last_response).to eq(1_i64)
    end

    it "deletes the key immediately when TTL is zero" do
      db.set(b("mykey"), b("myvalue"))

      handler.execute(cmd("EXPIRE", "mykey", "0"), conn)
      expect(conn.last_response).to eq(1_i64)
      expect(db.exists?(b("mykey"))).to be_false
    end
  end

  describe "EXPIREAT" do
    it "sets TTL with Unix timestamp" do
      db.set(b("mykey"), b("myvalue"))
      future_time = (Time.utc.to_unix + 100).to_s
      handler.execute(cmd("EXPIREAT", "mykey", future_time), conn)
      expect(conn.last_response).to eq(1_i64)
    end

    it "returns 0 for non-existing key" do
      future_time = (Time.utc.to_unix + 100).to_s
      handler.execute(cmd("EXPIREAT", "nonexistent", future_time), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "PEXPIRE" do
    it "sets TTL in milliseconds" do
      db.set(b("mykey"), b("myvalue"))
      handler.execute(cmd("PEXPIRE", "mykey", "100000"), conn)
      expect(conn.last_response).to eq(1_i64)
    end

    it "returns 0 for non-existing key" do
      handler.execute(cmd("PEXPIRE", "nonexistent", "100000"), conn)
      expect(conn.last_response).to eq(0_i64)
    end

    it "deletes the key immediately when TTL is non-positive" do
      db.set(b("mykey"), b("myvalue"))

      handler.execute(cmd("PEXPIRE", "mykey", "0"), conn)
      expect(conn.last_response).to eq(1_i64)
      expect(db.exists?(b("mykey"))).to be_false
    end
  end

  describe "PEXPIREAT" do
    it "sets TTL with Unix timestamp in milliseconds" do
      db.set(b("mykey"), b("myvalue"))
      future_time = (Time.utc.to_unix_ms + 100_000).to_s
      handler.execute(cmd("PEXPIREAT", "mykey", future_time), conn)
      expect(conn.last_response).to eq(1_i64)
    end
  end

  describe "TTL" do
    it "returns -2 for non-existing key" do
      handler.execute(cmd("TTL", "nonexistent"), conn)
      expect(conn.last_response).to eq(-2_i64)
    end

    it "returns -1 for key without TTL" do
      db.set(b("mykey"), b("myvalue"))
      handler.execute(cmd("TTL", "mykey"), conn)
      expect(conn.last_response).to eq(-1_i64)
    end

    it "returns remaining seconds for key with TTL" do
      db.set(b("mykey"), b("myvalue"))
      handler.execute(cmd("EXPIRE", "mykey", "100"), conn)
      conn.clear
      handler.execute(cmd("TTL", "mykey"), conn)
      result = conn.last_response.as(Int64)
      expect(result).to be >= 95
      expect(result).to be <= 100
    end
  end

  describe "PTTL" do
    it "returns -2 for non-existing key" do
      handler.execute(cmd("PTTL", "nonexistent"), conn)
      expect(conn.last_response).to eq(-2_i64)
    end

    it "returns -1 for key without TTL" do
      db.set(b("mykey"), b("myvalue"))
      handler.execute(cmd("PTTL", "mykey"), conn)
      expect(conn.last_response).to eq(-1_i64)
    end

    it "returns remaining milliseconds for key with TTL" do
      db.set(b("mykey"), b("myvalue"))
      handler.execute(cmd("PEXPIRE", "mykey", "100000"), conn)
      conn.clear
      handler.execute(cmd("PTTL", "mykey"), conn)
      result = conn.last_response.as(Int64)
      expect(result).to be >= 95_000
      expect(result).to be <= 100_000
    end
  end

  describe "PERSIST" do
    it "removes TTL and returns 1" do
      db.set(b("mykey"), b("myvalue"))
      handler.execute(cmd("EXPIRE", "mykey", "100"), conn)
      conn.clear
      handler.execute(cmd("PERSIST", "mykey"), conn)
      expect(conn.last_response).to eq(1_i64)

      # Verify TTL is removed
      handler.execute(cmd("TTL", "mykey"), conn)
      expect(conn.last_response).to eq(-1_i64)
    end

    it "returns 0 if key has no TTL" do
      db.set(b("mykey"), b("myvalue"))
      handler.execute(cmd("PERSIST", "mykey"), conn)
      expect(conn.last_response).to eq(0_i64)
    end

    it "returns 0 for non-existing key" do
      handler.execute(cmd("PERSIST", "nonexistent"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "RENAME" do
    it "renames existing key" do
      db.set(b("foo"), b("bar"))
      handler.execute(cmd("RENAME", "foo", "baz"), conn)
      expect(conn.last_response).to eq("OK")

      handler.execute(cmd("GET", "baz"), conn)
      expect(conn.last_response).to eq(b("bar"))

      handler.execute(cmd("GET", "foo"), conn)
      expect(conn.last_response).to be_nil
    end

    it "overwrites destination if exists" do
      db.set(b("foo"), b("value1"))
      db.set(b("bar"), b("value2"))
      handler.execute(cmd("RENAME", "foo", "bar"), conn)
      expect(conn.last_response).to eq("OK")

      handler.execute(cmd("GET", "bar"), conn)
      expect(conn.last_response).to eq(b("value1"))
    end

    it "preserves TTL of source key" do
      db.set(b("foo"), b("bar"))
      handler.execute(cmd("EXPIRE", "foo", "100"), conn)
      conn.clear
      handler.execute(cmd("RENAME", "foo", "baz"), conn)
      expect(conn.last_response).to eq("OK")

      handler.execute(cmd("TTL", "baz"), conn)
      result = conn.last_response.as(Int64)
      expect(result).to be >= 95
    end

    it "returns error for non-existing source" do
      handler.execute(cmd("RENAME", "nonexistent", "bar"), conn)
      expect(conn.errors.size).to eq(1)
      expect(conn.last_error).to contain("no such key")
    end
  end

  describe "RENAMENX" do
    it "renames if destination doesn't exist" do
      db.set(b("foo"), b("bar"))
      handler.execute(cmd("RENAMENX", "foo", "baz"), conn)
      expect(conn.last_response).to eq(1_i64)

      handler.execute(cmd("GET", "baz"), conn)
      expect(conn.last_response).to eq(b("bar"))
    end

    it "returns 0 if destination exists" do
      db.set(b("foo"), b("value1"))
      db.set(b("bar"), b("value2"))
      handler.execute(cmd("RENAMENX", "foo", "bar"), conn)
      expect(conn.last_response).to eq(0_i64)

      # Original key should still exist
      handler.execute(cmd("GET", "foo"), conn)
      expect(conn.last_response).to eq(b("value1"))
    end

    it "returns error for non-existing source" do
      handler.execute(cmd("RENAMENX", "nonexistent", "bar"), conn)
      expect(conn.errors.size).to eq(1)
      expect(conn.last_error).to contain("no such key")
    end
  end
end
