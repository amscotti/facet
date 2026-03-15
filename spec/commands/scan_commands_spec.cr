require "../spec_helper"

Spectator.describe "SCAN Commands" do
  let(db_manager) { Redis::DatabaseManager.new }
  let(db) { db_manager.get_or_create(0) }
  let(handler) { Redis::CommandHandler.new(db_manager) }
  let(conn) { TestConnection.new }

  def cmd(*args) : Array(Redis::RespValue)
    result = [] of Redis::RespValue
    args.each { |arg| result << b(arg) }
    result
  end

  describe "SCAN" do
    it "starts with cursor 0 and returns keys" do
      db.set(b("key1"), b("value1"))
      db.set(b("key2"), b("value2"))
      db.set(b("key3"), b("value3"))

      handler.execute(cmd("SCAN", "0"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))

      arr = result.as(Array)
      expect(arr.size).to eq(2) # cursor + keys array
    end

    it "returns cursor 0 when complete" do
      db.set(b("k1"), b("v1"))
      db.set(b("k2"), b("v2"))

      handler.execute(cmd("SCAN", "0"), conn)
      result = conn.last_response.as(Array)
      cursor = String.new(result[0].as(Bytes))
      # With small dataset, should complete in one pass
      expect(cursor).to eq("0")
    end

    it "filters keys with MATCH pattern" do
      db.set(b("user:1"), b("alice"))
      db.set(b("user:2"), b("bob"))
      db.set(b("post:1"), b("hello"))

      handler.execute(cmd("SCAN", "0", "MATCH", "user:*"), conn)
      result = conn.last_response.as(Array)
      keys = result[1].as(Array)

      expect(keys.size).to eq(2)
    end

    it "supports character classes and escaped specials in MATCH" do
      db.set(b("hello"), b("1"))
      db.set(b("hallo"), b("1"))
      db.set(b("h?llo"), b("1"))

      handler.execute(cmd("SCAN", "0", "MATCH", "h[ae]llo"), conn)
      result = conn.last_response.as(Array)
      keys = result[1].as(Array)
      expect(keys).to contain(b("hello"))
      expect(keys).to contain(b("hallo"))
      expect(keys).not_to contain(b("h?llo"))

      handler.execute(cmd("SCAN", "0", "MATCH", "h\\?llo"), conn)
      escaped = conn.last_response.as(Array)[1].as(Array)
      expect(escaped).to eq([b("h?llo")] of Redis::RespValue)
    end

    it "respects COUNT hint" do
      10.times do |i|
        db.set(b("key#{i}"), b("value#{i}"))
      end

      handler.execute(cmd("SCAN", "0", "COUNT", "3"), conn)
      result = conn.last_response.as(Array)
      keys = result[1].as(Array)

      expect(keys.size).to be <= 6 # Count is a hint, may return up to 2x
    end

    it "continues iteration with returned cursor" do
      20.times do |i|
        db.set(b("k#{i}"), b("v#{i}"))
      end

      handler.execute(cmd("SCAN", "0", "COUNT", "5"), conn)
      result = conn.last_response.as(Array)
      cursor = String.new(result[0].as(Bytes))

      if cursor != "0"
        handler.execute(cmd("SCAN", cursor, "COUNT", "5"), conn)
        result2 = conn.last_response.as(Array)
        expect(result2).to be_a(Array(Redis::RespValue))
      end
    end

    it "returns an error for an invalid cursor" do
      handler.execute(cmd("SCAN", "nope"), conn)
      expect(conn.last_error).to contain("invalid cursor")
    end

    it "returns an error for invalid COUNT" do
      handler.execute(cmd("SCAN", "0", "COUNT", "0"), conn)
      expect(conn.last_error).to contain("syntax error")
    end
  end

  describe "HSCAN" do
    it "iterates over hash fields" do
      handler.execute(cmd("HSET", "myhash", "field1", "value1", "field2", "value2"), conn)
      conn.clear

      handler.execute(cmd("HSCAN", "myhash", "0"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))

      arr = result.as(Array)
      expect(arr.size).to eq(2) # cursor + items array
    end

    it "returns field-value pairs" do
      handler.execute(cmd("HSET", "myhash", "f1", "v1", "f2", "v2"), conn)
      conn.clear

      handler.execute(cmd("HSCAN", "myhash", "0"), conn)
      result = conn.last_response.as(Array)
      items = result[1].as(Array)

      # Should have pairs: [field1, value1, field2, value2]
      expect(items.size).to eq(4)
    end

    it "returns empty for non-existing key" do
      handler.execute(cmd("HSCAN", "nonexistent", "0"), conn)
      result = conn.last_response.as(Array)
      cursor = String.new(result[0].as(Bytes))
      items = result[1].as(Array)

      expect(cursor).to eq("0")
      expect(items.size).to eq(0)
    end

    it "filters by MATCH pattern" do
      handler.execute(cmd("HSET", "myhash", "user_name", "alice", "user_age", "30", "post_id", "1"), conn)
      conn.clear

      handler.execute(cmd("HSCAN", "myhash", "0", "MATCH", "user_*"), conn)
      result = conn.last_response.as(Array)
      items = result[1].as(Array)

      # Should only have user_name and user_age pairs (4 elements)
      expect(items.size).to eq(4)
    end

    it "returns an error for an invalid cursor" do
      handler.execute(cmd("HSCAN", "myhash", "nope"), conn)
      expect(conn.last_error).to contain("invalid cursor")
    end
  end

  describe "SSCAN" do
    it "iterates over set members" do
      handler.execute(cmd("SADD", "myset", "a", "b", "c"), conn)
      conn.clear

      handler.execute(cmd("SSCAN", "myset", "0"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))

      arr = result.as(Array)
      expect(arr.size).to eq(2) # cursor + members array
    end

    it "returns members" do
      handler.execute(cmd("SADD", "myset", "member1", "member2"), conn)
      conn.clear

      handler.execute(cmd("SSCAN", "myset", "0"), conn)
      result = conn.last_response.as(Array)
      members = result[1].as(Array)

      expect(members.size).to eq(2)
    end

    it "returns empty for non-existing key" do
      handler.execute(cmd("SSCAN", "nonexistent", "0"), conn)
      result = conn.last_response.as(Array)
      cursor = String.new(result[0].as(Bytes))
      members = result[1].as(Array)

      expect(cursor).to eq("0")
      expect(members.size).to eq(0)
    end

    it "filters by MATCH pattern" do
      handler.execute(cmd("SADD", "myset", "foo1", "foo2", "bar1"), conn)
      conn.clear

      handler.execute(cmd("SSCAN", "myset", "0", "MATCH", "foo*"), conn)
      result = conn.last_response.as(Array)
      members = result[1].as(Array)

      expect(members.size).to eq(2)
    end

    it "returns an error for an invalid cursor" do
      handler.execute(cmd("SSCAN", "myset", "nope"), conn)
      expect(conn.last_error).to contain("invalid cursor")
    end
  end

  describe "ZSCAN" do
    it "iterates over sorted set members with scores" do
      handler.execute(cmd("ZADD", "myzset", "1", "a", "2", "b", "3", "c"), conn)
      conn.clear

      handler.execute(cmd("ZSCAN", "myzset", "0"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))

      arr = result.as(Array)
      expect(arr.size).to eq(2) # cursor + items array
    end

    it "returns member-score pairs" do
      handler.execute(cmd("ZADD", "myzset", "1.5", "m1", "2.5", "m2"), conn)
      conn.clear

      handler.execute(cmd("ZSCAN", "myzset", "0"), conn)
      result = conn.last_response.as(Array)
      items = result[1].as(Array)

      # Should have pairs: [member1, score1, member2, score2]
      expect(items.size).to eq(4)
    end

    it "returns empty for non-existing key" do
      handler.execute(cmd("ZSCAN", "nonexistent", "0"), conn)
      result = conn.last_response.as(Array)
      cursor = String.new(result[0].as(Bytes))
      items = result[1].as(Array)

      expect(cursor).to eq("0")
      expect(items.size).to eq(0)
    end

    it "filters by MATCH pattern" do
      handler.execute(cmd("ZADD", "myzset", "1", "user:1", "2", "user:2", "3", "post:1"), conn)
      conn.clear

      handler.execute(cmd("ZSCAN", "myzset", "0", "MATCH", "user:*"), conn)
      result = conn.last_response.as(Array)
      items = result[1].as(Array)

      # Should only have user:1 and user:2 pairs (4 elements)
      expect(items.size).to eq(4)
    end

    it "returns an error for an invalid cursor" do
      handler.execute(cmd("ZSCAN", "myzset", "nope"), conn)
      expect(conn.last_error).to contain("invalid cursor")
    end
  end
end
