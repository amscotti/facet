require "../spec_helper"

Spectator.describe "Hash Commands" do
  let(db_manager) { Redis::DatabaseManager.new }
  let(db) { db_manager.get_or_create(0) }
  let(handler) { Redis::CommandHandler.new(db_manager) }
  let(conn) { TestConnection.new }

  def cmd(*args) : Array(Redis::RespValue)
    result = [] of Redis::RespValue
    args.each { |arg| result << b(arg) }
    result
  end

  describe "HSET" do
    it "sets field in hash" do
      handler.execute(cmd("HSET", "myhash", "field1", "value1"), conn)
      expect(conn.last_response).to eq(1_i64)
    end

    it "sets multiple fields" do
      handler.execute(cmd("HSET", "myhash", "f1", "v1", "f2", "v2"), conn)
      expect(conn.last_response).to eq(2_i64)
    end

    it "returns 0 for updated field" do
      handler.execute(cmd("HSET", "myhash", "field1", "value1"), conn)
      conn.clear
      handler.execute(cmd("HSET", "myhash", "field1", "value2"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "HGET" do
    it "returns field value" do
      handler.execute(cmd("HSET", "myhash", "field1", "value1"), conn)
      conn.clear
      handler.execute(cmd("HGET", "myhash", "field1"), conn)
      expect(conn.last_response).to eq(b("value1"))
    end

    it "returns nil for non-existent field" do
      handler.execute(cmd("HGET", "myhash", "nonexistent"), conn)
      expect(conn.last_response).to be_nil
    end
  end

  describe "HMSET" do
    it "sets multiple fields" do
      handler.execute(cmd("HMSET", "myhash", "f1", "v1", "f2", "v2"), conn)
      expect(conn.last_response).to eq("OK")
    end
  end

  describe "HMGET" do
    before_each do
      handler.execute(cmd("HSET", "myhash", "f1", "v1", "f2", "v2"), conn)
      conn.clear
    end

    it "returns multiple field values" do
      handler.execute(cmd("HMGET", "myhash", "f1", "f2", "f3"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      arr = result.as(Array)
      expect(arr[0]).to eq(b("v1"))
      expect(arr[1]).to eq(b("v2"))
      expect(arr[2]).to be_nil
    end
  end

  describe "HDEL" do
    before_each do
      handler.execute(cmd("HSET", "myhash", "f1", "v1", "f2", "v2"), conn)
      conn.clear
    end

    it "deletes fields" do
      handler.execute(cmd("HDEL", "myhash", "f1"), conn)
      expect(conn.last_response).to eq(1_i64)
    end

    it "returns 0 for non-existent fields" do
      handler.execute(cmd("HDEL", "myhash", "nonexistent"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "HEXISTS" do
    before_each do
      handler.execute(cmd("HSET", "myhash", "f1", "v1"), conn)
      conn.clear
    end

    it "returns 1 for existing field" do
      handler.execute(cmd("HEXISTS", "myhash", "f1"), conn)
      expect(conn.last_response).to eq(1_i64)
    end

    it "returns 0 for non-existent field" do
      handler.execute(cmd("HEXISTS", "myhash", "nonexistent"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "HLEN" do
    it "returns number of fields" do
      handler.execute(cmd("HSET", "myhash", "f1", "v1", "f2", "v2"), conn)
      conn.clear
      handler.execute(cmd("HLEN", "myhash"), conn)
      expect(conn.last_response).to eq(2_i64)
    end

    it "returns 0 for non-existent hash" do
      handler.execute(cmd("HLEN", "nonexistent"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "HKEYS" do
    it "returns all field names" do
      handler.execute(cmd("HSET", "myhash", "f1", "v1", "f2", "v2"), conn)
      conn.clear
      handler.execute(cmd("HKEYS", "myhash"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to eq(2)
    end
  end

  describe "HVALS" do
    it "returns all values" do
      handler.execute(cmd("HSET", "myhash", "f1", "v1", "f2", "v2"), conn)
      conn.clear
      handler.execute(cmd("HVALS", "myhash"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to eq(2)
    end
  end

  describe "HGETALL" do
    it "returns all fields and values" do
      handler.execute(cmd("HSET", "myhash", "f1", "v1", "f2", "v2"), conn)
      conn.clear
      handler.execute(cmd("HGETALL", "myhash"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to eq(4) # 2 fields + 2 values
    end
  end

  describe "HINCRBY" do
    it "increments integer field" do
      handler.execute(cmd("HSET", "myhash", "counter", "10"), conn)
      conn.clear
      handler.execute(cmd("HINCRBY", "myhash", "counter", "5"), conn)
      expect(conn.last_response).to eq(15_i64)
    end

    it "creates field if not exists" do
      handler.execute(cmd("HINCRBY", "myhash", "counter", "5"), conn)
      expect(conn.last_response).to eq(5_i64)
    end
  end

  describe "HINCRBYFLOAT" do
    it "increments float field" do
      handler.execute(cmd("HSET", "myhash", "counter", "10.5"), conn)
      conn.clear
      handler.execute(cmd("HINCRBYFLOAT", "myhash", "counter", "0.5"), conn)
      result = conn.last_response
      expect(result).to be_a(Bytes)
      expect(String.new(result.as(Bytes))).to eq("11.0")
    end
  end

  describe "HSETNX" do
    it "sets field if not exists" do
      handler.execute(cmd("HSETNX", "myhash", "f1", "v1"), conn)
      expect(conn.last_response).to eq(1_i64)
    end

    it "does not set if field exists" do
      handler.execute(cmd("HSET", "myhash", "f1", "v1"), conn)
      conn.clear
      handler.execute(cmd("HSETNX", "myhash", "f1", "v2"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "HSTRLEN" do
    it "returns field value length" do
      handler.execute(cmd("HSET", "myhash", "f1", "hello"), conn)
      conn.clear
      handler.execute(cmd("HSTRLEN", "myhash", "f1"), conn)
      expect(conn.last_response).to eq(5_i64)
    end

    it "returns 0 for non-existent field" do
      handler.execute(cmd("HSTRLEN", "myhash", "nonexistent"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end
end
