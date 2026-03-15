require "../spec_helper"

Spectator.describe "Set Commands" do
  let(db_manager) { Redis::DatabaseManager.new }
  let(db) { db_manager.get_or_create(0) }
  let(handler) { Redis::CommandHandler.new(db_manager) }
  let(conn) { TestConnection.new }

  def cmd(*args) : Array(Redis::RespValue)
    result = [] of Redis::RespValue
    args.each { |arg| result << b(arg) }
    result
  end

  describe "SADD" do
    it "adds members to set" do
      handler.execute(cmd("SADD", "myset", "a", "b", "c"), conn)
      expect(conn.last_response).to eq(3_i64)
    end

    it "returns count of new members only" do
      handler.execute(cmd("SADD", "myset", "a"), conn)
      conn.clear
      handler.execute(cmd("SADD", "myset", "a", "b"), conn)
      expect(conn.last_response).to eq(1_i64)
    end
  end

  describe "SREM" do
    before_each do
      handler.execute(cmd("SADD", "myset", "a", "b", "c"), conn)
      conn.clear
    end

    it "removes members from set" do
      handler.execute(cmd("SREM", "myset", "a", "b"), conn)
      expect(conn.last_response).to eq(2_i64)
    end

    it "returns 0 for non-existent members" do
      handler.execute(cmd("SREM", "myset", "x"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "SISMEMBER" do
    before_each do
      handler.execute(cmd("SADD", "myset", "a", "b"), conn)
      conn.clear
    end

    it "returns 1 for existing member" do
      handler.execute(cmd("SISMEMBER", "myset", "a"), conn)
      expect(conn.last_response).to eq(1_i64)
    end

    it "returns 0 for non-existing member" do
      handler.execute(cmd("SISMEMBER", "myset", "x"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "SMISMEMBER" do
    before_each do
      handler.execute(cmd("SADD", "myset", "a", "b"), conn)
      conn.clear
    end

    it "checks multiple members" do
      handler.execute(cmd("SMISMEMBER", "myset", "a", "x", "b"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      arr = result.as(Array)
      expect(arr[0]).to eq(1_i64)
      expect(arr[1]).to eq(0_i64)
      expect(arr[2]).to eq(1_i64)
    end
  end

  describe "SMEMBERS" do
    it "returns all members" do
      handler.execute(cmd("SADD", "myset", "a", "b", "c"), conn)
      conn.clear
      handler.execute(cmd("SMEMBERS", "myset"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to eq(3)
    end

    it "returns empty array for non-existent set" do
      handler.execute(cmd("SMEMBERS", "nonexistent"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to eq(0)
    end
  end

  describe "SCARD" do
    it "returns set cardinality" do
      handler.execute(cmd("SADD", "myset", "a", "b", "c"), conn)
      conn.clear
      handler.execute(cmd("SCARD", "myset"), conn)
      expect(conn.last_response).to eq(3_i64)
    end

    it "returns 0 for non-existent set" do
      handler.execute(cmd("SCARD", "nonexistent"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "SPOP" do
    before_each do
      handler.execute(cmd("SADD", "myset", "a", "b", "c"), conn)
      conn.clear
    end

    it "pops random member" do
      handler.execute(cmd("SPOP", "myset"), conn)
      result = conn.last_response
      expect(result).to be_a(Bytes)
    end

    it "pops multiple members with count" do
      handler.execute(cmd("SPOP", "myset", "2"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to eq(2)
    end

    it "returns an error for a non-positive count" do
      handler.execute(cmd("SPOP", "myset", "-1"), conn)
      expect(conn.last_error).to contain("must be positive")
    end
  end

  describe "SRANDMEMBER" do
    before_each do
      handler.execute(cmd("SADD", "myset", "a", "b", "c"), conn)
      conn.clear
    end

    it "returns random member without removal" do
      handler.execute(cmd("SRANDMEMBER", "myset"), conn)
      result = conn.last_response
      expect(result).to be_a(Bytes)

      handler.execute(cmd("SCARD", "myset"), conn)
      expect(conn.last_response).to eq(3_i64) # Unchanged
    end

    it "returns multiple members with count" do
      handler.execute(cmd("SRANDMEMBER", "myset", "2"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
    end

    it "returns an error for an invalid count" do
      handler.execute(cmd("SRANDMEMBER", "myset", "nope"), conn)
      expect(conn.last_error).to contain("integer")
    end
  end

  describe "SUNION" do
    before_each do
      handler.execute(cmd("SADD", "set1", "a", "b"), conn)
      handler.execute(cmd("SADD", "set2", "b", "c"), conn)
      conn.clear
    end

    it "returns union of sets" do
      handler.execute(cmd("SUNION", "set1", "set2"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to eq(3)
    end
  end

  describe "SINTER" do
    before_each do
      handler.execute(cmd("SADD", "set1", "a", "b", "c"), conn)
      handler.execute(cmd("SADD", "set2", "b", "c", "d"), conn)
      conn.clear
    end

    it "returns intersection of sets" do
      handler.execute(cmd("SINTER", "set1", "set2"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to eq(2)
    end
  end

  describe "SDIFF" do
    before_each do
      handler.execute(cmd("SADD", "set1", "a", "b", "c"), conn)
      handler.execute(cmd("SADD", "set2", "b", "c"), conn)
      conn.clear
    end

    it "returns difference of sets" do
      handler.execute(cmd("SDIFF", "set1", "set2"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to eq(1)
    end
  end

  describe "SUNIONSTORE" do
    before_each do
      handler.execute(cmd("SADD", "set1", "a", "b"), conn)
      handler.execute(cmd("SADD", "set2", "b", "c"), conn)
      conn.clear
    end

    it "stores union and returns count" do
      handler.execute(cmd("SUNIONSTORE", "dest", "set1", "set2"), conn)
      expect(conn.last_response).to eq(3_i64)

      handler.execute(cmd("SCARD", "dest"), conn)
      expect(conn.last_response).to eq(3_i64)
    end

    it "replaces an existing destination set" do
      handler.execute(cmd("SADD", "dest", "stale"), conn)
      conn.clear

      handler.execute(cmd("SUNIONSTORE", "dest", "set1", "set2"), conn)
      expect(conn.last_response).to eq(3_i64)

      handler.execute(cmd("SMEMBERS", "dest"), conn)
      result = conn.last_response.as(Array)
      expect(result).not_to contain(b("stale"))
    end
  end

  describe "SINTERSTORE" do
    before_each do
      handler.execute(cmd("SADD", "set1", "a", "b", "c"), conn)
      handler.execute(cmd("SADD", "set2", "b", "c", "d"), conn)
      conn.clear
    end

    it "stores intersection and returns count" do
      handler.execute(cmd("SINTERSTORE", "dest", "set1", "set2"), conn)
      expect(conn.last_response).to eq(2_i64)
    end
  end

  describe "SDIFFSTORE" do
    before_each do
      handler.execute(cmd("SADD", "set1", "a", "b", "c"), conn)
      handler.execute(cmd("SADD", "set2", "b", "c"), conn)
      conn.clear
    end

    it "stores difference and returns count" do
      handler.execute(cmd("SDIFFSTORE", "dest", "set1", "set2"), conn)
      expect(conn.last_response).to eq(1_i64)
    end
  end

  describe "SMOVE" do
    before_each do
      handler.execute(cmd("SADD", "src", "a", "b"), conn)
      conn.clear
    end

    it "moves member to destination" do
      handler.execute(cmd("SMOVE", "src", "dst", "a"), conn)
      expect(conn.last_response).to eq(1_i64)

      handler.execute(cmd("SISMEMBER", "src", "a"), conn)
      expect(conn.last_response).to eq(0_i64)

      handler.execute(cmd("SISMEMBER", "dst", "a"), conn)
      expect(conn.last_response).to eq(1_i64)
    end

    it "returns 0 for non-existing member" do
      handler.execute(cmd("SMOVE", "src", "dst", "x"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end
end
