require "../spec_helper"

Spectator.describe "List Commands" do
  let(db_manager) { Redis::DatabaseManager.new }
  let(db) { db_manager.get_or_create(0) }
  let(handler) { Redis::CommandHandler.new(db_manager) }
  let(conn) { TestConnection.new }

  def cmd(*args) : Array(Redis::RespValue)
    result = [] of Redis::RespValue
    args.each { |arg| result << b(arg) }
    result
  end

  describe "LPUSH" do
    it "pushes values to left" do
      handler.execute(cmd("LPUSH", "mylist", "a", "b", "c"), conn)
      expect(conn.last_response).to eq(3_i64)
    end

    it "creates list if not exists" do
      handler.execute(cmd("LPUSH", "newlist", "a"), conn)
      expect(conn.last_response).to eq(1_i64)
    end
  end

  describe "RPUSH" do
    it "pushes values to right" do
      handler.execute(cmd("RPUSH", "mylist", "a", "b", "c"), conn)
      expect(conn.last_response).to eq(3_i64)
    end
  end

  describe "LPUSHX" do
    it "pushes only if list exists" do
      handler.execute(cmd("LPUSHX", "nonexistent", "a"), conn)
      expect(conn.last_response).to eq(0_i64)

      handler.execute(cmd("LPUSH", "mylist", "a"), conn)
      conn.clear
      handler.execute(cmd("LPUSHX", "mylist", "b"), conn)
      expect(conn.last_response).to eq(2_i64)
    end
  end

  describe "RPUSHX" do
    it "pushes only if list exists" do
      handler.execute(cmd("RPUSHX", "nonexistent", "a"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "LPOP" do
    before_each do
      handler.execute(cmd("RPUSH", "mylist", "a", "b", "c"), conn)
      conn.clear
    end

    it "pops from left" do
      handler.execute(cmd("LPOP", "mylist"), conn)
      expect(conn.last_response).to eq(b("a"))
    end

    it "pops multiple with count" do
      handler.execute(cmd("LPOP", "mylist", "2"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      arr = result.as(Array)
      expect(arr.size).to eq(2)
    end

    it "returns nil for empty/non-existent list" do
      handler.execute(cmd("LPOP", "nonexistent"), conn)
      expect(conn.last_response).to be_nil
    end

    it "returns an error for a non-positive count" do
      handler.execute(cmd("LPOP", "mylist", "-1"), conn)
      expect(conn.last_error).to contain("must be positive")
    end
  end

  describe "RPOP" do
    before_each do
      handler.execute(cmd("RPUSH", "mylist", "a", "b", "c"), conn)
      conn.clear
    end

    it "pops from right" do
      handler.execute(cmd("RPOP", "mylist"), conn)
      expect(conn.last_response).to eq(b("c"))
    end
  end

  describe "LLEN" do
    it "returns list length" do
      handler.execute(cmd("RPUSH", "mylist", "a", "b", "c"), conn)
      conn.clear
      handler.execute(cmd("LLEN", "mylist"), conn)
      expect(conn.last_response).to eq(3_i64)
    end

    it "returns 0 for non-existent list" do
      handler.execute(cmd("LLEN", "nonexistent"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "LINDEX" do
    before_each do
      handler.execute(cmd("RPUSH", "mylist", "a", "b", "c"), conn)
      conn.clear
    end

    it "returns element at index" do
      handler.execute(cmd("LINDEX", "mylist", "1"), conn)
      expect(conn.last_response).to eq(b("b"))
    end

    it "supports negative index" do
      handler.execute(cmd("LINDEX", "mylist", "-1"), conn)
      expect(conn.last_response).to eq(b("c"))
    end

    it "returns nil for out of range" do
      handler.execute(cmd("LINDEX", "mylist", "100"), conn)
      expect(conn.last_response).to be_nil
    end
  end

  describe "LSET" do
    before_each do
      handler.execute(cmd("RPUSH", "mylist", "a", "b", "c"), conn)
      conn.clear
    end

    it "sets element at index" do
      handler.execute(cmd("LSET", "mylist", "1", "x"), conn)
      expect(conn.last_response).to eq("OK")
    end

    it "returns error for out of range" do
      handler.execute(cmd("LSET", "mylist", "100", "x"), conn)
      expect(conn.errors.size).to eq(1)
    end
  end

  describe "LRANGE" do
    before_each do
      handler.execute(cmd("RPUSH", "mylist", "a", "b", "c", "d", "e"), conn)
      conn.clear
    end

    it "returns range of elements" do
      handler.execute(cmd("LRANGE", "mylist", "1", "3"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      arr = result.as(Array)
      expect(arr.size).to eq(3)
    end

    it "returns all with 0 to -1" do
      handler.execute(cmd("LRANGE", "mylist", "0", "-1"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(5)
    end
  end

  describe "LINSERT" do
    before_each do
      handler.execute(cmd("RPUSH", "mylist", "a", "c"), conn)
      conn.clear
    end

    it "inserts before pivot" do
      handler.execute(cmd("LINSERT", "mylist", "BEFORE", "c", "b"), conn)
      expect(conn.last_response).to eq(3_i64)
    end

    it "inserts after pivot" do
      handler.execute(cmd("LINSERT", "mylist", "AFTER", "a", "b"), conn)
      expect(conn.last_response).to eq(3_i64)
    end

    it "returns -1 if pivot not found" do
      handler.execute(cmd("LINSERT", "mylist", "BEFORE", "x", "y"), conn)
      expect(conn.last_response).to eq(-1_i64)
    end
  end

  describe "LREM" do
    before_each do
      handler.execute(cmd("RPUSH", "mylist", "a", "b", "a", "c", "a"), conn)
      conn.clear
    end

    it "removes occurrences" do
      handler.execute(cmd("LREM", "mylist", "2", "a"), conn)
      expect(conn.last_response).to eq(2_i64)
    end

    it "removes all with count 0" do
      handler.execute(cmd("LREM", "mylist", "0", "a"), conn)
      expect(conn.last_response).to eq(3_i64)
    end
  end

  describe "LTRIM" do
    before_each do
      handler.execute(cmd("RPUSH", "mylist", "a", "b", "c", "d", "e"), conn)
      conn.clear
    end

    it "trims list to range" do
      handler.execute(cmd("LTRIM", "mylist", "1", "3"), conn)
      expect(conn.last_response).to eq("OK")

      handler.execute(cmd("LLEN", "mylist"), conn)
      expect(conn.last_response).to eq(3_i64)
    end
  end

  describe "LMOVE" do
    before_each do
      handler.execute(cmd("RPUSH", "src", "a", "b", "c"), conn)
      conn.clear
    end

    it "moves element between lists" do
      handler.execute(cmd("LMOVE", "src", "dst", "LEFT", "RIGHT"), conn)
      expect(conn.last_response).to eq(b("a"))

      handler.execute(cmd("LLEN", "src"), conn)
      expect(conn.last_response).to eq(2_i64)
    end

    it "returns nil for empty source" do
      handler.execute(cmd("LMOVE", "empty", "dst", "LEFT", "RIGHT"), conn)
      expect(conn.last_response).to be_nil
    end
  end

  describe "LPOS" do
    before_each do
      handler.execute(cmd("RPUSH", "mylist", "a", "b", "a", "c", "a"), conn)
      conn.clear
    end

    it "returns all matches when COUNT 0 is provided" do
      handler.execute(cmd("LPOS", "mylist", "a", "COUNT", "0"), conn)
      expect(conn.last_response).to eq([0_i64, 2_i64, 4_i64] of Redis::RespValue)
    end

    it "returns an error for COUNT below zero" do
      handler.execute(cmd("LPOS", "mylist", "a", "COUNT", "-1"), conn)
      expect(conn.last_error).to contain("COUNT can't be negative")
    end

    it "returns an error for RANK 0" do
      handler.execute(cmd("LPOS", "mylist", "a", "RANK", "0"), conn)
      expect(conn.last_error).to contain("RANK can't be zero")
    end

    it "returns an error for unknown options" do
      handler.execute(cmd("LPOS", "mylist", "a", "BOGUS", "1"), conn)
      expect(conn.last_error).to contain("syntax error")
    end
  end
end
