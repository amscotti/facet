require "../spec_helper"

Spectator.describe "Sorted Set Commands" do
  let(db_manager) { Redis::DatabaseManager.new }
  let(db) { db_manager.get_or_create(0) }
  let(handler) { Redis::CommandHandler.new(db_manager) }
  let(conn) { TestConnection.new }

  def cmd(*args) : Array(Redis::RespValue)
    result = [] of Redis::RespValue
    args.each { |arg| result << b(arg) }
    result
  end

  describe "ZADD" do
    it "adds member with score" do
      handler.execute(cmd("ZADD", "myzset", "1", "a"), conn)
      expect(conn.last_response).to eq(1_i64)
    end

    it "adds multiple members" do
      handler.execute(cmd("ZADD", "myzset", "1", "a", "2", "b", "3", "c"), conn)
      expect(conn.last_response).to eq(3_i64)
    end

    it "updates score of existing member" do
      handler.execute(cmd("ZADD", "myzset", "1", "a"), conn)
      conn.clear
      handler.execute(cmd("ZADD", "myzset", "5", "a"), conn)
      expect(conn.last_response).to eq(0_i64) # No new members

      handler.execute(cmd("ZSCORE", "myzset", "a"), conn)
      expect(conn.last_response).to eq(b("5.0"))
    end

    it "supports NX flag" do
      handler.execute(cmd("ZADD", "myzset", "1", "a"), conn)
      conn.clear
      handler.execute(cmd("ZADD", "myzset", "NX", "5", "a"), conn)
      expect(conn.last_response).to eq(0_i64)
    end

    it "supports XX flag" do
      handler.execute(cmd("ZADD", "myzset", "XX", "1", "a"), conn)
      expect(conn.last_response).to eq(0_i64) # Doesn't exist
    end

    it "supports CH flag" do
      handler.execute(cmd("ZADD", "myzset", "1", "a"), conn)
      conn.clear
      handler.execute(cmd("ZADD", "myzset", "CH", "5", "a"), conn)
      expect(conn.last_response).to eq(1_i64) # Changed
    end

    it "supports INCR option" do
      handler.execute(cmd("ZADD", "myzset", "INCR", "5", "a"), conn)
      expect(conn.last_response).to eq(b("5.0"))

      conn.clear
      handler.execute(cmd("ZADD", "myzset", "INCR", "3", "a"), conn)
      expect(conn.last_response).to eq(b("8.0"))
    end

    it "INCR with NX returns nil if member exists" do
      handler.execute(cmd("ZADD", "myzset", "1", "a"), conn)
      conn.clear
      handler.execute(cmd("ZADD", "myzset", "NX", "INCR", "5", "a"), conn)
      expect(conn.last_response).to be_nil
      # Score should be unchanged
      handler.execute(cmd("ZSCORE", "myzset", "a"), conn)
      expect(conn.last_response).to eq(b("1.0"))
    end

    it "INCR with NX increments if member does not exist" do
      handler.execute(cmd("ZADD", "myzset", "NX", "INCR", "5", "newmember"), conn)
      expect(conn.last_response).to eq(b("5.0"))
    end

    it "INCR with XX returns nil if member does not exist" do
      handler.execute(cmd("ZADD", "myzset", "XX", "INCR", "5", "nonexistent"), conn)
      expect(conn.last_response).to be_nil
    end

    it "INCR with XX increments if member exists" do
      handler.execute(cmd("ZADD", "myzset", "1", "a"), conn)
      conn.clear
      handler.execute(cmd("ZADD", "myzset", "XX", "INCR", "5", "a"), conn)
      expect(conn.last_response).to eq(b("6.0"))
    end
  end

  describe "ZREM" do
    before_each do
      handler.execute(cmd("ZADD", "myzset", "1", "a", "2", "b", "3", "c"), conn)
      conn.clear
    end

    it "removes members" do
      handler.execute(cmd("ZREM", "myzset", "a", "b"), conn)
      expect(conn.last_response).to eq(2_i64)
    end

    it "returns 0 for non-existing members" do
      handler.execute(cmd("ZREM", "myzset", "x"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "ZSCORE" do
    before_each do
      handler.execute(cmd("ZADD", "myzset", "1.5", "a"), conn)
      conn.clear
    end

    it "returns score of member" do
      handler.execute(cmd("ZSCORE", "myzset", "a"), conn)
      expect(conn.last_response).to eq(b("1.5"))
    end

    it "returns nil for non-existing member" do
      handler.execute(cmd("ZSCORE", "myzset", "x"), conn)
      expect(conn.last_response).to be_nil
    end
  end

  describe "ZRANK" do
    before_each do
      handler.execute(cmd("ZADD", "myzset", "1", "a", "2", "b", "3", "c"), conn)
      conn.clear
    end

    it "returns rank (ascending)" do
      handler.execute(cmd("ZRANK", "myzset", "a"), conn)
      expect(conn.last_response).to eq(0_i64)
      handler.execute(cmd("ZRANK", "myzset", "c"), conn)
      expect(conn.last_response).to eq(2_i64)
    end

    it "returns nil for non-existing member" do
      handler.execute(cmd("ZRANK", "myzset", "x"), conn)
      expect(conn.last_response).to be_nil
    end

    it "handles members with same score correctly" do
      # Binary search should still find correct members when multiple share same score
      handler.execute(cmd("ZADD", "samescores", "1", "a", "1", "b", "1", "c", "2", "d", "2", "e"), conn)
      conn.clear

      handler.execute(cmd("ZRANK", "samescores", "a"), conn)
      expect(conn.last_response).to eq(0_i64)

      handler.execute(cmd("ZRANK", "samescores", "b"), conn)
      expect(conn.last_response).to eq(1_i64)

      handler.execute(cmd("ZRANK", "samescores", "c"), conn)
      expect(conn.last_response).to eq(2_i64)

      handler.execute(cmd("ZRANK", "samescores", "d"), conn)
      expect(conn.last_response).to eq(3_i64)

      handler.execute(cmd("ZRANK", "samescores", "e"), conn)
      expect(conn.last_response).to eq(4_i64)
    end

    it "handles large sorted set efficiently" do
      # Add many members with sequential scores
      100.times do |i|
        handler.execute(cmd("ZADD", "largeset", i.to_s, "member#{i}"), conn)
      end
      conn.clear

      # Check ranks at various positions
      handler.execute(cmd("ZRANK", "largeset", "member0"), conn)
      expect(conn.last_response).to eq(0_i64)

      handler.execute(cmd("ZRANK", "largeset", "member50"), conn)
      expect(conn.last_response).to eq(50_i64)

      handler.execute(cmd("ZRANK", "largeset", "member99"), conn)
      expect(conn.last_response).to eq(99_i64)
    end
  end

  describe "ZREVRANK" do
    before_each do
      handler.execute(cmd("ZADD", "myzset", "1", "a", "2", "b", "3", "c"), conn)
      conn.clear
    end

    it "returns rank (descending)" do
      handler.execute(cmd("ZREVRANK", "myzset", "a"), conn)
      expect(conn.last_response).to eq(2_i64)
      handler.execute(cmd("ZREVRANK", "myzset", "c"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "ZCARD" do
    it "returns cardinality" do
      handler.execute(cmd("ZADD", "myzset", "1", "a", "2", "b"), conn)
      conn.clear
      handler.execute(cmd("ZCARD", "myzset"), conn)
      expect(conn.last_response).to eq(2_i64)
    end

    it "returns 0 for non-existing set" do
      handler.execute(cmd("ZCARD", "nonexistent"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "ZCOUNT" do
    before_each do
      handler.execute(cmd("ZADD", "myzset", "1", "a", "2", "b", "3", "c", "4", "d"), conn)
      conn.clear
    end

    it "counts members in score range" do
      handler.execute(cmd("ZCOUNT", "myzset", "2", "3"), conn)
      expect(conn.last_response).to eq(2_i64)
    end

    it "supports -inf and +inf" do
      handler.execute(cmd("ZCOUNT", "myzset", "-inf", "+inf"), conn)
      expect(conn.last_response).to eq(4_i64)
    end

    it "supports exclusive min bound with parenthesis" do
      # (2 means > 2, so should count 3 and 4 (members c, d)
      handler.execute(cmd("ZCOUNT", "myzset", "(2", "4"), conn)
      expect(conn.last_response).to eq(2_i64)
    end

    it "supports exclusive max bound with parenthesis" do
      # 2 to (4 means >= 2 and < 4, so should count 2 and 3 (members b, c)
      handler.execute(cmd("ZCOUNT", "myzset", "2", "(4"), conn)
      expect(conn.last_response).to eq(2_i64)
    end

    it "supports both exclusive bounds" do
      # (1 to (4 means > 1 and < 4, so should count 2 and 3 (members b, c)
      handler.execute(cmd("ZCOUNT", "myzset", "(1", "(4"), conn)
      expect(conn.last_response).to eq(2_i64)
    end

    it "handles exclusive bounds with exact score matches" do
      # When a member has exactly the boundary score, exclusive should exclude it
      handler.execute(cmd("ZADD", "exactscores", "1.5", "a", "2.0", "b", "2.5", "c"), conn)
      conn.clear

      # (2 should exclude member with score exactly 2.0
      handler.execute(cmd("ZCOUNT", "exactscores", "(2", "+inf"), conn)
      expect(conn.last_response).to eq(1_i64) # only c

      # 2) should exclude member with score exactly 2.0
      handler.execute(cmd("ZCOUNT", "exactscores", "-inf", "(2"), conn)
      expect(conn.last_response).to eq(1_i64) # only a
    end
  end

  describe "ZRANGE" do
    before_each do
      handler.execute(cmd("ZADD", "myzset", "1", "a", "2", "b", "3", "c"), conn)
      conn.clear
    end

    it "returns members in range" do
      handler.execute(cmd("ZRANGE", "myzset", "0", "1"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      arr = result.as(Array)
      expect(arr.size).to eq(2)
    end

    it "returns all with 0 to -1" do
      handler.execute(cmd("ZRANGE", "myzset", "0", "-1"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(3)
    end

    it "supports WITHSCORES" do
      handler.execute(cmd("ZRANGE", "myzset", "0", "0", "WITHSCORES"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(2) # member + score
    end
  end

  describe "ZREVRANGE" do
    before_each do
      handler.execute(cmd("ZADD", "myzset", "1", "a", "2", "b", "3", "c"), conn)
      conn.clear
    end

    it "returns members in reverse order" do
      handler.execute(cmd("ZREVRANGE", "myzset", "0", "1"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(2)
    end
  end

  describe "ZRANGEBYSCORE" do
    before_each do
      handler.execute(cmd("ZADD", "myzset", "1", "a", "2", "b", "3", "c", "4", "d"), conn)
      conn.clear
    end

    it "returns members in score range" do
      handler.execute(cmd("ZRANGEBYSCORE", "myzset", "2", "3"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to eq(2)
    end

    it "supports WITHSCORES" do
      handler.execute(cmd("ZRANGEBYSCORE", "myzset", "1", "2", "WITHSCORES"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(4) # 2 members + 2 scores
    end

    it "supports LIMIT" do
      handler.execute(cmd("ZRANGEBYSCORE", "myzset", "1", "4", "LIMIT", "1", "2"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(2)
    end

    it "supports exclusive min bound with parenthesis" do
      # (2 means > 2, should return c (3) and d (4)
      handler.execute(cmd("ZRANGEBYSCORE", "myzset", "(2", "4"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(2)
      expect(result[0]).to eq(b("c"))
      expect(result[1]).to eq(b("d"))
    end

    it "supports exclusive max bound with parenthesis" do
      # 2 to (4 means >= 2 and < 4, should return b (2) and c (3)
      handler.execute(cmd("ZRANGEBYSCORE", "myzset", "2", "(4"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(2)
      expect(result[0]).to eq(b("b"))
      expect(result[1]).to eq(b("c"))
    end

    it "supports both exclusive bounds" do
      # (1 to (4 means > 1 and < 4, should return b (2) and c (3)
      handler.execute(cmd("ZRANGEBYSCORE", "myzset", "(1", "(4"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(2)
      expect(result[0]).to eq(b("b"))
      expect(result[1]).to eq(b("c"))
    end

    it "handles exclusive bounds with exact score matches" do
      handler.execute(cmd("ZADD", "exactscores", "1.5", "x", "2.0", "y", "2.5", "z"), conn)
      conn.clear

      # (2 should exclude member with score exactly 2.0
      handler.execute(cmd("ZRANGEBYSCORE", "exactscores", "(2", "+inf"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(1)
      expect(result[0]).to eq(b("z"))

      # (2 should exclude member with score exactly 2.0
      handler.execute(cmd("ZRANGEBYSCORE", "exactscores", "-inf", "(2"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(1)
      expect(result[0]).to eq(b("x"))
    end

    it "handles exclusive bounds with WITHSCORES" do
      handler.execute(cmd("ZRANGEBYSCORE", "myzset", "(1", "(4", "WITHSCORES"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(4) # b, 2.0, c, 3.0
      expect(result[0]).to eq(b("b"))
      expect(result[2]).to eq(b("c"))
    end
  end

  describe "ZINCRBY" do
    it "increments score" do
      handler.execute(cmd("ZADD", "myzset", "5", "a"), conn)
      conn.clear
      handler.execute(cmd("ZINCRBY", "myzset", "2", "a"), conn)
      expect(conn.last_response).to eq(b("7.0"))
    end

    it "creates member if not exists" do
      handler.execute(cmd("ZINCRBY", "myzset", "3", "a"), conn)
      expect(conn.last_response).to eq(b("3.0"))
    end
  end

  describe "ZPOPMIN" do
    before_each do
      handler.execute(cmd("ZADD", "myzset", "1", "a", "2", "b", "3", "c"), conn)
      conn.clear
    end

    it "pops member with lowest score" do
      handler.execute(cmd("ZPOPMIN", "myzset"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      arr = result.as(Array)
      expect(arr.size).to eq(2) # member + score
    end

    it "pops multiple with count" do
      handler.execute(cmd("ZPOPMIN", "myzset", "2"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(4) # 2 members + 2 scores
    end
  end

  describe "ZPOPMAX" do
    before_each do
      handler.execute(cmd("ZADD", "myzset", "1", "a", "2", "b", "3", "c"), conn)
      conn.clear
    end

    it "pops member with highest score" do
      handler.execute(cmd("ZPOPMAX", "myzset"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
    end
  end

  describe "ZMSCORE" do
    before_each do
      handler.execute(cmd("ZADD", "myzset", "1", "a", "2", "b"), conn)
      conn.clear
    end

    it "returns scores of multiple members" do
      handler.execute(cmd("ZMSCORE", "myzset", "a", "b", "x"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      arr = result.as(Array)
      expect(arr[0]).to eq(b("1.0"))
      expect(arr[1]).to eq(b("2.0"))
      expect(arr[2]).to be_nil
    end
  end
end
