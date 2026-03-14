require "../spec_helper"

Spectator.describe Redis::SortedSetType do
  let(zset) { Redis::SortedSetType.new }

  describe "#zadd" do
    it "adds member with score" do
      added, _ = zset.zadd(b("a"), 1.0)
      expect(added).to eq(1_i64)
      expect(zset.zscore(b("a"))).to eq(1.0)
    end

    it "updates score of existing member" do
      zset.zadd(b("a"), 1.0)
      added, changed = zset.zadd(b("a"), 2.0)
      expect(added).to eq(0_i64)
      expect(changed).to be_true
      expect(zset.zscore(b("a"))).to eq(2.0)
    end

    context "with NX flag" do
      it "only adds new members" do
        zset.zadd(b("a"), 1.0)
        added, _ = zset.zadd(b("a"), 2.0, nx: true)
        expect(added).to eq(0_i64)
        expect(zset.zscore(b("a"))).to eq(1.0) # Unchanged
      end

      it "adds truly new members" do
        added, _ = zset.zadd(b("a"), 1.0, nx: true)
        expect(added).to eq(1_i64)
      end
    end

    context "with XX flag" do
      it "only updates existing members" do
        added, _ = zset.zadd(b("a"), 1.0, xx: true)
        expect(added).to eq(0_i64)
        expect(zset.zscore(b("a"))).to be_nil
      end

      it "updates existing members" do
        zset.zadd(b("a"), 1.0)
        added, changed = zset.zadd(b("a"), 2.0, xx: true)
        expect(added).to eq(0_i64)
        expect(changed).to be_true
        expect(zset.zscore(b("a"))).to eq(2.0)
      end
    end

    context "with GT flag" do
      before_each { zset.zadd(b("a"), 5.0) }

      it "updates only if new score > current" do
        _, changed = zset.zadd(b("a"), 10.0, gt: true)
        expect(changed).to be_true
        expect(zset.zscore(b("a"))).to eq(10.0)
      end

      it "does not update if new score <= current" do
        _, changed = zset.zadd(b("a"), 3.0, gt: true)
        expect(changed).to be_false
        expect(zset.zscore(b("a"))).to eq(5.0)
      end
    end

    context "with LT flag" do
      before_each { zset.zadd(b("a"), 5.0) }

      it "updates only if new score < current" do
        _, changed = zset.zadd(b("a"), 3.0, lt: true)
        expect(changed).to be_true
        expect(zset.zscore(b("a"))).to eq(3.0)
      end

      it "does not update if new score >= current" do
        _, changed = zset.zadd(b("a"), 10.0, lt: true)
        expect(changed).to be_false
        expect(zset.zscore(b("a"))).to eq(5.0)
      end
    end

    context "with CH flag" do
      it "returns changed count instead of added count" do
        zset.zadd(b("a"), 1.0)
        count, _ = zset.zadd(b("a"), 2.0, ch: true)
        expect(count).to eq(1_i64) # Changed, not added
      end
    end
  end

  describe "#zrem" do
    before_each do
      zset.zadd(b("a"), 1.0)
      zset.zadd(b("b"), 2.0)
      zset.zadd(b("c"), 3.0)
    end

    it "removes single member" do
      result = zset.zrem([b("a")])
      expect(result).to eq(1_i64)
      expect(zset.zscore(b("a"))).to be_nil
    end

    it "removes multiple members" do
      result = zset.zrem([b("a"), b("b")])
      expect(result).to eq(2_i64)
    end

    it "returns 0 for non-existing members" do
      result = zset.zrem([b("nonexistent")])
      expect(result).to eq(0_i64)
    end
  end

  describe "#zscore" do
    it "returns score of member" do
      zset.zadd(b("a"), 1.5)
      expect(zset.zscore(b("a"))).to eq(1.5)
    end

    it "returns nil for non-existing member" do
      expect(zset.zscore(b("nonexistent"))).to be_nil
    end
  end

  describe "#zrank" do
    before_each do
      zset.zadd(b("a"), 1.0)
      zset.zadd(b("b"), 2.0)
      zset.zadd(b("c"), 3.0)
    end

    it "returns 0-based rank (ascending)" do
      expect(zset.zrank(b("a"))).to eq(0_i64)
      expect(zset.zrank(b("b"))).to eq(1_i64)
      expect(zset.zrank(b("c"))).to eq(2_i64)
    end

    it "returns nil for non-existing member" do
      expect(zset.zrank(b("nonexistent"))).to be_nil
    end
  end

  describe "#zrevrank" do
    before_each do
      zset.zadd(b("a"), 1.0)
      zset.zadd(b("b"), 2.0)
      zset.zadd(b("c"), 3.0)
    end

    it "returns 0-based rank (descending)" do
      expect(zset.zrevrank(b("a"))).to eq(2_i64)
      expect(zset.zrevrank(b("b"))).to eq(1_i64)
      expect(zset.zrevrank(b("c"))).to eq(0_i64)
    end
  end

  describe "#zcard" do
    it "returns 0 for empty set" do
      expect(zset.zcard).to eq(0_i64)
    end

    it "returns correct count" do
      zset.zadd(b("a"), 1.0)
      zset.zadd(b("b"), 2.0)
      expect(zset.zcard).to eq(2_i64)
    end
  end

  describe "#zcount" do
    before_each do
      zset.zadd(b("a"), 1.0)
      zset.zadd(b("b"), 2.0)
      zset.zadd(b("c"), 3.0)
      zset.zadd(b("d"), 4.0)
    end

    it "counts members in score range" do
      expect(zset.zcount(2.0, 3.0)).to eq(2_i64)
    end

    it "includes boundaries" do
      expect(zset.zcount(1.0, 4.0)).to eq(4_i64)
    end

    it "returns 0 for out of range" do
      expect(zset.zcount(10.0, 20.0)).to eq(0_i64)
    end
  end

  describe "#zrange" do
    before_each do
      zset.zadd(b("a"), 1.0)
      zset.zadd(b("b"), 2.0)
      zset.zadd(b("c"), 3.0)
    end

    it "returns members in range" do
      result = zset.zrange(0, 1)
      expect(result).to eq([b("a"), b("b")])
    end

    it "supports negative indices" do
      result = zset.zrange(-2, -1)
      expect(result).to eq([b("b"), b("c")])
    end

    it "returns all with 0 to -1" do
      result = zset.zrange(0, -1)
      expect(result).to eq([b("a"), b("b"), b("c")])
    end

    it "returns with scores when requested" do
      result = zset.zrange(0, 0, withscores: true)
      expect(result).to eq([b("a"), 1.0])
    end

    it "supports reverse" do
      result = zset.zrange(0, 1, reverse: true)
      # Returns elements from the end of the sorted set
      expect(result).to eq([b("c"), b("b")])
    end
  end

  describe "#zrangebyscore" do
    before_each do
      zset.zadd(b("a"), 1.0)
      zset.zadd(b("b"), 2.0)
      zset.zadd(b("c"), 3.0)
      zset.zadd(b("d"), 4.0)
    end

    it "returns members in score range" do
      result = zset.zrangebyscore(2.0, 3.0)
      expect(result).to eq([b("b"), b("c")])
    end

    it "supports withscores" do
      result = zset.zrangebyscore(1.0, 2.0, withscores: true)
      expect(result).to eq([b("a"), 1.0, b("b"), 2.0])
    end

    it "supports offset and count" do
      result = zset.zrangebyscore(1.0, 4.0, offset: 1, count: 2)
      expect(result).to eq([b("b"), b("c")])
    end
  end

  describe "#zincrby" do
    it "increments score" do
      zset.zadd(b("a"), 5.0)
      result = zset.zincrby(b("a"), 2.5)
      expect(result).to eq(7.5)
    end

    it "creates member if not exists" do
      result = zset.zincrby(b("a"), 3.0)
      expect(result).to eq(3.0)
    end

    it "handles negative increment" do
      zset.zadd(b("a"), 5.0)
      result = zset.zincrby(b("a"), -2.0)
      expect(result).to eq(3.0)
    end
  end

  describe "#zpopmin" do
    before_each do
      zset.zadd(b("a"), 1.0)
      zset.zadd(b("b"), 2.0)
      zset.zadd(b("c"), 3.0)
    end

    it "pops member with lowest score" do
      result = zset.zpopmin
      expect(result.size).to eq(1)
      expect(result[0]).to eq({b("a"), 1.0})
      expect(zset.zcard).to eq(2_i64)
    end

    it "pops multiple members" do
      result = zset.zpopmin(2)
      expect(result.size).to eq(2)
      expect(result[0]).to eq({b("a"), 1.0})
      expect(result[1]).to eq({b("b"), 2.0})
    end
  end

  describe "#zpopmax" do
    before_each do
      zset.zadd(b("a"), 1.0)
      zset.zadd(b("b"), 2.0)
      zset.zadd(b("c"), 3.0)
    end

    it "pops member with highest score" do
      result = zset.zpopmax
      expect(result.size).to eq(1)
      expect(result[0]).to eq({b("c"), 3.0})
    end

    it "pops multiple members" do
      result = zset.zpopmax(2)
      expect(result.size).to eq(2)
      expect(result[0]).to eq({b("c"), 3.0})
      expect(result[1]).to eq({b("b"), 2.0})
    end
  end

  describe "#zmscore" do
    before_each do
      zset.zadd(b("a"), 1.0)
      zset.zadd(b("b"), 2.0)
    end

    it "returns scores of multiple members" do
      result = zset.zmscore([b("a"), b("b")])
      expect(result).to eq([1.0, 2.0])
    end

    it "returns nil for non-existing members" do
      result = zset.zmscore([b("a"), b("nonexistent")])
      expect(result).to eq([1.0, nil])
    end
  end

  describe "#zrandmember" do
    before_each do
      zset.zadd(b("a"), 1.0)
      zset.zadd(b("b"), 2.0)
      zset.zadd(b("c"), 3.0)
    end

    it "returns random member" do
      result = zset.zrandmember(1)
      expect(result.size).to eq(1)
      expect(zset.zcard).to eq(3_i64) # Set unchanged
    end

    it "returns with scores when requested" do
      result = zset.zrandmember(1, withscores: true)
      expect(result.size).to eq(2) # member + score
    end
  end

  describe "#empty?" do
    it "returns true for empty set" do
      expect(zset.empty?).to be_true
    end

    it "returns false for non-empty set" do
      zset.zadd(b("a"), 1.0)
      expect(zset.empty?).to be_false
    end
  end
end
