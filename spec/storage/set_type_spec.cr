require "../spec_helper"

Spectator.describe Redis::SetType do
  let(set) { Redis::SetType.new }

  describe "#sadd" do
    it "adds members to set" do
      result = set.sadd([b("a"), b("b")])
      expect(result).to eq(2_i64)
    end

    it "returns count of new members only" do
      set.sadd([b("a")])
      result = set.sadd([b("a"), b("b")])
      expect(result).to eq(1_i64)
    end

    it "does not add duplicates" do
      set.sadd([b("a"), b("a"), b("a")])
      expect(set.scard).to eq(1_i64)
    end
  end

  describe "#srem" do
    before_each { set.sadd([b("a"), b("b"), b("c")]) }

    it "removes members from set" do
      result = set.srem([b("a")])
      expect(result).to eq(1_i64)
      expect(set.sismember?(b("a"))).to be_false
    end

    it "removes multiple members" do
      result = set.srem([b("a"), b("b")])
      expect(result).to eq(2_i64)
    end

    it "returns 0 for non-existing members" do
      result = set.srem([b("nonexistent")])
      expect(result).to eq(0_i64)
    end

    it "counts only removed members" do
      result = set.srem([b("a"), b("nonexistent")])
      expect(result).to eq(1_i64)
    end
  end

  describe "#sismember?" do
    before_each { set.sadd([b("a"), b("b")]) }

    it "returns true for existing member" do
      expect(set.sismember?(b("a"))).to be_true
    end

    it "returns false for non-existing member" do
      expect(set.sismember?(b("c"))).to be_false
    end
  end

  describe "#smismember" do
    before_each { set.sadd([b("a"), b("b")]) }

    it "checks multiple members" do
      result = set.smismember([b("a"), b("c"), b("b")])
      expect(result).to eq([true, false, true])
    end
  end

  describe "#smembers" do
    it "returns empty array for empty set" do
      expect(set.smembers).to eq([] of Bytes)
    end

    it "returns all members" do
      set.sadd([b("a"), b("b"), b("c")])
      members = set.smembers
      expect(members.size).to eq(3)
      expect(members).to contain(b("a"))
      expect(members).to contain(b("b"))
      expect(members).to contain(b("c"))
    end
  end

  describe "#scard" do
    it "returns 0 for empty set" do
      expect(set.scard).to eq(0_i64)
    end

    it "returns correct count" do
      set.sadd([b("a"), b("b"), b("c")])
      expect(set.scard).to eq(3_i64)
    end
  end

  describe "#spop" do
    before_each { set.sadd([b("a"), b("b"), b("c")]) }

    it "pops single member" do
      result = set.spop
      expect(result.size).to eq(1)
      expect(set.scard).to eq(2_i64)
    end

    it "pops multiple members" do
      result = set.spop(2)
      expect(result.size).to eq(2)
      expect(set.scard).to eq(1_i64)
    end

    it "returns empty array from empty set" do
      empty_set = Redis::SetType.new
      result = empty_set.spop
      expect(result).to eq([] of Bytes)
    end
  end

  describe "#srandmember" do
    before_each { set.sadd([b("a"), b("b"), b("c")]) }

    it "returns random member without removal" do
      result = set.srandmember(1)
      expect(result.size).to eq(1)
      expect(set.scard).to eq(3_i64) # Set unchanged
    end

    it "returns multiple members" do
      result = set.srandmember(2)
      expect(result.size).to eq(2)
    end

    it "can return duplicates with negative count" do
      result = set.srandmember(-5)
      expect(result.size).to eq(5)
    end

    it "limits to set size with positive count" do
      result = set.srandmember(10)
      expect(result.size).to be <= 3
    end
  end

  describe "#sunion" do
    let(other) { Redis::SetType.new }

    before_each do
      set.sadd([b("a"), b("b")])
      other.sadd([b("b"), b("c")])
    end

    it "returns union of two sets" do
      result = set.sunion(other)
      expect(result.scard).to eq(3_i64)
      expect(result.sismember?(b("a"))).to be_true
      expect(result.sismember?(b("b"))).to be_true
      expect(result.sismember?(b("c"))).to be_true
    end
  end

  describe "#sinter" do
    let(other) { Redis::SetType.new }

    before_each do
      set.sadd([b("a"), b("b"), b("c")])
      other.sadd([b("b"), b("c"), b("d")])
    end

    it "returns intersection of two sets" do
      result = set.sinter(other)
      expect(result.scard).to eq(2_i64)
      expect(result.sismember?(b("b"))).to be_true
      expect(result.sismember?(b("c"))).to be_true
    end

    it "returns empty set when no common elements" do
      other2 = Redis::SetType.new
      other2.sadd([b("x"), b("y")])
      result = set.sinter(other2)
      expect(result.scard).to eq(0_i64)
    end
  end

  describe "#sdiff" do
    let(other) { Redis::SetType.new }

    before_each do
      set.sadd([b("a"), b("b"), b("c")])
      other.sadd([b("b"), b("c"), b("d")])
    end

    it "returns difference of two sets" do
      result = set.sdiff(other)
      expect(result.scard).to eq(1_i64)
      expect(result.sismember?(b("a"))).to be_true
    end

    it "returns all elements when other is empty" do
      empty = Redis::SetType.new
      result = set.sdiff(empty)
      expect(result.scard).to eq(3_i64)
    end
  end

  describe "#smove" do
    let(dest) { Redis::SetType.new }

    before_each { set.sadd([b("a"), b("b")]) }

    it "moves member to destination" do
      result = set.smove(dest, b("a"))
      expect(result).to be_true
      expect(set.sismember?(b("a"))).to be_false
      expect(dest.sismember?(b("a"))).to be_true
    end

    it "returns false for non-existing member" do
      result = set.smove(dest, b("nonexistent"))
      expect(result).to be_false
    end
  end

  describe "#empty?" do
    it "returns true for empty set" do
      expect(set.empty?).to be_true
    end

    it "returns false for non-empty set" do
      set.sadd([b("a")])
      expect(set.empty?).to be_false
    end
  end
end
