require "../spec_helper"

Spectator.describe Redis::ListType do
  let(list) { Redis::ListType.new }

  describe "#lpush" do
    it "pushes values to left" do
      result = list.lpush([b("a"), b("b")])
      expect(result).to eq(2_i64)
    end

    it "returns new length" do
      list.lpush([b("a")])
      result = list.lpush([b("b")])
      expect(result).to eq(2_i64)
    end

    it "pushes in order (first element ends up leftmost)" do
      list.lpush([b("a"), b("b"), b("c")])
      expect(list.lindex(0)).to eq(b("a"))
      expect(list.lindex(1)).to eq(b("b"))
      expect(list.lindex(2)).to eq(b("c"))
    end
  end

  describe "#rpush" do
    it "pushes values to right" do
      result = list.rpush([b("a"), b("b")])
      expect(result).to eq(2_i64)
    end

    it "pushes in order (first element ends up leftmost)" do
      list.rpush([b("a"), b("b"), b("c")])
      expect(list.lindex(0)).to eq(b("a"))
      expect(list.lindex(1)).to eq(b("b"))
      expect(list.lindex(2)).to eq(b("c"))
    end
  end

  describe "#lpop" do
    before_each { list.rpush([b("a"), b("b"), b("c")]) }

    it "pops single element from left" do
      result = list.lpop
      expect(result).to eq([b("a")])
      expect(list.llen).to eq(2_i64)
    end

    it "pops multiple elements from left" do
      result = list.lpop(2)
      expect(result).to eq([b("a"), b("b")])
      expect(list.llen).to eq(1_i64)
    end

    it "returns empty array when list is empty" do
      list.lpop(3)
      result = list.lpop
      expect(result).to eq([] of Bytes)
    end
  end

  describe "#rpop" do
    before_each { list.rpush([b("a"), b("b"), b("c")]) }

    it "pops single element from right" do
      result = list.rpop
      expect(result).to eq([b("c")])
    end

    it "pops multiple elements from right" do
      result = list.rpop(2)
      expect(result).to eq([b("c"), b("b")])
    end
  end

  describe "#llen" do
    it "returns 0 for empty list" do
      expect(list.llen).to eq(0_i64)
    end

    it "returns correct length" do
      list.rpush([b("a"), b("b"), b("c")])
      expect(list.llen).to eq(3_i64)
    end
  end

  describe "#lindex" do
    before_each { list.rpush([b("a"), b("b"), b("c")]) }

    it "returns element at index" do
      expect(list.lindex(0)).to eq(b("a"))
      expect(list.lindex(1)).to eq(b("b"))
      expect(list.lindex(2)).to eq(b("c"))
    end

    it "supports negative indices" do
      expect(list.lindex(-1)).to eq(b("c"))
      expect(list.lindex(-2)).to eq(b("b"))
      expect(list.lindex(-3)).to eq(b("a"))
    end

    it "returns nil for out of range index" do
      expect(list.lindex(10)).to be_nil
    end
  end

  describe "#lset" do
    before_each { list.rpush([b("a"), b("b"), b("c")]) }

    it "sets element at index" do
      result = list.lset(1, b("x"))
      expect(result).to be_true
      expect(list.lindex(1)).to eq(b("x"))
    end

    it "supports negative indices" do
      result = list.lset(-1, b("x"))
      expect(result).to be_true
      expect(list.lindex(2)).to eq(b("x"))
    end

    it "returns false for out of range index" do
      result = list.lset(10, b("x"))
      expect(result).to be_false
    end
  end

  describe "#lrange" do
    before_each { list.rpush([b("a"), b("b"), b("c"), b("d"), b("e")]) }

    it "returns range of elements" do
      result = list.lrange(1, 3)
      expect(result).to eq([b("b"), b("c"), b("d")])
    end

    it "supports negative indices" do
      result = list.lrange(-3, -1)
      expect(result).to eq([b("c"), b("d"), b("e")])
    end

    it "returns all elements with 0 to -1" do
      result = list.lrange(0, -1)
      expect(result).to eq([b("a"), b("b"), b("c"), b("d"), b("e")])
    end

    it "returns empty array if start > end" do
      result = list.lrange(3, 1)
      expect(result).to eq([] of Bytes)
    end

    it "clamps end to list length" do
      result = list.lrange(3, 100)
      expect(result).to eq([b("d"), b("e")])
    end
  end

  describe "#linsert" do
    before_each { list.rpush([b("a"), b("c")]) }

    it "inserts before pivot" do
      result = list.linsert(b("c"), b("b"), before: true)
      expect(result).to eq(3_i64)
      expect(list.lrange(0, -1)).to eq([b("a"), b("b"), b("c")])
    end

    it "inserts after pivot" do
      result = list.linsert(b("a"), b("b"), before: false)
      expect(result).to eq(3_i64)
      expect(list.lrange(0, -1)).to eq([b("a"), b("b"), b("c")])
    end

    it "returns -1 if pivot not found" do
      result = list.linsert(b("x"), b("y"), before: true)
      expect(result).to eq(-1_i64)
    end
  end

  describe "#lpos" do
    before_each { list.rpush([b("a"), b("b"), b("a"), b("c"), b("a")]) }

    it "finds first position of element" do
      result = list.lpos(b("a"))
      expect(result).to eq([0_i64])
    end

    it "finds multiple positions with count" do
      result = list.lpos(b("a"), count: 3)
      expect(result).to eq([0_i64, 2_i64, 4_i64])
    end

    it "respects rank parameter" do
      result = list.lpos(b("a"), rank: 2)
      expect(result).to eq([2_i64])
    end

    it "returns empty array if not found" do
      result = list.lpos(b("x"))
      expect(result).to eq([] of Int64)
    end
  end

  describe "#lrem" do
    before_each { list.rpush([b("a"), b("b"), b("a"), b("c"), b("a")]) }

    it "removes all occurrences with count 0" do
      result = list.lrem(0, b("a"))
      expect(result).to eq(3_i64)
      expect(list.lrange(0, -1)).to eq([b("b"), b("c")])
    end

    it "removes from head with positive count" do
      result = list.lrem(2, b("a"))
      expect(result).to eq(2_i64)
      expect(list.lrange(0, -1)).to eq([b("b"), b("c"), b("a")])
    end

    it "removes from tail with negative count" do
      result = list.lrem(-2, b("a"))
      expect(result).to eq(2_i64)
      expect(list.lrange(0, -1)).to eq([b("a"), b("b"), b("c")])
    end
  end

  describe "#ltrim" do
    before_each { list.rpush([b("a"), b("b"), b("c"), b("d"), b("e")]) }

    it "trims list to specified range" do
      list.ltrim(1, 3)
      expect(list.lrange(0, -1)).to eq([b("b"), b("c"), b("d")])
    end

    it "supports negative indices" do
      list.ltrim(-3, -1)
      expect(list.lrange(0, -1)).to eq([b("c"), b("d"), b("e")])
    end

    it "clears list if start > end" do
      list.ltrim(3, 1)
      expect(list.empty?).to be_true
    end
  end

  describe "#lmove" do
    let(dest) { Redis::ListType.new }

    before_each do
      list.rpush([b("a"), b("b"), b("c")])
    end

    it "moves from left to left" do
      result = list.lmove(dest, :left, :left)
      expect(result).to eq(b("a"))
      expect(list.lrange(0, -1)).to eq([b("b"), b("c")])
      expect(dest.lrange(0, -1)).to eq([b("a")])
    end

    it "moves from right to right" do
      result = list.lmove(dest, :right, :right)
      expect(result).to eq(b("c"))
      expect(list.lrange(0, -1)).to eq([b("a"), b("b")])
      expect(dest.lrange(0, -1)).to eq([b("c")])
    end

    it "moves from left to right" do
      result = list.lmove(dest, :left, :right)
      expect(result).to eq(b("a"))
      expect(dest.lrange(0, -1)).to eq([b("a")])
    end

    it "returns nil when source is empty" do
      empty_list = Redis::ListType.new
      result = empty_list.lmove(dest, :left, :left)
      expect(result).to be_nil
    end
  end

  describe "#empty?" do
    it "returns true for empty list" do
      expect(list.empty?).to be_true
    end

    it "returns false for non-empty list" do
      list.rpush([b("a")])
      expect(list.empty?).to be_false
    end
  end

  describe "#size" do
    it "returns 0 for empty list" do
      expect(list.size).to eq(0)
    end

    it "returns correct size" do
      list.rpush([b("a"), b("b"), b("c")])
      expect(list.size).to eq(3)
    end
  end
end
