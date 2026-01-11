require "../spec_helper"

Spectator.describe Redis::HashType do
  let(hash) { Redis::HashType.new }

  describe "#hset" do
    it "sets a field value" do
      hash.hset(b("field1"), b("value1"))
      expect(hash.hget(b("field1"))).to eq(b("value1"))
    end

    it "returns true for new field" do
      result = hash.hset(b("field1"), b("value1"))
      expect(result).to be_true
    end

    it "returns false for existing field" do
      hash.hset(b("field1"), b("value1"))
      result = hash.hset(b("field1"), b("value2"))
      expect(result).to be_false
    end

    it "overwrites existing value" do
      hash.hset(b("field1"), b("value1"))
      hash.hset(b("field1"), b("value2"))
      expect(hash.hget(b("field1"))).to eq(b("value2"))
    end
  end

  describe "#hget" do
    it "returns value for existing field" do
      hash.hset(b("field1"), b("value1"))
      expect(hash.hget(b("field1"))).to eq(b("value1"))
    end

    it "returns nil for non-existing field" do
      expect(hash.hget(b("nonexistent"))).to be_nil
    end
  end

  describe "#hdel" do
    before_each do
      hash.hset(b("field1"), b("value1"))
      hash.hset(b("field2"), b("value2"))
      hash.hset(b("field3"), b("value3"))
    end

    it "deletes single field" do
      result = hash.hdel([b("field1")])
      expect(result).to eq(1_i64)
      expect(hash.hget(b("field1"))).to be_nil
    end

    it "deletes multiple fields" do
      result = hash.hdel([b("field1"), b("field2")])
      expect(result).to eq(2_i64)
    end

    it "returns 0 for non-existing fields" do
      result = hash.hdel([b("nonexistent")])
      expect(result).to eq(0_i64)
    end

    it "counts only deleted fields" do
      result = hash.hdel([b("field1"), b("nonexistent")])
      expect(result).to eq(1_i64)
    end
  end

  describe "#hexists?" do
    it "returns true for existing field" do
      hash.hset(b("field1"), b("value1"))
      expect(hash.hexists?(b("field1"))).to be_true
    end

    it "returns false for non-existing field" do
      expect(hash.hexists?(b("field1"))).to be_false
    end
  end

  describe "#hlen" do
    it "returns 0 for empty hash" do
      expect(hash.hlen).to eq(0_i64)
    end

    it "returns correct count" do
      hash.hset(b("field1"), b("value1"))
      hash.hset(b("field2"), b("value2"))
      expect(hash.hlen).to eq(2_i64)
    end
  end

  describe "#hkeys" do
    it "returns empty array for empty hash" do
      expect(hash.hkeys).to eq([] of Bytes)
    end

    it "returns all field names" do
      hash.hset(b("field1"), b("value1"))
      hash.hset(b("field2"), b("value2"))
      keys = hash.hkeys
      expect(keys.size).to eq(2)
      expect(keys).to contain(b("field1"))
      expect(keys).to contain(b("field2"))
    end
  end

  describe "#hvals" do
    it "returns empty array for empty hash" do
      expect(hash.hvals).to eq([] of Bytes)
    end

    it "returns all values" do
      hash.hset(b("field1"), b("value1"))
      hash.hset(b("field2"), b("value2"))
      vals = hash.hvals
      expect(vals.size).to eq(2)
      expect(vals).to contain(b("value1"))
      expect(vals).to contain(b("value2"))
    end
  end

  describe "#hgetall" do
    it "returns empty array for empty hash" do
      expect(hash.hgetall).to eq([] of Bytes)
    end

    it "returns alternating field/value pairs" do
      hash.hset(b("field1"), b("value1"))
      result = hash.hgetall
      expect(result.size).to eq(2)
      expect(result[0]).to eq(b("field1"))
      expect(result[1]).to eq(b("value1"))
    end
  end

  describe "#hincrby" do
    it "increments integer value" do
      hash.hset(b("counter"), b("10"))
      result = hash.hincrby(b("counter"), 5_i64)
      expect(result).to eq(15_i64)
    end

    it "creates field if not exists" do
      result = hash.hincrby(b("counter"), 5_i64)
      expect(result).to eq(5_i64)
    end

    it "handles negative increment" do
      hash.hset(b("counter"), b("10"))
      result = hash.hincrby(b("counter"), -3_i64)
      expect(result).to eq(7_i64)
    end
  end

  describe "#hincrbyfloat" do
    it "increments float value" do
      hash.hset(b("counter"), b("10.5"))
      result = hash.hincrbyfloat(b("counter"), 0.5)
      expect(result).to eq(11.0)
    end

    it "creates field if not exists" do
      result = hash.hincrbyfloat(b("counter"), 2.5)
      expect(result).to eq(2.5)
    end

    it "handles negative increment" do
      hash.hset(b("counter"), b("10.0"))
      result = hash.hincrbyfloat(b("counter"), -3.5)
      expect(result).to eq(6.5)
    end
  end

  describe "#hsetnx" do
    it "sets field if not exists" do
      result = hash.hsetnx(b("field1"), b("value1"))
      expect(result).to be_true
      expect(hash.hget(b("field1"))).to eq(b("value1"))
    end

    it "does not overwrite existing field" do
      hash.hset(b("field1"), b("value1"))
      result = hash.hsetnx(b("field1"), b("value2"))
      expect(result).to be_false
      expect(hash.hget(b("field1"))).to eq(b("value1"))
    end
  end

  describe "#hstrlen" do
    it "returns length of field value" do
      hash.hset(b("field1"), b("hello"))
      expect(hash.hstrlen(b("field1"))).to eq(5_i64)
    end

    it "returns 0 for non-existing field" do
      expect(hash.hstrlen(b("nonexistent"))).to eq(0_i64)
    end
  end

  describe "#empty?" do
    it "returns true for empty hash" do
      expect(hash.empty?).to be_true
    end

    it "returns false for non-empty hash" do
      hash.hset(b("field1"), b("value1"))
      expect(hash.empty?).to be_false
    end
  end
end
