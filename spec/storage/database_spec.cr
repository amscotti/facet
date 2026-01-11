require "../spec_helper"

Spectator.describe Redis::Database do
  let(db) { Redis::Database.new }

  describe "string operations" do
    describe "#get and #set" do
      it "sets and gets a value" do
        db.set(b("key"), b("value"))
        expect(db.get(b("key"))).to eq(b("value"))
      end

      it "returns nil for non-existing key" do
        expect(db.get(b("nonexistent"))).to be_nil
      end

      it "overwrites existing value" do
        db.set(b("key"), b("value1"))
        db.set(b("key"), b("value2"))
        expect(db.get(b("key"))).to eq(b("value2"))
      end
    end

    describe "#set with TTL" do
      it "sets value with expiry" do
        future = Time.utc.to_unix_ms + 10_000
        db.set(b("key"), b("value"), future)
        expect(db.get(b("key"))).to eq(b("value"))
      end

      it "returns nil for expired key" do
        past = Time.utc.to_unix_ms - 1000
        db.set(b("key"), b("value"), past)
        expect(db.get(b("key"))).to be_nil
      end
    end

    describe "#append" do
      it "appends to existing value" do
        db.set(b("key"), b("Hello"))
        result = db.append(b("key"), b(" World"))
        expect(result).to eq(11_i64)
        expect(db.get(b("key"))).to eq(b("Hello World"))
      end

      it "creates key if not exists" do
        result = db.append(b("key"), b("value"))
        expect(result).to eq(5_i64)
        expect(db.get(b("key"))).to eq(b("value"))
      end
    end

    describe "#strlen" do
      it "returns length of string" do
        db.set(b("key"), b("hello"))
        expect(db.strlen(b("key"))).to eq(5_i64)
      end

      it "returns 0 for non-existing key" do
        expect(db.strlen(b("nonexistent"))).to eq(0_i64)
      end
    end

    describe "#getrange" do
      before_each { db.set(b("key"), b("Hello World")) }

      it "returns substring" do
        result = db.getrange(b("key"), 0, 4)
        expect(result).to eq(b("Hello"))
      end

      it "supports negative indices" do
        result = db.getrange(b("key"), -5, -1)
        expect(result).to eq(b("World"))
      end

      it "returns empty for non-existing key" do
        result = db.getrange(b("nonexistent"), 0, 10)
        expect(result).to eq(Bytes.empty)
      end
    end

    describe "#setrange" do
      it "overwrites at offset" do
        db.set(b("key"), b("Hello World"))
        result = db.setrange(b("key"), 6, b("Redis"))
        expect(result).to eq(11_i64)
        expect(db.get(b("key"))).to eq(b("Hello Redis"))
      end

      it "pads with null bytes if offset > length" do
        db.set(b("key"), b("Hello"))
        db.setrange(b("key"), 10, b("World"))
        result = db.get(b("key"))
        expect(result).not_to be_nil
        expect(result.as(Bytes).size).to eq(15)
      end

      it "creates key if not exists" do
        db.setrange(b("key"), 0, b("value"))
        expect(db.get(b("key"))).to eq(b("value"))
      end
    end
  end

  describe "numeric operations" do
    describe "#incr" do
      it "increments value by 1" do
        db.set(b("counter"), b("10"))
        result = db.incr(b("counter"))
        expect(result).to eq(11_i64)
      end

      it "creates key with value 1 if not exists" do
        result = db.incr(b("counter"))
        expect(result).to eq(1_i64)
      end
    end

    describe "#incrby" do
      it "increments by specified amount" do
        db.set(b("counter"), b("10"))
        result = db.incrby(b("counter"), 5_i64)
        expect(result).to eq(15_i64)
      end

      it "handles negative increment" do
        db.set(b("counter"), b("10"))
        result = db.incrby(b("counter"), -3_i64)
        expect(result).to eq(7_i64)
      end
    end

    describe "#incrbyfloat" do
      it "increments by float" do
        db.set(b("counter"), b("10.5"))
        result = db.incrbyfloat(b("counter"), 0.5)
        expect(result).to eq(11.0)
      end

      it "creates key if not exists" do
        result = db.incrbyfloat(b("counter"), 2.5)
        expect(result).to eq(2.5)
      end
    end

    describe "#decr" do
      it "decrements value by 1" do
        db.set(b("counter"), b("10"))
        result = db.decr(b("counter"))
        expect(result).to eq(9_i64)
      end

      it "creates key with value -1 if not exists" do
        result = db.decr(b("counter"))
        expect(result).to eq(-1_i64)
      end
    end

    describe "#decrby" do
      it "decrements by specified amount" do
        db.set(b("counter"), b("10"))
        result = db.decrby(b("counter"), 3_i64)
        expect(result).to eq(7_i64)
      end
    end
  end

  describe "key operations" do
    describe "#del" do
      it "deletes existing key" do
        db.set(b("key"), b("value"))
        result = db.del(b("key"))
        expect(result).to be_true
        expect(db.get(b("key"))).to be_nil
      end

      it "returns false for non-existing key" do
        result = db.del(b("nonexistent"))
        expect(result).to be_false
      end
    end

    describe "#exists?" do
      it "returns true for existing key" do
        db.set(b("key"), b("value"))
        expect(db.exists?(b("key"))).to be_true
      end

      it "returns false for non-existing key" do
        expect(db.exists?(b("nonexistent"))).to be_false
      end

      it "returns false for expired key" do
        past = Time.utc.to_unix_ms - 1000
        db.set(b("key"), b("value"), past)
        expect(db.exists?(b("key"))).to be_false
      end
    end

    describe "#type_of" do
      it "returns 'none' for non-existing key" do
        expect(db.type_of(b("nonexistent"))).to eq("none")
      end

      it "returns 'string' for string value" do
        db.set(b("key"), b("value"))
        expect(db.type_of(b("key"))).to eq("string")
      end

      it "returns 'list' for list value" do
        db.get_or_create_list(b("key")).lpush([b("a")])
        expect(db.type_of(b("key"))).to eq("list")
      end

      it "returns 'hash' for hash value" do
        db.get_or_create_hash(b("key")).hset(b("f"), b("v"))
        expect(db.type_of(b("key"))).to eq("hash")
      end

      it "returns 'set' for set value" do
        db.get_or_create_set(b("key")).sadd([b("a")])
        expect(db.type_of(b("key"))).to eq("set")
      end

      it "returns 'zset' for sorted set value" do
        db.get_or_create_sorted_set(b("key")).zadd(b("a"), 1.0)
        expect(db.type_of(b("key"))).to eq("zset")
      end
    end

    describe "#keys" do
      it "returns empty array for empty db" do
        expect(db.keys).to eq([] of Bytes)
      end

      it "returns all keys" do
        db.set(b("key1"), b("value1"))
        db.set(b("key2"), b("value2"))
        keys = db.keys
        expect(keys.size).to eq(2)
        expect(keys).to contain(b("key1"))
        expect(keys).to contain(b("key2"))
      end
    end

    describe "#size" do
      it "returns 0 for empty db" do
        expect(db.size).to eq(0)
      end

      it "returns correct count" do
        db.set(b("key1"), b("value1"))
        db.set(b("key2"), b("value2"))
        expect(db.size).to eq(2)
      end
    end

    describe "#clear" do
      it "removes all keys" do
        db.set(b("key1"), b("value1"))
        db.set(b("key2"), b("value2"))
        db.clear
        expect(db.size).to eq(0)
      end
    end
  end

  describe "type-specific getters" do
    describe "#get_or_create_list" do
      it "creates list if not exists" do
        list = db.get_or_create_list(b("mylist"))
        expect(list).to be_a(Redis::ListType)
      end

      it "returns existing list" do
        db.get_or_create_list(b("mylist")).lpush([b("a")])
        list = db.get_or_create_list(b("mylist"))
        expect(list.llen).to eq(1_i64)
      end

      it "raises WrongTypeError if key is not a list" do
        db.set(b("mykey"), b("value"))
        expect { db.get_or_create_list(b("mykey")) }.to raise_error(Redis::WrongTypeError)
      end
    end

    describe "#get_list" do
      it "returns nil if not exists" do
        expect(db.get_list(b("mylist"))).to be_nil
      end

      it "returns list if exists" do
        db.get_or_create_list(b("mylist")).lpush([b("a")])
        list = db.get_list(b("mylist"))
        expect(list).not_to be_nil
        expect(list.as(Redis::ListType).llen).to eq(1_i64)
      end
    end

    describe "#get_or_create_hash" do
      it "creates hash if not exists" do
        hash = db.get_or_create_hash(b("myhash"))
        expect(hash).to be_a(Redis::HashType)
      end

      it "raises WrongTypeError if key is not a hash" do
        db.set(b("mykey"), b("value"))
        expect { db.get_or_create_hash(b("mykey")) }.to raise_error(Redis::WrongTypeError)
      end
    end

    describe "#get_or_create_set" do
      it "creates set if not exists" do
        set = db.get_or_create_set(b("myset"))
        expect(set).to be_a(Redis::SetType)
      end

      it "raises WrongTypeError if key is not a set" do
        db.set(b("mykey"), b("value"))
        expect { db.get_or_create_set(b("mykey")) }.to raise_error(Redis::WrongTypeError)
      end
    end

    describe "#get_or_create_sorted_set" do
      it "creates sorted set if not exists" do
        zset = db.get_or_create_sorted_set(b("myzset"))
        expect(zset).to be_a(Redis::SortedSetType)
      end

      it "raises WrongTypeError if key is not a sorted set" do
        db.set(b("mykey"), b("value"))
        expect { db.get_or_create_sorted_set(b("mykey")) }.to raise_error(Redis::WrongTypeError)
      end
    end
  end

  describe "#cleanup_empty" do
    it "removes empty list" do
      list = db.get_or_create_list(b("mylist"))
      list.lpush([b("a")])
      list.lpop
      db.cleanup_empty(b("mylist"))
      expect(db.exists?(b("mylist"))).to be_false
    end

    it "removes empty hash" do
      hash = db.get_or_create_hash(b("myhash"))
      hash.hset(b("f"), b("v"))
      hash.hdel([b("f")])
      db.cleanup_empty(b("myhash"))
      expect(db.exists?(b("myhash"))).to be_false
    end

    it "removes empty set" do
      set = db.get_or_create_set(b("myset"))
      set.sadd([b("a")])
      set.srem([b("a")])
      db.cleanup_empty(b("myset"))
      expect(db.exists?(b("myset"))).to be_false
    end

    it "removes empty sorted set" do
      zset = db.get_or_create_sorted_set(b("myzset"))
      zset.zadd(b("a"), 1.0)
      zset.zrem([b("a")])
      db.cleanup_empty(b("myzset"))
      expect(db.exists?(b("myzset"))).to be_false
    end

    it "does not remove non-empty collection" do
      list = db.get_or_create_list(b("mylist"))
      list.lpush([b("a")])
      db.cleanup_empty(b("mylist"))
      expect(db.exists?(b("mylist"))).to be_true
    end

    it "does not remove string values" do
      db.set(b("mykey"), b("value"))
      db.cleanup_empty(b("mykey"))
      expect(db.exists?(b("mykey"))).to be_true
    end
  end
end
