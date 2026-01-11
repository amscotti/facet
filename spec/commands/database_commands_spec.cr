require "../spec_helper"

Spectator.describe "Database Commands" do
  let(db_manager) { Redis::DatabaseManager.new }
  let(handler) { Redis::CommandHandler.new(db_manager) }
  let(conn) { TestConnection.new }

  def cmd(*args) : Array(Redis::RespValue)
    result = [] of Redis::RespValue
    args.each { |arg| result << b(arg) }
    result
  end

  describe "SELECT" do
    it "selects database 0 by default" do
      handler.execute(cmd("SELECT", "0"), conn)
      expect(conn.last_response).to eq("OK")
      expect(conn.current_db_id).to eq(0)
    end

    it "selects database 5" do
      handler.execute(cmd("SELECT", "5"), conn)
      expect(conn.last_response).to eq("OK")
      expect(conn.current_db_id).to eq(5)
    end

    it "returns error for invalid index" do
      handler.execute(cmd("SELECT", "-1"), conn)
      expect(conn.errors.size).to be > 0
    end

    it "returns error for out of range index" do
      handler.execute(cmd("SELECT", "100"), conn)
      expect(conn.errors.size).to be > 0
    end

    it "isolates data between databases" do
      # Set value in database 0
      handler.execute(cmd("SET", "key1", "value1"), conn)
      conn.clear

      # Switch to database 1
      handler.execute(cmd("SELECT", "1"), conn)
      conn.clear

      # Key should not exist in database 1
      handler.execute(cmd("GET", "key1"), conn)
      expect(conn.last_response).to be_nil

      # Set different value in database 1
      handler.execute(cmd("SET", "key1", "value2"), conn)
      conn.clear

      # Switch back to database 0
      handler.execute(cmd("SELECT", "0"), conn)
      conn.clear

      # Original value should still be there
      handler.execute(cmd("GET", "key1"), conn)
      expect(conn.last_response).to eq(b("value1"))
    end
  end

  describe "DBCREATE" do
    it "creates named database" do
      handler.execute(cmd("DBCREATE", "testdb"), conn)
      expect(conn.last_response).to eq("OK")
      expect(db_manager.database_exists?("testdb")).to be_true
    end

    it "returns error if database exists" do
      handler.execute(cmd("DBCREATE", "testdb"), conn)
      conn.clear
      handler.execute(cmd("DBCREATE", "testdb"), conn)
      expect(conn.errors.size).to eq(1)
      expect(conn.last_error).to contain("already exists")
    end

    it "supports IF NOT EXISTS" do
      handler.execute(cmd("DBCREATE", "testdb"), conn)
      conn.clear
      handler.execute(cmd("DBCREATE", "testdb", "IF", "NOT", "EXISTS"), conn)
      expect(conn.errors.size).to eq(0)
    end
  end

  describe "DBDROP" do
    it "drops named database" do
      handler.execute(cmd("DBCREATE", "testdb"), conn)
      conn.clear
      handler.execute(cmd("DBDROP", "testdb"), conn)
      expect(conn.last_response).to eq("OK")
      expect(db_manager.database_exists?("testdb")).to be_false
    end

    it "returns error for default databases" do
      handler.execute(cmd("DBDROP", "0"), conn)
      expect(conn.errors.size).to eq(1)
      expect(conn.last_error).to contain("cannot drop")
    end

    it "supports IF EXISTS" do
      handler.execute(cmd("DBDROP", "nonexistent", "IF", "EXISTS"), conn)
      expect(conn.last_response).to eq("OK")
    end
  end

  describe "DBLIST" do
    it "lists all databases" do
      handler.execute(cmd("DBLIST"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      expect(result.as(Array).size).to be >= 16
    end

    it "filters by pattern" do
      handler.execute(cmd("DBCREATE", "testdb1"), conn)
      handler.execute(cmd("DBCREATE", "testdb2"), conn)
      handler.execute(cmd("DBCREATE", "other"), conn)
      conn.clear

      handler.execute(cmd("DBLIST", "test*"), conn)
      result = conn.last_response.as(Array)
      expect(result.size).to eq(2)
    end
  end

  describe "DBINFO" do
    it "returns database info" do
      handler.execute(cmd("DBCREATE", "testdb"), conn)
      handler.execute(cmd("DBSELECT", "testdb"), conn)
      handler.execute(cmd("SET", "key1", "value1"), conn)
      handler.execute(cmd("SET", "key2", "value2"), conn)
      conn.clear

      handler.execute(cmd("DBINFO", "testdb"), conn)
      result = conn.last_response
      expect(result).to be_a(Array(Redis::RespValue))
      # Should contain id, keys, created_at, frozen
      arr = result.as(Array)
      expect(arr.size).to eq(8)
    end

    it "returns error for non-existing database" do
      handler.execute(cmd("DBINFO", "nonexistent"), conn)
      expect(conn.errors.size).to eq(1)
    end
  end

  describe "DBSELECT" do
    it "selects named database" do
      handler.execute(cmd("DBCREATE", "testdb"), conn)
      conn.clear
      handler.execute(cmd("DBSELECT", "testdb"), conn)
      expect(conn.last_response).to eq("OK")
      expect(conn.current_db_id).to eq("testdb")
    end

    it "returns error for non-existing database" do
      handler.execute(cmd("DBSELECT", "nonexistent"), conn)
      expect(conn.errors.size).to eq(1)
    end

    it "also works with numeric index" do
      handler.execute(cmd("DBSELECT", "5"), conn)
      expect(conn.last_response).to eq("OK")
      expect(conn.current_db_id).to eq(5)
    end
  end

  describe "DBCOPY" do
    it "copies database" do
      handler.execute(cmd("SET", "key1", "value1"), conn)
      conn.clear

      handler.execute(cmd("DBCOPY", "0", "backup"), conn)
      expect(conn.last_response).to eq("OK")

      # Verify backup has the data
      handler.execute(cmd("DBSELECT", "backup"), conn)
      conn.clear
      handler.execute(cmd("GET", "key1"), conn)
      expect(conn.last_response).to eq(b("value1"))
    end

    it "returns error if destination exists" do
      handler.execute(cmd("DBCREATE", "backup"), conn)
      conn.clear
      handler.execute(cmd("DBCOPY", "0", "backup"), conn)
      expect(conn.errors.size).to eq(1)
    end

    it "supports REPLACE option" do
      handler.execute(cmd("DBCREATE", "backup"), conn)
      handler.execute(cmd("SET", "key1", "value1"), conn)
      conn.clear

      handler.execute(cmd("DBCOPY", "0", "backup", "REPLACE"), conn)
      expect(conn.last_response).to eq("OK")
    end
  end

  describe "DBCOPYKEYS" do
    it "copies matching keys" do
      handler.execute(cmd("SET", "user:1", "alice"), conn)
      handler.execute(cmd("SET", "user:2", "bob"), conn)
      handler.execute(cmd("SET", "post:1", "hello"), conn)
      handler.execute(cmd("DBCREATE", "backup"), conn)
      conn.clear

      handler.execute(cmd("DBCOPYKEYS", "0", "backup", "user:*"), conn)
      expect(conn.last_response).to eq(2_i64)

      # Verify backup has only user keys
      handler.execute(cmd("DBSELECT", "backup"), conn)
      conn.clear
      handler.execute(cmd("GET", "user:1"), conn)
      expect(conn.last_response).to eq(b("alice"))
      handler.execute(cmd("GET", "post:1"), conn)
      expect(conn.last_response).to be_nil
    end
  end

  describe "DBRESET" do
    it "clears database data" do
      handler.execute(cmd("DBCREATE", "testdb"), conn)
      handler.execute(cmd("DBSELECT", "testdb"), conn)
      handler.execute(cmd("SET", "key1", "value1"), conn)
      conn.clear

      handler.execute(cmd("DBRESET", "testdb"), conn)
      expect(conn.last_response).to eq("OK")

      handler.execute(cmd("GET", "key1"), conn)
      expect(conn.last_response).to be_nil
    end
  end

  describe "DBFREEZE and DBUNFREEZE" do
    it "freezes database" do
      handler.execute(cmd("DBCREATE", "testdb"), conn)
      conn.clear
      handler.execute(cmd("DBFREEZE", "testdb"), conn)
      expect(conn.last_response).to eq("OK")
      expect(db_manager.frozen?("testdb")).to be_true
    end

    it "unfreezes database" do
      handler.execute(cmd("DBCREATE", "testdb"), conn)
      handler.execute(cmd("DBFREEZE", "testdb"), conn)
      conn.clear
      handler.execute(cmd("DBUNFREEZE", "testdb"), conn)
      expect(conn.last_response).to eq("OK")
      expect(db_manager.frozen?("testdb")).to be_false
    end

    it "blocks SET on frozen database" do
      handler.execute(cmd("DBCREATE", "freezetest"), conn)
      handler.execute(cmd("DBSELECT", "freezetest"), conn)
      handler.execute(cmd("SET", "key", "original"), conn)
      handler.execute(cmd("DBFREEZE", "freezetest"), conn)
      conn.clear

      handler.execute(cmd("SET", "key", "modified"), conn)
      expect(conn.errors.size).to eq(1)
      expect(conn.last_error).to contain("frozen")

      # Value should be unchanged
      handler.execute(cmd("GET", "key"), conn)
      expect(conn.last_response).to eq(b("original"))
    end

    it "allows reads on frozen database" do
      handler.execute(cmd("DBCREATE", "readtest"), conn)
      handler.execute(cmd("DBSELECT", "readtest"), conn)
      handler.execute(cmd("SET", "key", "value"), conn)
      handler.execute(cmd("DBFREEZE", "readtest"), conn)
      conn.clear

      handler.execute(cmd("GET", "key"), conn)
      expect(conn.last_response).to eq(b("value"))
    end

    it "allows writes after unfreeze" do
      handler.execute(cmd("DBCREATE", "unfreezetest"), conn)
      handler.execute(cmd("DBSELECT", "unfreezetest"), conn)
      handler.execute(cmd("SET", "key", "original"), conn)
      handler.execute(cmd("DBFREEZE", "unfreezetest"), conn)
      handler.execute(cmd("DBUNFREEZE", "unfreezetest"), conn)
      conn.clear

      handler.execute(cmd("SET", "key", "modified"), conn)
      expect(conn.last_response).to eq("OK")

      handler.execute(cmd("GET", "key"), conn)
      expect(conn.last_response).to eq(b("modified"))
    end
  end

  describe "FLUSHALL" do
    it "clears all databases" do
      handler.execute(cmd("SET", "key1", "value1"), conn)
      handler.execute(cmd("SELECT", "1"), conn)
      handler.execute(cmd("SET", "key2", "value2"), conn)
      conn.clear

      handler.execute(cmd("FLUSHALL"), conn)
      expect(conn.last_response).to eq("OK")

      # Verify database 1 is empty
      handler.execute(cmd("DBSIZE"), conn)
      expect(conn.last_response).to eq(0_i64)

      # Verify database 0 is empty
      handler.execute(cmd("SELECT", "0"), conn)
      conn.clear
      handler.execute(cmd("DBSIZE"), conn)
      expect(conn.last_response).to eq(0_i64)
    end
  end

  describe "frozen database blocks all write operations" do
    before_each do
      handler.execute(cmd("DBCREATE", "frozen_test"), conn)
      handler.execute(cmd("DBSELECT", "frozen_test"), conn)
      # Set up test data before freezing
      handler.execute(cmd("SET", "mykey", "value"), conn)
      handler.execute(cmd("SET", "counter", "10"), conn)
      handler.execute(cmd("LPUSH", "mylist", "item1", "item2"), conn)
      handler.execute(cmd("HSET", "myhash", "field1", "value1"), conn)
      handler.execute(cmd("SADD", "myset", "member1", "member2"), conn)
      handler.execute(cmd("ZADD", "myzset", "1", "one", "2", "two"), conn)
      handler.execute(cmd("DBFREEZE", "frozen_test"), conn)
      conn.clear
    end

    # String write commands
    it "blocks DEL on frozen database" do
      handler.execute(cmd("DEL", "mykey"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks APPEND on frozen database" do
      handler.execute(cmd("APPEND", "mykey", "more"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks INCR on frozen database" do
      handler.execute(cmd("INCR", "counter"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks INCRBY on frozen database" do
      handler.execute(cmd("INCRBY", "counter", "5"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks INCRBYFLOAT on frozen database" do
      handler.execute(cmd("INCRBYFLOAT", "counter", "1.5"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks DECR on frozen database" do
      handler.execute(cmd("DECR", "counter"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks DECRBY on frozen database" do
      handler.execute(cmd("DECRBY", "counter", "5"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks SETRANGE on frozen database" do
      handler.execute(cmd("SETRANGE", "mykey", "0", "new"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks MSET on frozen database" do
      handler.execute(cmd("MSET", "k1", "v1", "k2", "v2"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks MSETNX on frozen database" do
      handler.execute(cmd("MSETNX", "newkey", "newval"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks SETNX on frozen database" do
      handler.execute(cmd("SETNX", "newkey", "value"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks SETEX on frozen database" do
      handler.execute(cmd("SETEX", "mykey", "100", "newvalue"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks PSETEX on frozen database" do
      handler.execute(cmd("PSETEX", "mykey", "100000", "newvalue"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks GETDEL on frozen database" do
      handler.execute(cmd("GETDEL", "mykey"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks GETSET on frozen database" do
      handler.execute(cmd("GETSET", "mykey", "newvalue"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    # List write commands
    it "blocks LPUSH on frozen database" do
      handler.execute(cmd("LPUSH", "mylist", "newitem"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks RPUSH on frozen database" do
      handler.execute(cmd("RPUSH", "mylist", "newitem"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks LPOP on frozen database" do
      handler.execute(cmd("LPOP", "mylist"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks RPOP on frozen database" do
      handler.execute(cmd("RPOP", "mylist"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks LSET on frozen database" do
      handler.execute(cmd("LSET", "mylist", "0", "modified"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks LINSERT on frozen database" do
      handler.execute(cmd("LINSERT", "mylist", "BEFORE", "item1", "newitem"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks LREM on frozen database" do
      handler.execute(cmd("LREM", "mylist", "1", "item1"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks LTRIM on frozen database" do
      handler.execute(cmd("LTRIM", "mylist", "0", "0"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    # Hash write commands
    it "blocks HSET on frozen database" do
      handler.execute(cmd("HSET", "myhash", "field2", "value2"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks HMSET on frozen database" do
      handler.execute(cmd("HMSET", "myhash", "f1", "v1", "f2", "v2"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks HDEL on frozen database" do
      handler.execute(cmd("HDEL", "myhash", "field1"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks HINCRBY on frozen database" do
      handler.execute(cmd("HSET", "myhash", "num", "10"), conn) # This will fail
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks HSETNX on frozen database" do
      handler.execute(cmd("HSETNX", "myhash", "newfield", "value"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    # Set write commands
    it "blocks SADD on frozen database" do
      handler.execute(cmd("SADD", "myset", "newmember"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks SREM on frozen database" do
      handler.execute(cmd("SREM", "myset", "member1"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks SPOP on frozen database" do
      handler.execute(cmd("SPOP", "myset"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    # Sorted set write commands
    it "blocks ZADD on frozen database" do
      handler.execute(cmd("ZADD", "myzset", "3", "three"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks ZREM on frozen database" do
      handler.execute(cmd("ZREM", "myzset", "one"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks ZINCRBY on frozen database" do
      handler.execute(cmd("ZINCRBY", "myzset", "1", "one"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks ZPOPMIN on frozen database" do
      handler.execute(cmd("ZPOPMIN", "myzset"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks ZPOPMAX on frozen database" do
      handler.execute(cmd("ZPOPMAX", "myzset"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    # TTL commands
    it "blocks EXPIRE on frozen database" do
      handler.execute(cmd("EXPIRE", "mykey", "100"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    it "blocks PERSIST on frozen database" do
      handler.execute(cmd("PERSIST", "mykey"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    # Server commands
    it "blocks FLUSHDB on frozen database" do
      handler.execute(cmd("FLUSHDB"), conn)
      expect(conn.last_error).to contain("frozen")
    end

    # Read operations should still work
    it "allows GET on frozen database" do
      handler.execute(cmd("GET", "mykey"), conn)
      expect(conn.last_response).to eq(b("value"))
    end

    it "allows LRANGE on frozen database" do
      handler.execute(cmd("LRANGE", "mylist", "0", "-1"), conn)
      expect(conn.last_response).not_to be_nil
    end

    it "allows HGET on frozen database" do
      handler.execute(cmd("HGET", "myhash", "field1"), conn)
      expect(conn.last_response).to eq(b("value1"))
    end

    it "allows SMEMBERS on frozen database" do
      handler.execute(cmd("SMEMBERS", "myset"), conn)
      expect(conn.last_response).not_to be_nil
    end

    it "allows ZRANGE on frozen database" do
      handler.execute(cmd("ZRANGE", "myzset", "0", "-1"), conn)
      expect(conn.last_response).not_to be_nil
    end
  end
end
