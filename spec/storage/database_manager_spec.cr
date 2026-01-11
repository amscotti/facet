require "../spec_helper"

Spectator.describe Redis::DatabaseManager do
  let(manager) { Redis::DatabaseManager.new }

  describe "#initialize" do
    it "creates default 16 numeric databases" do
      expect(manager.database_count).to eq(16)
    end

    it "has database 0 available by default" do
      db = manager.get(0)
      expect(db).not_to be_nil
    end

    it "has databases 0-15 available" do
      (0...16).each do |i|
        expect(manager.database_exists?(i)).to be_true
      end
    end
  end

  describe "#get" do
    it "returns database for existing id" do
      db = manager.get(0)
      expect(db).not_to be_nil
    end

    it "returns nil for non-existing id" do
      db = manager.get("nonexistent")
      expect(db).to be_nil
    end
  end

  describe "#get_or_create" do
    it "returns existing database" do
      db1 = manager.get_or_create(0)
      db2 = manager.get_or_create(0)
      expect(db1).to be(db2)
    end

    it "creates new database if not exists" do
      db = manager.get_or_create(20)
      expect(db).not_to be_nil
      expect(manager.database_exists?(20)).to be_true
    end
  end

  describe "#create_database" do
    it "creates named database" do
      success, error = manager.create_database("testdb")
      expect(success).to be_true
      expect(error).to be_nil
      expect(manager.database_exists?("testdb")).to be_true
    end

    it "fails if database already exists" do
      manager.create_database("testdb")
      success, error = manager.create_database("testdb")
      expect(success).to be_false
      expect(error).to contain("already exists")
    end

    it "succeeds with IF NOT EXISTS when database exists" do
      manager.create_database("testdb")
      success, error = manager.create_database("testdb", if_not_exists: true)
      expect(success).to be_false
      expect(error).to be_nil
    end
  end

  describe "#drop_database" do
    it "drops named database" do
      manager.create_database("testdb")
      success, _ = manager.drop_database("testdb")
      expect(success).to be_true
      expect(manager.database_exists?("testdb")).to be_false
    end

    it "fails to drop default numeric databases" do
      success, error = manager.drop_database(0)
      expect(success).to be_false
      expect(error).to contain("cannot drop default")
    end

    it "fails for non-existing database" do
      success, error = manager.drop_database("nonexistent")
      expect(success).to be_false
      expect(error).to contain("does not exist")
    end

    it "succeeds with IF EXISTS when database doesn't exist" do
      success, error = manager.drop_database("nonexistent", if_exists: true)
      expect(success).to be_false
      expect(error).to be_nil
    end
  end

  describe "#list_databases" do
    it "lists all databases" do
      manager.create_database("testdb1")
      manager.create_database("testdb2")
      databases = manager.list_databases
      expect(databases.size).to be >= 18 # 16 default + 2 created
    end

    it "filters by pattern" do
      manager.create_database("testdb1")
      manager.create_database("testdb2")
      manager.create_database("other")
      databases = manager.list_databases("test*")
      expect(databases.size).to eq(2)
    end
  end

  describe "#database_info" do
    it "returns info for existing database" do
      manager.create_database("testdb")
      db = manager.get("testdb").as(Redis::Database)
      db.set(b("key1"), b("value1"))
      db.set(b("key2"), b("value2"))

      info = manager.database_info("testdb")
      expect(info).not_to be_nil
      expect(info.as(Redis::DatabaseInfo).key_count).to eq(2)
      expect(info.as(Redis::DatabaseInfo).frozen?).to be_false
    end

    it "returns nil for non-existing database" do
      info = manager.database_info("nonexistent")
      expect(info).to be_nil
    end
  end

  describe "#copy_database" do
    it "copies all data from source to destination" do
      db = manager.get_or_create(0)
      db.set(b("key1"), b("value1"))
      db.set(b("key2"), b("value2"))

      success, _ = manager.copy_database(0, "backup")
      expect(success).to be_true

      backup = manager.get("backup").as(Redis::Database)
      expect(backup.get(b("key1"))).to eq(b("value1"))
      expect(backup.get(b("key2"))).to eq(b("value2"))
    end

    it "fails if destination exists without REPLACE" do
      manager.create_database("backup")
      success, error = manager.copy_database(0, "backup")
      expect(success).to be_false
      expect(error).to contain("already exists")
    end

    it "succeeds with REPLACE when destination exists" do
      manager.create_database("backup")
      db = manager.get_or_create(0)
      db.set(b("key1"), b("value1"))

      success, _ = manager.copy_database(0, "backup", replace: true)
      expect(success).to be_true

      backup = manager.get("backup").as(Redis::Database)
      expect(backup.get(b("key1"))).to eq(b("value1"))
    end
  end

  describe "#copy_keys" do
    it "copies matching keys" do
      db0 = manager.get_or_create(0)
      db0.set(b("user:1"), b("alice"))
      db0.set(b("user:2"), b("bob"))
      db0.set(b("post:1"), b("hello"))

      manager.create_database("backup")
      count, error = manager.copy_keys(0, "backup", "user:*")
      expect(error).to be_nil
      expect(count).to eq(2)

      backup = manager.get("backup").as(Redis::Database)
      expect(backup.get(b("user:1"))).to eq(b("alice"))
      expect(backup.get(b("user:2"))).to eq(b("bob"))
      expect(backup.get(b("post:1"))).to be_nil
    end
  end

  describe "#reset_database" do
    it "clears all data in database" do
      db = manager.get_or_create(0)
      db.set(b("key1"), b("value1"))

      success, _ = manager.reset_database(0)
      expect(success).to be_true
      expect(db.size).to eq(0)
    end

    it "fails for frozen database" do
      manager.create_database("testdb")
      manager.freeze_database("testdb")

      success, error = manager.reset_database("testdb")
      expect(success).to be_false
      expect(error).to contain("frozen")
    end
  end

  describe "#freeze_database and #unfreeze_database" do
    it "freezes database" do
      manager.create_database("testdb")
      success, _ = manager.freeze_database("testdb")
      expect(success).to be_true
      expect(manager.frozen?("testdb")).to be_true
    end

    it "unfreezes database" do
      manager.create_database("testdb")
      manager.freeze_database("testdb")

      success, _ = manager.unfreeze_database("testdb")
      expect(success).to be_true
      expect(manager.frozen?("testdb")).to be_false
    end

    it "fails to freeze already frozen database" do
      manager.create_database("testdb")
      manager.freeze_database("testdb")

      success, error = manager.freeze_database("testdb")
      expect(success).to be_false
      expect(error).to contain("already frozen")
    end
  end

  describe "#flush_all" do
    it "clears all databases" do
      db0 = manager.get_or_create(0)
      db0.set(b("key1"), b("value1"))

      manager.create_database("testdb")
      testdb = manager.get("testdb").as(Redis::Database)
      testdb.set(b("key2"), b("value2"))

      manager.flush_all

      expect(db0.size).to eq(0)
      expect(testdb.size).to eq(0)
    end
  end

  # ============================================================================
  # Database Copy - TTL Preservation Tests
  # ============================================================================

  describe "TTL preservation during copy" do
    it "preserves TTL for string values" do
      db = manager.get_or_create(0)
      future_ttl = Time.utc.to_unix_ms + 60_000 # 60 seconds from now
      db.set(b("key1"), b("value1"), future_ttl)

      success, _ = manager.copy_database(0, "backup")
      expect(success).to be_true

      backup = manager.get("backup").as(Redis::Database)
      pttl = backup.pttl(b("key1"))
      expect(pttl).to be > 0
      expect(pttl).to be <= 60_000
    end

    it "preserves TTL for list values" do
      db = manager.get_or_create(0)
      list = db.get_or_create_list(b("mylist"))
      list.rpush([b("item1"), b("item2")])
      future_ttl = Time.utc.to_unix_ms + 60_000
      db.expire(b("mylist"), future_ttl)

      success, _ = manager.copy_database(0, "backup")
      expect(success).to be_true

      backup = manager.get("backup").as(Redis::Database)
      pttl = backup.pttl(b("mylist"))
      expect(pttl).to be > 0
      expect(pttl).to be <= 60_000
    end

    it "preserves TTL for hash values" do
      db = manager.get_or_create(0)
      hash = db.get_or_create_hash(b("myhash"))
      hash.hset(b("field1"), b("value1"))
      future_ttl = Time.utc.to_unix_ms + 60_000
      db.expire(b("myhash"), future_ttl)

      success, _ = manager.copy_database(0, "backup")
      expect(success).to be_true

      backup = manager.get("backup").as(Redis::Database)
      pttl = backup.pttl(b("myhash"))
      expect(pttl).to be > 0
      expect(pttl).to be <= 60_000
    end

    it "preserves TTL for set values" do
      db = manager.get_or_create(0)
      set = db.get_or_create_set(b("myset"))
      set.sadd([b("member1"), b("member2")])
      future_ttl = Time.utc.to_unix_ms + 60_000
      db.expire(b("myset"), future_ttl)

      success, _ = manager.copy_database(0, "backup")
      expect(success).to be_true

      backup = manager.get("backup").as(Redis::Database)
      pttl = backup.pttl(b("myset"))
      expect(pttl).to be > 0
      expect(pttl).to be <= 60_000
    end

    it "preserves TTL for sorted set values" do
      db = manager.get_or_create(0)
      zset = db.get_or_create_sorted_set(b("myzset"))
      zset.zadd(b("member1"), 1.0)
      zset.zadd(b("member2"), 2.0)
      future_ttl = Time.utc.to_unix_ms + 60_000
      db.expire(b("myzset"), future_ttl)

      success, _ = manager.copy_database(0, "backup")
      expect(success).to be_true

      backup = manager.get("backup").as(Redis::Database)
      pttl = backup.pttl(b("myzset"))
      expect(pttl).to be > 0
      expect(pttl).to be <= 60_000
    end

    it "preserves TTL during copy_keys for all types" do
      db = manager.get_or_create(0)
      future_ttl = Time.utc.to_unix_ms + 60_000

      # String with TTL
      db.set(b("test:string"), b("value"), future_ttl)

      # List with TTL
      list = db.get_or_create_list(b("test:list"))
      list.rpush([b("item1")])
      db.expire(b("test:list"), future_ttl)

      # Hash with TTL
      hash = db.get_or_create_hash(b("test:hash"))
      hash.hset(b("field"), b("value"))
      db.expire(b("test:hash"), future_ttl)

      manager.create_database("backup")
      count, _ = manager.copy_keys(0, "backup", "test:*")
      expect(count).to eq(3)

      backup = manager.get("backup").as(Redis::Database)
      expect(backup.pttl(b("test:string"))).to be > 0
      expect(backup.pttl(b("test:list"))).to be > 0
      expect(backup.pttl(b("test:hash"))).to be > 0
    end
  end

  # ============================================================================
  # Database Copy - Continue Adding Items Use Case
  # ============================================================================

  describe "copy database and continue modifications" do
    it "allows adding new keys to copied database" do
      # Setup source with initial data
      source = manager.get_or_create(0)
      source.set(b("existing"), b("value"))

      # Copy to test database
      manager.copy_database(0, "test_copy")
      test_db = manager.get("test_copy").as(Redis::Database)

      # Add new keys to copy - should work
      test_db.set(b("new_key"), b("new_value"))
      expect(test_db.get(b("new_key"))).to eq(b("new_value"))
      expect(test_db.get(b("existing"))).to eq(b("value"))

      # Source should be unchanged
      expect(source.get(b("new_key"))).to be_nil
      expect(source.get(b("existing"))).to eq(b("value"))
    end

    it "allows modifying existing keys in copied database" do
      source = manager.get_or_create(0)
      source.set(b("key"), b("original"))

      manager.copy_database(0, "test_copy")
      test_db = manager.get("test_copy").as(Redis::Database)

      # Modify in copy
      test_db.set(b("key"), b("modified"))
      expect(test_db.get(b("key"))).to eq(b("modified"))

      # Source unchanged
      expect(source.get(b("key"))).to eq(b("original"))
    end

    it "allows deleting keys from copied database" do
      source = manager.get_or_create(0)
      source.set(b("key1"), b("value1"))
      source.set(b("key2"), b("value2"))

      manager.copy_database(0, "test_copy")
      test_db = manager.get("test_copy").as(Redis::Database)

      # Delete from copy
      test_db.del(b("key1"))
      expect(test_db.exists?(b("key1"))).to be_false
      expect(test_db.get(b("key2"))).to eq(b("value2"))

      # Source unchanged
      expect(source.get(b("key1"))).to eq(b("value1"))
      expect(source.get(b("key2"))).to eq(b("value2"))
    end

    it "allows modifications to lists in copied database" do
      source = manager.get_or_create(0)
      list = source.get_or_create_list(b("mylist"))
      list.rpush([b("item1"), b("item2")])

      manager.copy_database(0, "test_copy")
      test_db = manager.get("test_copy").as(Redis::Database)
      test_list = test_db.get_list(b("mylist"))

      # Modify copy
      test_list.as(Redis::ListType).rpush([b("item3")])
      expect(test_list.as(Redis::ListType).llen).to eq(3)

      # Source unchanged
      expect(list.llen).to eq(2)
    end

    it "allows modifications to hashes in copied database" do
      source = manager.get_or_create(0)
      hash = source.get_or_create_hash(b("myhash"))
      hash.hset(b("field1"), b("value1"))

      manager.copy_database(0, "test_copy")
      test_db = manager.get("test_copy").as(Redis::Database)
      test_hash = test_db.get_hash(b("myhash"))

      # Modify copy
      test_hash.as(Redis::HashType).hset(b("field2"), b("value2"))
      expect(test_hash.as(Redis::HashType).hlen).to eq(2)

      # Source unchanged
      expect(hash.hlen).to eq(1)
    end
  end

  # ============================================================================
  # Test Isolation Scenario - Primary Use Case
  # ============================================================================

  describe "test isolation use case" do
    it "supports creating isolated test databases from baseline" do
      # Setup baseline/fixture data
      baseline = manager.get_or_create(0)
      baseline.set(b("user:1:name"), b("Alice"))
      baseline.set(b("user:1:email"), b("alice@example.com"))
      baseline.set(b("config:setting"), b("production"))

      list = baseline.get_or_create_list(b("user:1:orders"))
      list.rpush([b("order:100"), b("order:101")])

      # Freeze baseline to prevent accidental modifications
      manager.freeze_database(0)

      # Create test database for test case 1
      manager.copy_database(0, "test_case_1")
      test1 = manager.get("test_case_1").as(Redis::Database)

      # Test case 1: Modify user and add order
      test1.set(b("user:1:name"), b("Alice Updated"))
      test1.get_or_create_list(b("user:1:orders")).rpush([b("order:102")])
      expect(test1.get(b("user:1:name"))).to eq(b("Alice Updated"))
      expect(test1.get_list(b("user:1:orders")).as(Redis::ListType).llen).to eq(3)

      # Create test database for test case 2 from same baseline
      manager.copy_database(0, "test_case_2")
      test2 = manager.get("test_case_2").as(Redis::Database)

      # Test case 2: Different modifications
      test2.set(b("user:1:email"), b("newemail@example.com"))
      test2.del(b("config:setting"))
      expect(test2.get(b("user:1:email"))).to eq(b("newemail@example.com"))
      expect(test2.exists?(b("config:setting"))).to be_false

      # Verify baseline unchanged (still frozen)
      source = manager.get(0).as(Redis::Database)
      expect(source.get(b("user:1:name"))).to eq(b("Alice"))
      expect(source.get(b("user:1:email"))).to eq(b("alice@example.com"))
      expect(source.get_list(b("user:1:orders")).as(Redis::ListType).llen).to eq(2)

      # Verify test databases are independent
      expect(test1.get(b("user:1:email"))).to eq(b("alice@example.com")) # unchanged in test1
      expect(test2.get(b("user:1:name"))).to eq(b("Alice"))              # unchanged in test2

      # Clean up test databases
      manager.drop_database("test_case_1")
      manager.drop_database("test_case_2")
      expect(manager.database_exists?("test_case_1")).to be_false
      expect(manager.database_exists?("test_case_2")).to be_false
    end

    it "supports selective key copying for partial test fixtures" do
      # Setup full production-like data
      source = manager.get_or_create(0)
      source.set(b("user:1:data"), b("user1"))
      source.set(b("user:2:data"), b("user2"))
      source.set(b("product:1:data"), b("product1"))
      source.set(b("product:2:data"), b("product2"))
      source.set(b("cache:temp"), b("cached"))

      # Create test database with only user data
      manager.create_database("user_tests")
      count, _ = manager.copy_keys(0, "user_tests", "user:*")
      expect(count).to eq(2)

      user_db = manager.get("user_tests").as(Redis::Database)
      expect(user_db.get(b("user:1:data"))).to eq(b("user1"))
      expect(user_db.get(b("user:2:data"))).to eq(b("user2"))
      expect(user_db.get(b("product:1:data"))).to be_nil
      expect(user_db.get(b("cache:temp"))).to be_nil
    end

    it "supports parallel test execution with separate databases" do
      baseline = manager.get_or_create(0)
      baseline.set(b("counter"), b("0"))

      # Simulate parallel test execution
      test_dbs = [] of String
      5.times do |i|
        db_name = "parallel_test_#{i}"
        manager.copy_database(0, db_name)
        test_dbs << db_name
      end

      # Each test modifies its own database
      test_dbs.each_with_index do |db_name, i|
        db = manager.get(db_name).as(Redis::Database)
        db.set(b("counter"), b("#{i + 1}"))
        db.set(b("test_id"), b("#{i}"))
      end

      # Verify isolation - each database has its own state
      test_dbs.each_with_index do |db_name, i|
        db = manager.get(db_name).as(Redis::Database)
        expect(db.get(b("counter"))).to eq(b("#{i + 1}"))
        expect(db.get(b("test_id"))).to eq(b("#{i}"))
      end

      # Cleanup
      test_dbs.each { |name| manager.drop_database(name) }
    end
  end
end
