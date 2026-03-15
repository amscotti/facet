require "./database"

module Redis
  alias DatabaseId = Int32 | String

  struct DatabaseInfo
    getter id : DatabaseId
    getter key_count : Int64
    getter created_at : Time
    getter? frozen : Bool

    def initialize(@id : DatabaseId, @key_count : Int64, @created_at : Time, @frozen : Bool)
    end
  end

  class DatabaseManager
    DEFAULT_DB_COUNT = 16

    @databases : Hash(DatabaseId, Database)
    @frozen : Set(DatabaseId)
    @created_at : Hash(DatabaseId, Time)
    @mutex : Mutex

    def initialize(default_count = DEFAULT_DB_COUNT)
      @databases = Hash(DatabaseId, Database).new
      @frozen = Set(DatabaseId).new
      @created_at = Hash(DatabaseId, Time).new
      @mutex = Mutex.new

      # Create default numeric databases (0-15 like Redis)
      default_count.times do |i|
        create_database_internal(i)
      end
    end

    # Get a database by ID (returns nil if not exists)
    def get(id : DatabaseId) : Database?
      @mutex.synchronize do
        @databases[id]?
      end
    end

    # Get a database, creating if it doesn't exist (for numeric IDs only)
    def get_or_create(id : Int32) : Database
      @mutex.synchronize do
        db = @databases[id]?
        return db if db
        create_database_internal(id)
      end
    end

    # Create a new named or numeric database
    def create_database(id : DatabaseId, if_not_exists : Bool = false) : {Bool, String?}
      @mutex.synchronize do
        if @databases.has_key?(id)
          return {false, nil} if if_not_exists
          return {false, "database '#{id}' already exists"}
        end
        create_database_internal(id)
        {true, nil}
      end
    end

    # Drop/delete a database
    def drop_database(id : DatabaseId, if_exists : Bool = false) : {Bool, String?}
      @mutex.synchronize do
        # Don't allow dropping default numeric databases
        if id.is_a?(Int32) && id < DEFAULT_DB_COUNT
          return {false, "cannot drop default database #{id}"}
        end

        unless @databases.has_key?(id)
          return {false, nil} if if_exists
          return {false, "database '#{id}' does not exist"}
        end

        @databases.delete(id)
        @frozen.delete(id)
        @created_at.delete(id)
        {true, nil}
      end
    end

    # Check if database exists
    def database_exists?(id : DatabaseId) : Bool
      @mutex.synchronize do
        @databases.has_key?(id)
      end
    end

    # List all databases with their info
    def list_databases(pattern : String? = nil) : Array(DatabaseInfo)
      @mutex.synchronize do
        result = [] of DatabaseInfo
        @databases.each do |db_id, database|
          # Filter by pattern if provided
          if pattern
            id_str = db_id.to_s
            next unless matches_pattern?(id_str, pattern)
          end

          created = @created_at[db_id]? || Time.utc
          result << DatabaseInfo.new(
            id: db_id,
            key_count: database.size.to_i64,
            created_at: created,
            frozen: @frozen.includes?(db_id)
          )
        end
        result
      end
    end

    # Get info for a specific database
    def database_info(id : DatabaseId) : DatabaseInfo?
      @mutex.synchronize do
        db = @databases[id]?
        return nil unless db

        created = @created_at[id]? || Time.utc
        DatabaseInfo.new(
          id: id,
          key_count: db.size.to_i64,
          created_at: created,
          frozen: @frozen.includes?(id)
        )
      end
    end

    # Copy all data from source to destination database
    def copy_database(source_id : DatabaseId, dest_id : DatabaseId, replace : Bool = false) : {Bool, String?}
      @mutex.synchronize do
        source = @databases[source_id]?
        return {false, "source database '#{source_id}' does not exist"} unless source

        if @databases.has_key?(dest_id) && !replace
          return {false, "destination database '#{dest_id}' already exists"}
        end

        # Create new database and copy all data
        dest = Database.new
        copy_database_data(source, dest)

        @databases[dest_id] = dest
        @created_at[dest_id] = Time.utc
        @frozen.delete(dest_id) # New copy is not frozen

        {true, nil}
      end
    end

    # Copy keys matching pattern from source to destination
    def copy_keys(source_id : DatabaseId, dest_id : DatabaseId, pattern : String) : {Int64, String?}
      @mutex.synchronize do
        source = @databases[source_id]?
        return {0_i64, "source database '#{source_id}' does not exist"} unless source

        dest = @databases[dest_id]?
        return {0_i64, "destination database '#{dest_id}' does not exist"} unless dest

        if @frozen.includes?(dest_id)
          return {0_i64, "destination database '#{dest_id}' is frozen"}
        end

        count = copy_matching_keys(source, dest, pattern)
        {count, nil}
      end
    end

    # Reset a database (clear all data but keep it)
    def reset_database(id : DatabaseId) : {Bool, String?}
      @mutex.synchronize do
        db = @databases[id]?
        return {false, "database '#{id}' does not exist"} unless db

        if @frozen.includes?(id)
          return {false, "database '#{id}' is frozen"}
        end

        db.clear
        {true, nil}
      end
    end

    # Freeze a database (make read-only)
    def freeze_database(id : DatabaseId) : {Bool, String?}
      @mutex.synchronize do
        return {false, "database '#{id}' does not exist"} unless @databases.has_key?(id)

        if @frozen.includes?(id)
          return {false, "database '#{id}' is already frozen"}
        end

        @frozen.add(id)
        {true, nil}
      end
    end

    # Unfreeze a database (make writable)
    def unfreeze_database(id : DatabaseId) : {Bool, String?}
      @mutex.synchronize do
        return {false, "database '#{id}' does not exist"} unless @databases.has_key?(id)

        unless @frozen.includes?(id)
          return {false, "database '#{id}' is not frozen"}
        end

        @frozen.delete(id)
        {true, nil}
      end
    end

    # Check if database is frozen
    def frozen?(id : DatabaseId) : Bool
      @mutex.synchronize do
        @frozen.includes?(id)
      end
    end

    # Flush all databases
    def flush_all : Nil
      @mutex.synchronize do
        @databases.each_value do |database|
          database.clear
        end
      end
    end

    # Get total database count
    def database_count : Int32
      @mutex.synchronize do
        @databases.size
      end
    end

    private def create_database_internal(id : DatabaseId) : Database
      db = Database.new
      @databases[id] = db
      @created_at[id] = Time.utc
      db
    end

    private def copy_database_data(source : Database, dest : Database) : Nil
      source.keys.each do |key|
        entry = source.get_entry(key)
        next unless entry

        # Deep copy the entry data
        case data = entry.data
        when Bytes
          dest.set(key.clone, data.clone, entry.ttl)
        when ListType
          copy_list(key.clone, data, dest, entry.ttl)
        when HashType
          copy_hash(key.clone, data, dest, entry.ttl)
        when SetType
          copy_set(key.clone, data, dest, entry.ttl)
        when SortedSetType
          copy_sorted_set(key.clone, data, dest, entry.ttl)
        end
      end
    end

    private def copy_matching_keys(source : Database, dest : Database, pattern : String) : Int64
      count = 0_i64
      matcher = GlobMatcher.compile(pattern)

      source.keys.each do |key|
        next unless matcher.matches?(key)

        entry = source.get_entry(key)
        next unless entry

        case data = entry.data
        when Bytes
          dest.set(key.clone, data.clone, entry.ttl)
        when ListType
          copy_list(key.clone, data, dest, entry.ttl)
        when HashType
          copy_hash(key.clone, data, dest, entry.ttl)
        when SetType
          copy_set(key.clone, data, dest, entry.ttl)
        when SortedSetType
          copy_sorted_set(key.clone, data, dest, entry.ttl)
        end
        count += 1
      end
      count
    end

    private def copy_list(key : Bytes, source : ListType, dest : Database, ttl : Int64?) : Nil
      list = dest.get_or_create_list(key)
      source.lrange(0, -1).each do |item|
        list.rpush([item.clone])
      end
      dest.expire(key, ttl) if ttl
    end

    private def copy_hash(key : Bytes, source : HashType, dest : Database, ttl : Int64?) : Nil
      hash = dest.get_or_create_hash(key)
      source.hkeys.each do |field|
        if val = source.hget(field)
          hash.hset(field.clone, val.clone)
        end
      end
      dest.expire(key, ttl) if ttl
    end

    private def copy_set(key : Bytes, source : SetType, dest : Database, ttl : Int64?) : Nil
      set = dest.get_or_create_set(key)
      source.smembers.each do |member|
        set.sadd([member.clone])
      end
      dest.expire(key, ttl) if ttl
    end

    private def copy_sorted_set(key : Bytes, source : SortedSetType, dest : Database, ttl : Int64?) : Nil
      zset = dest.get_or_create_sorted_set(key)
      # Get all members with scores
      members_scores = source.zrange(0, -1, withscores: true)
      i = 0
      while i < members_scores.size - 1
        member = members_scores[i].as(Bytes)
        score = members_scores[i + 1].as(Float64)
        zset.zadd(member.clone, score)
        i += 2
      end
      dest.expire(key, ttl) if ttl
    end

    private def matches_pattern?(str : String, pattern : String) : Bool
      GlobMatcher.compile(pattern).matches?(str.to_slice)
    end
  end
end
