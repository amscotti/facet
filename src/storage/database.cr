require "./data_types/list_type"

module Redis
  # Size limits (configurable via environment variables)
  MAX_KEY_SIZE   = (ENV["FACET_MAX_KEY_SIZE"]?.try(&.to_i) || 1024 * 1024).to_i32         # 1MB default
  MAX_VALUE_SIZE = (ENV["FACET_MAX_VALUE_SIZE"]?.try(&.to_i) || 512 * 1024 * 1024).to_i64 # 512MB default

  alias DataValue = Bytes | ListType | HashType | SetType | SortedSetType

  # LRU cache for compiled regex patterns to avoid re-compilation
  # Improves performance of SCAN/HSCAN/SSCAN/ZSCAN operations
  module RegexCache
    CACHE_SIZE = 128
    @@cache = Hash(String, Regex).new(initial_capacity: CACHE_SIZE)
    @@access_order = [] of String
    @@mutex = Mutex.new

    def self.get(pattern : String) : Regex
      @@mutex.synchronize do
        if regex = @@cache[pattern]?
          # Update access order (move to end)
          @@access_order.delete(pattern)
          @@access_order << pattern
          return regex
        end

        # Cache miss - compile new regex
        regex = compile_pattern(pattern)

        # Evict oldest if at capacity
        if @@cache.size >= CACHE_SIZE
          oldest = @@access_order.shift
          @@cache.delete(oldest)
        end

        # Add to cache
        @@cache[pattern] = regex
        @@access_order << pattern
        regex
      end
    end

    private def self.compile_pattern(pattern : String) : Regex
      escaped = Regex.escape(pattern)
      regex_str = escaped.gsub("\\*", ".*").gsub("\\?", ".")
      Regex.new("^#{regex_str}$")
    end
  end

  # Helper module for efficient numeric conversions
  module NumericConversions
    # Parse Int64 from Bytes without intermediate allocations
    def self.bytes_to_i64(bytes : Bytes?) : Int64
      return 0_i64 unless bytes
      String.new(bytes).to_i64
    end

    # Parse Float64 from Bytes without intermediate allocations
    def self.bytes_to_f64(bytes : Bytes?) : Float64
      return 0.0_f64 unless bytes
      String.new(bytes).to_f64
    end

    # Convert Int64 to Bytes
    def self.i64_to_bytes(num : Int64) : Bytes
      num.to_s.to_slice
    end

    # Convert Float64 to Bytes
    def self.f64_to_bytes(num : Float64) : Bytes
      num.to_s.to_slice
    end
  end

  class HashType
    @data : Hash(Bytes, Bytes)

    def initialize
      @data = Hash(Bytes, Bytes).new
    end

    def hset(field : Bytes, value : Bytes) : Bool
      is_new = !@data.has_key?(field)
      @data[field] = value
      is_new
    end

    def hget(field : Bytes) : Bytes?
      @data[field]?
    end

    def hdel(fields : Array(Bytes)) : Int64
      count = 0_i64
      fields.each do |field|
        count += 1 if @data.delete(field)
      end
      count
    end

    def hexists?(field : Bytes) : Bool
      @data.has_key?(field)
    end

    def hlen : Int64
      @data.size.to_i64
    end

    def hkeys : Array(Bytes)
      @data.keys
    end

    def hvals : Array(Bytes)
      @data.values
    end

    def hgetall : Array(Bytes)
      result = [] of Bytes
      @data.each do |key, val|
        result << key
        result << val
      end
      result
    end

    def hincrby(field : Bytes, increment : Int64) : Int64
      current = @data[field]?
      value = NumericConversions.bytes_to_i64(current)
      new_value = value + increment
      @data[field] = NumericConversions.i64_to_bytes(new_value)
      new_value
    end

    def hincrbyfloat(field : Bytes, increment : Float64) : Float64
      current = @data[field]?
      value = NumericConversions.bytes_to_f64(current)
      new_value = value + increment
      @data[field] = NumericConversions.f64_to_bytes(new_value)
      new_value
    end

    def hsetnx(field : Bytes, value : Bytes) : Bool
      return false if @data.has_key?(field)
      @data[field] = value
      true
    end

    def hstrlen(field : Bytes) : Int64
      val = @data[field]?
      val ? val.size.to_i64 : 0_i64
    end

    def empty? : Bool
      @data.empty?
    end

    # Cursor-based field iteration
    def hscan(cursor : Int64, pattern : String? = nil, count : Int64 = 10) : {Int64, Array(Bytes)}
      all_fields = @data.keys
      start_idx = cursor.to_i

      return {0_i64, [] of Bytes} if start_idx >= all_fields.size

      results = [] of Bytes
      idx = start_idx
      regex = pattern ? pattern_to_regex(pattern) : nil

      scanned = 0
      max_scan = count * 2
      while idx < all_fields.size && results.size < count * 2 # Return field + value pairs
        field = all_fields[idx]
        if regex.nil? || regex.matches?(String.new(field))
          results << field
          results << @data[field]
        end
        idx += 1
        scanned += 1
        break if scanned >= max_scan
      end

      next_cursor = idx >= all_fields.size ? 0_i64 : idx.to_i64
      {next_cursor, results}
    end

    private def pattern_to_regex(pattern : String) : Regex
      RegexCache.get(pattern)
    end
  end

  class SetType
    @data : Set(Bytes)

    def initialize
      @data = Set(Bytes).new
    end

    def sadd(members : Array(Bytes)) : Int64
      count = 0_i64
      members.each do |member|
        count += 1 if @data.add?(member)
      end
      count
    end

    def srem(members : Array(Bytes)) : Int64
      count = 0_i64
      members.each do |member|
        count += 1 if @data.delete(member)
      end
      count
    end

    def sismember?(member : Bytes) : Bool
      @data.includes?(member)
    end

    def smismember(members : Array(Bytes)) : Array(Bool)
      members.map { |member| @data.includes?(member) }
    end

    def smembers : Array(Bytes)
      @data.to_a
    end

    def scard : Int64
      @data.size.to_i64
    end

    def spop(count : Int32 = 1) : Array(Bytes)
      result = [] of Bytes
      count.times do
        break if @data.empty?
        member = @data.first
        @data.delete(member)
        result << member
      end
      result
    end

    def srandmember(count : Int64) : Array(Bytes)
      arr = @data.to_a
      return [] of Bytes if arr.empty?

      if count > 0
        arr.sample(Math.min(count.to_i, arr.size))
      else
        result = [] of Bytes
        count.abs.times { result << arr.sample }
        result
      end
    end

    def sunion(other : SetType) : SetType
      result = SetType.new
      @data.each { |member| result.sadd([member]) }
      other.@data.each { |member| result.sadd([member]) }
      result
    end

    def sinter(other : SetType) : SetType
      result = SetType.new
      @data.each do |member|
        result.sadd([member]) if other.sismember?(member)
      end
      result
    end

    def sdiff(other : SetType) : SetType
      result = SetType.new
      @data.each do |member|
        result.sadd([member]) unless other.sismember?(member)
      end
      result
    end

    def smove(destination : SetType, member : Bytes) : Bool
      return false unless @data.delete(member)
      destination.sadd([member])
      true
    end

    def empty? : Bool
      @data.empty?
    end

    # Cursor-based member iteration
    def sscan(cursor : Int64, pattern : String? = nil, count : Int64 = 10) : {Int64, Array(Bytes)}
      all_members = @data.to_a
      start_idx = cursor.to_i

      return {0_i64, [] of Bytes} if start_idx >= all_members.size

      results = [] of Bytes
      idx = start_idx
      regex = pattern ? pattern_to_regex(pattern) : nil

      scanned = 0
      max_scan = count * 2
      while idx < all_members.size && results.size < count
        member = all_members[idx]
        if regex.nil? || regex.matches?(String.new(member))
          results << member
        end
        idx += 1
        scanned += 1
        break if scanned >= max_scan
      end

      next_cursor = idx >= all_members.size ? 0_i64 : idx.to_i64
      {next_cursor, results}
    end

    private def pattern_to_regex(pattern : String) : Regex
      RegexCache.get(pattern)
    end
  end

  class SortedSetType
    @scores : Hash(Bytes, Float64)
    @members_by_score : Array({Float64, Bytes})

    def initialize
      @scores = Hash(Bytes, Float64).new
      @members_by_score = [] of {Float64, Bytes}
    end

    def zadd(member : Bytes, score : Float64, nx : Bool = false, xx : Bool = false, gt : Bool = false, lt : Bool = false, ch : Bool = false) : {Int64, Bool}
      existing_score = @scores[member]?
      added = 0_i64
      changed = false

      if existing_score
        return {0_i64, false} if nx

        should_update = if gt && lt
                          false
                        elsif gt
                          score > existing_score
                        elsif lt
                          score < existing_score
                        else
                          true
                        end

        if should_update && score != existing_score
          remove_from_sorted(member, existing_score)
          @scores[member] = score
          insert_sorted(member, score)
          changed = true
        end
      else
        return {0_i64, false} if xx
        @scores[member] = score
        insert_sorted(member, score)
        added = 1_i64
        changed = true
      end

      {ch ? (changed ? 1_i64 : 0_i64) : added, changed}
    end

    def zrem(members : Array(Bytes)) : Int64
      count = 0_i64
      members.each do |member|
        if score = @scores.delete(member)
          remove_from_sorted(member, score)
          count += 1
        end
      end
      count
    end

    def zscore(member : Bytes) : Float64?
      @scores[member]?
    end

    def zrank(member : Bytes) : Int64?
      score = @scores[member]?
      return nil unless score

      # Binary search to find first entry with score >= target
      # O(log n) instead of O(n) linear search
      left = 0
      right = @members_by_score.size

      while left < right
        mid = left + (right - left) // 2
        if @members_by_score[mid][0] < score
          left = mid + 1
        else
          right = mid
        end
      end

      # Now scan from left to find the exact member among same-score entries
      # This is O(k) where k is members with same score (usually small)
      idx = left
      while idx < @members_by_score.size
        entry = @members_by_score[idx]
        break if entry[0] > score # Past our score
        return idx.to_i64 if entry[1] == member
        idx += 1
      end

      nil # Should not happen if @scores is consistent
    end

    def zrevrank(member : Bytes) : Int64?
      rank = zrank(member)
      return nil unless rank
      (@members_by_score.size - 1 - rank).to_i64
    end

    def zcard : Int64
      @scores.size.to_i64
    end

    def zcount(min : Float64, max : Float64, min_exclusive : Bool = false, max_exclusive : Bool = false) : Int64
      @members_by_score.count do |entry|
        score = entry[0]
        in_min = min_exclusive ? score > min : score >= min
        in_max = max_exclusive ? score < max : score <= max
        in_min && in_max
      end.to_i64
    end

    def zrange(start_idx : Int64, end_idx : Int64, reverse : Bool = false, withscores : Bool = false) : Array(Bytes | Float64)
      len = @members_by_score.size.to_i64
      start_norm = normalize_range_index(start_idx, len)
      end_norm = normalize_range_index(end_idx, len)

      return [] of (Bytes | Float64) if start_norm > end_norm || start_norm >= len
      end_norm = len - 1 if end_norm >= len

      result = [] of (Bytes | Float64)
      range = start_norm.to_i..end_norm.to_i

      if reverse
        range.reverse_each do |idx|
          actual_idx = @members_by_score.size - 1 - idx
          entry = @members_by_score[actual_idx]
          result << entry[1]
          result << entry[0] if withscores
        end
      else
        range.each do |idx|
          entry = @members_by_score[idx]
          result << entry[1]
          result << entry[0] if withscores
        end
      end
      result
    end

    def zrangebyscore(min : Float64, max : Float64, withscores : Bool = false, offset : Int64 = 0, count : Int64 = -1, min_exclusive : Bool = false, max_exclusive : Bool = false) : Array(Bytes | Float64)
      result = [] of (Bytes | Float64)
      skipped = 0_i64
      added = 0_i64

      @members_by_score.each do |entry|
        # Check max bound (exclusive or inclusive)
        if max_exclusive
          break if entry[0] >= max
        else
          break if entry[0] > max
        end
        # Check min bound (exclusive or inclusive)
        if min_exclusive
          next if entry[0] <= min
        else
          next if entry[0] < min
        end

        if skipped < offset
          skipped += 1
          next
        end

        result << entry[1]
        result << entry[0] if withscores
        added += 1

        break if count > 0 && added >= count
      end
      result
    end

    def zincrby(member : Bytes, increment : Float64) : Float64
      current = @scores[member]? || 0.0
      new_score = current + increment

      if @scores.has_key?(member)
        remove_from_sorted(member, current)
      end

      @scores[member] = new_score
      insert_sorted(member, new_score)
      new_score
    end

    def zpopmin(count : Int32 = 1) : Array({Bytes, Float64})
      result = [] of {Bytes, Float64}
      count.times do
        break if @members_by_score.empty?
        entry = @members_by_score.shift
        @scores.delete(entry[1])
        result << {entry[1], entry[0]}
      end
      result
    end

    def zpopmax(count : Int32 = 1) : Array({Bytes, Float64})
      result = [] of {Bytes, Float64}
      count.times do
        break if @members_by_score.empty?
        entry = @members_by_score.pop
        @scores.delete(entry[1])
        result << {entry[1], entry[0]}
      end
      result
    end

    def zmscore(members : Array(Bytes)) : Array(Float64?)
      members.map { |member| @scores[member]? }
    end

    def zrandmember(count : Int64, withscores : Bool = false) : Array(Bytes | Float64)
      arr = @scores.keys
      return [] of (Bytes | Float64) if arr.empty?

      abs_count = count.abs

      result = [] of (Bytes | Float64)
      abs_count.times do
        member = arr.sample
        result << member
        if withscores
          result << @scores[member]
        end
      end

      result
    end

    def empty? : Bool
      @scores.empty?
    end

    # Cursor-based member iteration with scores
    def zscan(cursor : Int64, pattern : String? = nil, count : Int64 = 10) : {Int64, Array(Bytes | Float64)}
      all_members = @scores.keys
      start_idx = cursor.to_i

      return {0_i64, [] of (Bytes | Float64)} if start_idx >= all_members.size

      results = [] of (Bytes | Float64)
      idx = start_idx
      regex = pattern ? pattern_to_regex(pattern) : nil

      scanned = 0
      max_scan = count * 2
      while idx < all_members.size && results.size < count * 2 # Return member + score pairs
        member = all_members[idx]
        if regex.nil? || regex.matches?(String.new(member))
          results << member
          results << @scores[member]
        end
        idx += 1
        scanned += 1
        break if scanned >= max_scan
      end

      next_cursor = idx >= all_members.size ? 0_i64 : idx.to_i64
      {next_cursor, results}
    end

    private def pattern_to_regex(pattern : String) : Regex
      RegexCache.get(pattern)
    end

    private def insert_sorted(member : Bytes, score : Float64) : Nil
      insert_idx = @members_by_score.bsearch_index { |entry| entry[0] > score || (entry[0] == score && String.new(entry[1]) > String.new(member)) }
      if insert_idx
        @members_by_score.insert(insert_idx, {score, member})
      else
        @members_by_score.push({score, member})
      end
    end

    private def remove_from_sorted(member : Bytes, score : Float64) : Nil
      @members_by_score.reject! { |entry| entry[1] == member }
    end

    private def normalize_range_index(idx : Int64, len : Int64) : Int64
      if idx < 0
        normalized = len + idx
        normalized < 0 ? 0_i64 : normalized
      else
        idx
      end
    end

    def zrangestore(source : SortedSetType, start_idx : Int64, end_idx : Int64, reverse : Bool = false, byscore : Bool = false) : Int64
      len = source.@members_by_score.size.to_i64
      start_norm = normalize_range_index(start_idx, len)
      end_norm = normalize_range_index(end_idx, len)

      if start_norm > end_norm || start_norm >= len
        return 0_i64
      end

      end_norm = len - 1 if end_norm >= len

      range = if reverse
                (len - 1 - end_norm.to_i)..(len - 1 - start_norm.to_i)
              else
                start_norm.to_i..end_norm.to_i
              end

      result = [] of {Float64, Bytes}

      range.reverse_each do |idx|
        actual_idx = if reverse
                       idx
                     else
                       end_norm.to_i - (idx - start_norm.to_i)
                     end

        entry = source.@members_by_score[actual_idx]
        @scores[entry[1]] = entry[0]
        @members_by_score.push({entry[0], entry[1]})
        result << {entry[0], entry[1]}
      end

      result.size.to_i64
    end
  end

  struct Entry
    getter data : DataValue
    getter ttl : Int64?

    def initialize(@data : DataValue, @ttl : Int64? = nil)
    end

    def string_value : Bytes?
      @data.as?(Bytes)
    end

    def list_value : ListType?
      @data.as?(ListType)
    end

    def hash_value : HashType?
      @data.as?(HashType)
    end

    def set_value : SetType?
      @data.as?(SetType)
    end

    def sorted_set_value : SortedSetType?
      @data.as?(SortedSetType)
    end
  end

  class Database
    @data : Hash(Bytes, Entry)
    @key_versions : Hash(Bytes, Int64)
    @global_version : Int64

    def initialize
      @data = Hash(Bytes, Entry).new(initial_capacity: 1000)
      @key_versions = Hash(Bytes, Int64).new
      @global_version = 0_i64
    end

    # Get the current version of a key (for WATCH)
    def get_key_version(key : Bytes) : Int64
      @key_versions[key]? || 0_i64
    end

    # Mark a key as modified (for WATCH tracking)
    # Call this after any write operation on a key
    def mark_key_modified(key : Bytes) : Nil
      touch_key(key)
    end

    # Increment version when a key is modified (called by write operations)
    private def touch_key(key : Bytes) : Nil
      @global_version += 1
      @key_versions[key] = @global_version
    end

    # Remove version tracking when key is deleted
    private def untouch_key(key : Bytes) : Nil
      @global_version += 1
      @key_versions[key] = @global_version
    end

    def get(key : Bytes) : Bytes?
      entry = get_valid_entry(key)
      return nil unless entry
      entry.string_value
    end

    def get_entry(key : Bytes) : Entry?
      get_valid_entry(key)
    end

    def set(key : Bytes, value : Bytes, ttl : Int64? = nil) : Nil
      validate_key_size(key)
      validate_value_size(value)
      @data[key] = Entry.new(value, ttl)
      touch_key(key)
    end

    def del(key : Bytes) : Bool
      result = @data.delete(key) != nil
      untouch_key(key) if result
      result
    end

    def exists?(key : Bytes) : Bool
      get_valid_entry(key) != nil
    end

    def type_of(key : Bytes) : String
      entry = get_valid_entry(key)
      return "none" unless entry

      case entry.data
      when Bytes         then "string"
      when ListType      then "list"
      when HashType      then "hash"
      when SetType       then "set"
      when SortedSetType then "zset"
      else                    "none"
      end
    end

    def size : Int32
      @data.size
    end

    def clear : Nil
      # Bump versions for all existing keys before clearing
      @data.keys.each { |key| untouch_key(key) }
      @data.clear
      @key_versions.clear
    end

    def keys : Array(Bytes)
      @data.keys
    end

    def append(key : Bytes, value : Bytes) : Int64
      existing = get(key)
      if existing
        new_value = existing + value
        set(key, new_value)
        new_value.size.to_i64
      else
        set(key, value)
        value.size.to_i64
      end
    end

    def strlen(key : Bytes) : Int64
      val = get(key)
      val ? val.size.to_i64 : 0_i64
    end

    def incr(key : Bytes) : Int64
      incrby(key, 1_i64)
    end

    def incrby(key : Bytes, increment : Int64) : Int64
      current = get(key)
      value = NumericConversions.bytes_to_i64(current)

      # Check for overflow before applying
      if increment > 0 && value > Int64::MAX - increment
        raise OverflowError.new
      end
      if increment < 0 && value < Int64::MIN - increment
        raise OverflowError.new
      end

      new_value = value + increment
      set(key, NumericConversions.i64_to_bytes(new_value))
      new_value
    end

    def incrbyfloat(key : Bytes, increment : Float64) : Float64
      current = get(key)
      value = NumericConversions.bytes_to_f64(current)
      new_value = value + increment
      set(key, NumericConversions.f64_to_bytes(new_value))
      new_value
    end

    def decr(key : Bytes) : Int64
      incrby(key, -1_i64)
    end

    def decrby(key : Bytes, decrement : Int64) : Int64
      incrby(key, -decrement)
    end

    def getrange(key : Bytes, start_idx : Int64, end_idx : Int64) : Bytes
      value = get(key)
      return Bytes.empty unless value

      len = value.size.to_i64
      start_idx = normalize_index(start_idx, len)
      end_idx = normalize_index(end_idx, len)

      return Bytes.empty if start_idx > end_idx || start_idx >= len

      end_idx = len - 1 if end_idx >= len
      value[start_idx.to_i..end_idx.to_i]
    end

    def setrange(key : Bytes, offset : Int64, value : Bytes) : Int64
      current = get(key) || Bytes.empty
      offset_i = offset.to_i

      if offset_i > current.size
        padding = Bytes.new(offset_i - current.size, 0_u8)
        current = current + padding
      end

      new_size = Math.max(current.size, offset_i + value.size)
      result = Bytes.new(new_size)

      # Use bulk memory copy for efficiency
      result.copy_from(current) if current.size > 0
      result[offset_i, value.size].copy_from(value) if value.size > 0

      set(key, result)
      result.size.to_i64
    end

    def get_or_create_list(key : Bytes) : ListType
      entry = get_valid_entry(key)
      if entry
        list = entry.list_value
        raise WrongTypeError.new unless list
        return list
      end
      list = ListType.new
      @data[key] = Entry.new(list)
      list
    end

    def get_list(key : Bytes) : ListType?
      entry = get_valid_entry(key)
      return nil unless entry
      entry.list_value
    end

    def get_or_create_hash(key : Bytes) : HashType
      entry = get_valid_entry(key)
      if entry
        hash = entry.hash_value
        raise WrongTypeError.new unless hash
        return hash
      end
      hash = HashType.new
      @data[key] = Entry.new(hash)
      hash
    end

    def get_hash(key : Bytes) : HashType?
      entry = get_valid_entry(key)
      return nil unless entry
      entry.hash_value
    end

    def get_or_create_set(key : Bytes) : SetType
      entry = get_valid_entry(key)
      if entry
        set_val = entry.set_value
        raise WrongTypeError.new unless set_val
        return set_val
      end
      set_val = SetType.new
      @data[key] = Entry.new(set_val)
      set_val
    end

    def get_set(key : Bytes) : SetType?
      entry = get_valid_entry(key)
      return nil unless entry
      entry.set_value
    end

    def get_or_create_sorted_set(key : Bytes) : SortedSetType
      entry = get_valid_entry(key)
      if entry
        zset = entry.sorted_set_value
        raise WrongTypeError.new unless zset
        return zset
      end
      zset = SortedSetType.new
      @data[key] = Entry.new(zset)
      zset
    end

    def get_sorted_set(key : Bytes) : SortedSetType?
      entry = get_valid_entry(key)
      return nil unless entry
      entry.sorted_set_value
    end

    def cleanup_empty(key : Bytes) : Nil
      entry = @data[key]?
      return unless entry

      should_delete = case entry.data
                      when ListType      then entry.data.as(ListType).empty?
                      when HashType      then entry.data.as(HashType).empty?
                      when SetType       then entry.data.as(SetType).empty?
                      when SortedSetType then entry.data.as(SortedSetType).empty?
                      else                    false
                      end

      @data.delete(key) if should_delete
    end

    # TTL Management Methods

    # Set TTL on existing key (returns false if key doesn't exist)
    def expire(key : Bytes, ttl_ms : Int64) : Bool
      entry = get_valid_entry(key)
      return false unless entry
      @data[key] = Entry.new(entry.data, ttl_ms)
      true
    end

    # Remove TTL from key (make persistent)
    def persist(key : Bytes) : Bool
      entry = get_valid_entry(key)
      return false unless entry
      return false unless entry.ttl # No TTL to remove
      @data[key] = Entry.new(entry.data, nil)
      true
    end

    # Get remaining TTL in milliseconds (-2 if no key, -1 if no TTL)
    def pttl(key : Bytes) : Int64
      entry = @data[key]?
      return -2_i64 unless entry

      if ttl = entry.ttl
        if Time.utc.to_unix_ms > ttl
          @data.delete(key)
          return -2_i64
        end
        return ttl - Time.utc.to_unix_ms
      end
      -1_i64
    end

    # Get remaining TTL in seconds
    def ttl(key : Bytes) : Int64
      pttl_val = pttl(key)
      return pttl_val if pttl_val < 0
      (pttl_val / 1000).to_i64
    end

    # Key Management Methods

    # Rename key (overwrites destination)
    def rename(old_key : Bytes, new_key : Bytes) : {Bool, String?}
      entry = get_valid_entry(old_key)
      return {false, "no such key"} unless entry

      @data[new_key] = entry
      @data.delete(old_key)
      touch_key(new_key)
      untouch_key(old_key)
      {true, nil}
    end

    # Rename only if new key doesn't exist
    def renamenx(old_key : Bytes, new_key : Bytes) : {Int64, String?}
      entry = get_valid_entry(old_key)
      return {-1_i64, "no such key"} unless entry

      if exists?(new_key)
        return {0_i64, nil}
      end

      @data[new_key] = entry
      @data.delete(old_key)
      touch_key(new_key)
      untouch_key(old_key)
      {1_i64, nil}
    end

    # SCAN Methods

    # Cursor-based key iteration
    def scan(cursor : Int64, pattern : String? = nil, count : Int64 = 10) : {Int64, Array(Bytes)}
      all_keys = keys
      start_idx = cursor.to_i

      return {0_i64, [] of Bytes} if start_idx >= all_keys.size

      results = [] of Bytes
      idx = start_idx
      regex = pattern ? pattern_to_regex(pattern) : nil

      # Scan up to count keys (or 2x count if filtering)
      scanned = 0
      max_scan = count * 2
      while idx < all_keys.size && results.size < count
        key = all_keys[idx]
        # Check expiration
        if get_valid_entry(key)
          if regex.nil? || regex.matches?(String.new(key))
            results << key
          end
        end
        idx += 1
        scanned += 1
        break if scanned >= max_scan
      end

      next_cursor = idx >= all_keys.size ? 0_i64 : idx.to_i64
      {next_cursor, results}
    end

    private def pattern_to_regex(pattern : String) : Regex
      RegexCache.get(pattern)
    end

    private def get_valid_entry(key : Bytes) : Entry?
      entry = @data[key]?
      return nil unless entry

      if ttl = entry.ttl
        if Time.utc.to_unix_ms > ttl
          @data.delete(key)
          return nil
        end
      end
      entry
    end

    private def normalize_index(idx : Int64, len : Int64) : Int64
      if idx < 0
        idx = len + idx
        idx = 0_i64 if idx < 0
      end
      idx.to_i64
    end

    private def validate_key_size(key : Bytes) : Nil
      raise KeyTooLargeError.new(key.size) if key.size > MAX_KEY_SIZE
    end

    private def validate_value_size(value : Bytes) : Nil
      raise ValueTooLargeError.new(value.size.to_i64) if value.size > MAX_VALUE_SIZE
    end
  end

  class WrongTypeError < Exception
    def initialize
      super("WRONGTYPE Operation against a key holding the wrong kind of value")
    end
  end

  class OverflowError < Exception
    def initialize
      super("increment or decrement would overflow")
    end
  end

  class KeyTooLargeError < Exception
    def initialize(size : Int32)
      super("key size #{size} exceeds maximum allowed #{MAX_KEY_SIZE}")
    end
  end

  class ValueTooLargeError < Exception
    def initialize(size : Int64)
      super("value size #{size} exceeds maximum allowed #{MAX_VALUE_SIZE}")
    end
  end
end
