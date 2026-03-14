require "../storage/database_manager"
require "../server/connection"

module Redis
  struct SetOptions
    property ttl : Int64?
    property? ttl_specified : Bool
    property? nx : Bool
    property? xx : Bool
    property? get_flag : Bool
    property? keepttl : Bool
    property error : String?

    def initialize
      @ttl = nil
      @ttl_specified = false
      @nx = false
      @xx = false
      @get_flag = false
      @keepttl = false
      @error = nil
    end
  end

  class CommandHandler
    VERSION = "0.1.0"

    @db_manager : DatabaseManager
    @start_time : Time

    def initialize(@db_manager : DatabaseManager)
      @start_time = Time.utc
    end

    # Helper to get the current database for a client connection
    private def current_db(client : Connection) : Database
      @db_manager.get(client.current_db_id) || @db_manager.get_or_create(0)
    end

    # Check if the current database is frozen (for write commands)
    private def check_frozen(client : Connection) : Bool
      if @db_manager.frozen?(client.current_db_id)
        client.send_error("database is frozen (read-only)")
        true
      else
        false
      end
    end

    def extract_string(value : RespValue) : String?
      case value
      when String
        value
      when Bytes
        String.new(value)
      else
        nil
      end
    end

    def extract_bytes(value : RespValue) : Bytes?
      case value
      when Bytes
        value
      when String
        value.to_slice
      else
        nil
      end
    end

    def execute(commands : Array(RespValue), client : Connection) : Nil
      return if commands.empty?

      command = commands.first
      command_str = extract_string(command)
      return client.send_error("Invalid command format") unless command_str

      cmd_upper = command_str.upcase
      args = commands[1..]

      # Handle transaction commands immediately (even during transaction)
      if cmd_upper == "MULTI" || cmd_upper == "EXEC" || cmd_upper == "DISCARD" ||
         cmd_upper == "WATCH" || cmd_upper == "UNWATCH"
        dispatch_transaction_commands(cmd_upper, args, client)
        return
      end

      # If in transaction mode, queue command instead of executing
      if client.in_transaction?
        client.queue_command(commands)
        client.send_simple_string("QUEUED")
        return
      end

      dispatch_command(cmd_upper, args, client)
    end

    # Execute a command and return its result (used by EXEC)
    def execute_and_capture(commands : Array(RespValue), client : Connection) : RespValue
      return nil if commands.empty?

      command = commands.first
      command_str = extract_string(command)
      return nil unless command_str

      # Create a capturing wrapper to collect the response
      capturing = CapturingConnection.new(client)
      cmd_upper = command_str.upcase
      args = commands[1..]

      dispatch_command(cmd_upper, args, capturing)
      capturing.captured_response
    end

    private def dispatch_command(cmd : String, args : Array(RespValue), client : Connection) : Nil
      return if dispatch_connection_commands(cmd, args, client)
      return if dispatch_database_commands(cmd, args, client)
      return if dispatch_key_commands(cmd, args, client)
      return if dispatch_scan_commands(cmd, args, client)
      return if dispatch_string_commands(cmd, args, client)
      return if dispatch_list_commands(cmd, args, client)
      return if dispatch_hash_commands(cmd, args, client)
      return if dispatch_set_commands(cmd, args, client)
      return if dispatch_sorted_set_commands(cmd, args, client)
      return if dispatch_server_commands(cmd, args, client)
      client.send_error("unknown command '#{cmd}'")
    rescue ex : KeyTooLargeError
      client.send_error(ex.message)
    rescue ex : ValueTooLargeError
      client.send_error(ex.message)
    end

    private def dispatch_connection_commands(cmd : String, args : Array(RespValue), client : Connection) : Bool
      case cmd
      when "PING"    then handle_ping(args, client)
      when "ECHO"    then handle_echo(args, client)
      when "COMMAND" then handle_command(args, client)
      when "QUIT"    then handle_quit(args, client)
      else                return false
      end
      true
    end

    private def handle_quit(args : Array(RespValue), client : Connection) : Nil
      client.send_ok
      client.close rescue nil
    end

    private def dispatch_database_commands(cmd : String, args : Array(RespValue), client : Connection) : Bool
      case cmd
      when "SELECT"     then handle_select(args, client)
      when "DBCREATE"   then handle_dbcreate(args, client)
      when "DBDROP"     then handle_dbdrop(args, client)
      when "DBLIST"     then handle_dblist(args, client)
      when "DBINFO"     then handle_dbinfo(args, client)
      when "DBSELECT"   then handle_dbselect(args, client)
      when "DBCOPY"     then handle_dbcopy(args, client)
      when "DBCOPYKEYS" then handle_dbcopykeys(args, client)
      when "DBRESET"    then handle_dbreset(args, client)
      when "DBFREEZE"   then handle_dbfreeze(args, client)
      when "DBUNFREEZE" then handle_dbunfreeze(args, client)
      when "FLUSHALL"   then handle_flushall(args, client)
      else                   return false
      end
      true
    end

    private def dispatch_key_commands(cmd : String, args : Array(RespValue), client : Connection) : Bool
      case cmd
      when "EXPIRE"    then handle_expire(args, client)
      when "EXPIREAT"  then handle_expireat(args, client)
      when "PEXPIRE"   then handle_pexpire(args, client)
      when "PEXPIREAT" then handle_pexpireat(args, client)
      when "TTL"       then handle_ttl(args, client)
      when "PTTL"      then handle_pttl(args, client)
      when "PERSIST"   then handle_persist(args, client)
      when "RENAME"    then handle_rename(args, client)
      when "RENAMENX"  then handle_renamenx(args, client)
      else                  return false
      end
      true
    end

    private def dispatch_scan_commands(cmd : String, args : Array(RespValue), client : Connection) : Bool
      case cmd
      when "SCAN"  then handle_scan(args, client)
      when "HSCAN" then handle_hscan(args, client)
      when "SSCAN" then handle_sscan(args, client)
      when "ZSCAN" then handle_zscan(args, client)
      else              return false
      end
      true
    end

    private def dispatch_transaction_commands(cmd : String, args : Array(RespValue), client : Connection) : Nil
      case cmd
      when "MULTI"   then handle_multi(args, client)
      when "EXEC"    then handle_exec(args, client)
      when "DISCARD" then handle_discard(args, client)
      when "WATCH"   then handle_watch(args, client)
      when "UNWATCH" then handle_unwatch(args, client)
      end
    end

    private def dispatch_string_commands(cmd : String, args : Array(RespValue), client : Connection) : Bool
      return true if dispatch_basic_string_commands(cmd, args, client)
      return true if dispatch_numeric_string_commands(cmd, args, client)
      return true if dispatch_multi_string_commands(cmd, args, client)
      false
    end

    private def dispatch_basic_string_commands(cmd : String, args : Array(RespValue), client : Connection) : Bool
      case cmd
      when "SET"      then handle_set(args, client)
      when "GET"      then handle_get(args, client)
      when "DEL"      then handle_del(args, client)
      when "EXISTS"   then handle_exists(args, client)
      when "TYPE"     then handle_type(args, client)
      when "APPEND"   then handle_append(args, client)
      when "STRLEN"   then handle_strlen(args, client)
      when "GETRANGE" then handle_getrange(args, client)
      when "SETRANGE" then handle_setrange(args, client)
      when "GETEX"    then handle_getex(args, client)
      when "GETDEL"   then handle_getdel(args, client)
      when "GETSET"   then handle_getset(args, client)
      else                 return false
      end
      true
    end

    private def dispatch_numeric_string_commands(cmd : String, args : Array(RespValue), client : Connection) : Bool
      case cmd
      when "INCR"        then handle_incr(args, client)
      when "INCRBY"      then handle_incrby(args, client)
      when "INCRBYFLOAT" then handle_incrbyfloat(args, client)
      when "DECR"        then handle_decr(args, client)
      when "DECRBY"      then handle_decrby(args, client)
      else                    return false
      end
      true
    end

    private def dispatch_multi_string_commands(cmd : String, args : Array(RespValue), client : Connection) : Bool
      case cmd
      when "MGET"   then handle_mget(args, client)
      when "MSET"   then handle_mset(args, client)
      when "MSETNX" then handle_msetnx(args, client)
      when "SETNX"  then handle_setnx(args, client)
      when "SETEX"  then handle_setex(args, client)
      when "PSETEX" then handle_psetex(args, client)
      else               return false
      end
      true
    end

    private def dispatch_server_commands(cmd : String, args : Array(RespValue), client : Connection) : Bool
      case cmd
      when "CONFIG"  then handle_config(args, client)
      when "DBSIZE"  then handle_dbsize(args, client)
      when "FLUSHDB" then handle_flushdb(args, client)
      when "KEYS"    then handle_keys(args, client)
      when "INFO"    then handle_info(args, client)
      when "TIME"    then handle_time(args, client)
      else                return false
      end
      true
    end

    private def dispatch_list_commands(cmd : String, args : Array(RespValue), client : Connection) : Bool
      case cmd
      when "LPUSH"   then handle_lpush(args, client)
      when "RPUSH"   then handle_rpush(args, client)
      when "LPUSHX"  then handle_lpushx(args, client)
      when "RPUSHX"  then handle_rpushx(args, client)
      when "LPOP"    then handle_lpop(args, client)
      when "RPOP"    then handle_rpop(args, client)
      when "LLEN"    then handle_llen(args, client)
      when "LINDEX"  then handle_lindex(args, client)
      when "LSET"    then handle_lset(args, client)
      when "LRANGE"  then handle_lrange(args, client)
      when "LINSERT" then handle_linsert(args, client)
      when "LPOS"    then handle_lpos(args, client)
      when "LREM"    then handle_lrem(args, client)
      when "LTRIM"   then handle_ltrim(args, client)
      when "LMOVE"   then handle_lmove(args, client)
      else                return false
      end
      true
    end

    private def dispatch_hash_commands(cmd : String, args : Array(RespValue), client : Connection) : Bool
      case cmd
      when "HSET"         then handle_hset(args, client)
      when "HGET"         then handle_hget(args, client)
      when "HMSET"        then handle_hmset(args, client)
      when "HMGET"        then handle_hmget(args, client)
      when "HDEL"         then handle_hdel(args, client)
      when "HEXISTS"      then handle_hexists(args, client)
      when "HLEN"         then handle_hlen(args, client)
      when "HKEYS"        then handle_hkeys(args, client)
      when "HVALS"        then handle_hvals(args, client)
      when "HGETALL"      then handle_hgetall(args, client)
      when "HINCRBY"      then handle_hincrby(args, client)
      when "HINCRBYFLOAT" then handle_hincrbyfloat(args, client)
      when "HSETNX"       then handle_hsetnx(args, client)
      when "HSTRLEN"      then handle_hstrlen(args, client)
      else                     return false
      end
      true
    end

    private def dispatch_set_commands(cmd : String, args : Array(RespValue), client : Connection) : Bool
      case cmd
      when "SADD"        then handle_sadd(args, client)
      when "SREM"        then handle_srem(args, client)
      when "SISMEMBER"   then handle_sismember(args, client)
      when "SMISMEMBER"  then handle_smismember(args, client)
      when "SMEMBERS"    then handle_smembers(args, client)
      when "SCARD"       then handle_scard(args, client)
      when "SPOP"        then handle_spop(args, client)
      when "SRANDMEMBER" then handle_srandmember(args, client)
      when "SUNION"      then handle_sunion(args, client)
      when "SINTER"      then handle_sinter(args, client)
      when "SDIFF"       then handle_sdiff(args, client)
      when "SUNIONSTORE" then handle_sunionstore(args, client)
      when "SINTERSTORE" then handle_sinterstore(args, client)
      when "SDIFFSTORE"  then handle_sdiffstore(args, client)
      when "SMOVE"       then handle_smove(args, client)
      else                    return false
      end
      true
    end

    private def dispatch_sorted_set_commands(cmd : String, args : Array(RespValue), client : Connection) : Bool
      case cmd
      when "ZADD"          then handle_zadd(args, client)
      when "ZREM"          then handle_zrem(args, client)
      when "ZSCORE"        then handle_zscore(args, client)
      when "ZRANK"         then handle_zrank(args, client)
      when "ZREVRANK"      then handle_zrevrank(args, client)
      when "ZCARD"         then handle_zcard(args, client)
      when "ZCOUNT"        then handle_zcount(args, client)
      when "ZRANGE"        then handle_zrange(args, client)
      when "ZREVRANGE"     then handle_zrevrange(args, client)
      when "ZRANGEBYSCORE" then handle_zrangebyscore(args, client)
      when "ZINCRBY"       then handle_zincrby(args, client)
      when "ZPOPMIN"       then handle_zpopmin(args, client)
      when "ZPOPMAX"       then handle_zpopmax(args, client)
      when "ZMSCORE"       then handle_zmscore(args, client)
      when "ZRANGESTORE"   then handle_zrangestore(args, client)
      when "ZRANDMEMBER"   then handle_zrandmember(args, client)
      else                      return false
      end
      true
    end

    private def handle_ping(args : Array(RespValue), client : Connection) : Nil
      if args.empty?
        client.send_simple_string("PONG")
      else
        msg = extract_bytes(args.first)
        client.send_bulk_string(msg)
      end
    end

    private def handle_echo(args : Array(RespValue), client : Connection) : Nil
      if args.empty?
        client.send_error("wrong number of arguments for 'echo' command")
        return
      end
      msg = extract_bytes(args.first)
      client.send_bulk_string(msg)
    end

    private def handle_command(args : Array(RespValue), client : Connection) : Nil
      client.send_simple_string("OK")
    end

    private def handle_set(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)

      if args.size < 2
        client.send_error("wrong number of arguments for 'set' command")
        return
      end

      key = extract_bytes(args[0])
      value = extract_bytes(args[1])
      return client.send_error("Invalid key or value") unless key && value

      opts = parse_set_options(args[2..])
      if err = opts.error
        client.send_error(err)
        return
      end

      if opts.nx? && opts.xx?
        client.send_error("XX and NX options at the same time are not compatible")
        return
      end

      if opts.keepttl? && opts.ttl_specified?
        client.send_error("syntax error")
        return
      end

      old_value = current_db(client).get(key) if opts.get_flag?
      exists = current_db(client).exists?(key)

      if opts.nx? && exists
        send_set_response(client, opts.get_flag?, old_value, false)
        return
      end

      if opts.xx? && !exists
        send_set_response(client, opts.get_flag?, nil, false)
        return
      end

      ttl = opts.ttl
      if opts.keepttl? && exists
        entry = current_db(client).get_entry(key)
        ttl = entry.ttl if entry
      end

      current_db(client).set(key, value, ttl)
      send_set_response(client, opts.get_flag?, old_value, true)
    end

    private def parse_set_options(args : Array(RespValue)) : SetOptions
      opts = SetOptions.new
      idx = 0

      while idx < args.size
        opt = extract_string(args[idx])
        break unless opt

        case opt.upcase
        when "EX"
          result = parse_ttl_option(args, idx, :seconds)
          return set_error(opts, result[:error]) if result[:error]
          opts.ttl = result[:ttl]
          opts.ttl_specified = true
          idx = result[:next_idx]
        when "PX"
          result = parse_ttl_option(args, idx, :milliseconds)
          return set_error(opts, result[:error]) if result[:error]
          opts.ttl = result[:ttl]
          opts.ttl_specified = true
          idx = result[:next_idx]
        when "EXAT"
          result = parse_ttl_option(args, idx, :unix_seconds)
          return set_error(opts, result[:error]) if result[:error]
          opts.ttl = result[:ttl]
          opts.ttl_specified = true
          idx = result[:next_idx]
        when "PXAT"
          result = parse_ttl_option(args, idx, :unix_milliseconds)
          return set_error(opts, result[:error]) if result[:error]
          opts.ttl = result[:ttl]
          opts.ttl_specified = true
          idx = result[:next_idx]
        when "NX"
          opts.nx = true
          idx += 1
        when "XX"
          opts.xx = true
          idx += 1
        when "GET"
          opts.get_flag = true
          idx += 1
        when "KEEPTTL"
          opts.keepttl = true
          idx += 1
        else
          return set_error(opts, "syntax error")
        end
      end

      opts
    end

    private def parse_ttl_option(args : Array(RespValue), idx : Int32, mode : Symbol) : NamedTuple(ttl: Int64?, error: String?, next_idx: Int32)
      next_idx = idx + 1
      if next_idx >= args.size
        return {ttl: nil, error: "syntax error", next_idx: next_idx}
      end

      val_str = extract_string(args[next_idx])
      unless val_str
        return {ttl: nil, error: "syntax error", next_idx: next_idx}
      end

      val = val_str.to_i64?
      unless val
        return {ttl: nil, error: "value is not an integer or out of range", next_idx: next_idx}
      end

      ttl = case mode
            when :seconds
              return {ttl: nil, error: "invalid expire time in 'set' command", next_idx: next_idx} if val <= 0
              Time.utc.to_unix_ms + (val * 1000)
            when :milliseconds
              return {ttl: nil, error: "invalid expire time in 'set' command", next_idx: next_idx} if val <= 0
              Time.utc.to_unix_ms + val
            when :unix_seconds      then val * 1000
            when :unix_milliseconds then val
            else                         val
            end

      {ttl: ttl, error: nil, next_idx: next_idx + 1}
    end

    private def set_error(opts : SetOptions, msg : String?) : SetOptions
      opts.error = msg
      opts
    end

    private def send_set_response(client : Connection, get_flag : Bool, old_value : Bytes?, success : Bool) : Nil
      if get_flag
        client.send_bulk_string(old_value)
      elsif success
        client.send_ok
      else
        client.send_nil
      end
    end

    private def parse_count_argument(arg : RespValue?, client : Connection) : Int64?
      value = extract_string(arg)
      unless value
        client.send_error("value is not an integer or out of range")
        return nil
      end

      count = value.to_i64?
      unless count
        client.send_error("value is not an integer or out of range")
        return nil
      end

      count
    end

    private def parse_positive_count_argument(arg : RespValue?, client : Connection) : Int32?
      count = parse_count_argument(arg, client)
      return nil unless count

      if count <= 0 || count > Int32::MAX
        client.send_error("value is out of range, must be positive")
        return nil
      end

      count.to_i32
    end

    private def parse_scan_cursor(value : String, client : Connection) : Int64?
      cursor = value.to_i64?
      unless cursor && cursor >= 0
        client.send_error("invalid cursor")
        return nil
      end

      cursor
    end

    private def parse_scan_options(args : Array(RespValue), start_idx : Int32, client : Connection) : NamedTuple(pattern: String?, count: Int64, error: String?)
      pattern : String? = nil
      count = 10_i64
      idx = start_idx

      while idx < args.size
        opt = extract_string(args[idx])
        return {pattern: nil, count: count, error: "syntax error"} unless opt

        case opt.upcase
        when "MATCH"
          idx += 1
          return {pattern: nil, count: count, error: "syntax error"} if idx >= args.size

          pattern = extract_string(args[idx])
          return {pattern: nil, count: count, error: "syntax error"} unless pattern
        when "COUNT"
          idx += 1
          return {pattern: nil, count: count, error: "syntax error"} if idx >= args.size

          parsed_count = parse_count_argument(args[idx], client)
          return {pattern: nil, count: count, error: "handled"} unless parsed_count
          return {pattern: nil, count: count, error: "syntax error"} if parsed_count <= 0

          count = parsed_count
        else
          return {pattern: nil, count: count, error: "syntax error"}
        end

        idx += 1
      end

      {pattern: pattern, count: count, error: nil}
    end

    private def handle_get(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'get' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      value = current_db(client).get(key)
      client.send_bulk_string(value)
    end

    private def handle_del(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)

      if args.empty?
        client.send_error("wrong number of arguments for 'del' command")
        return
      end

      count = 0_i64
      args.each do |arg|
        key = extract_bytes(arg)
        next unless key
        count += 1 if current_db(client).del(key)
      end
      client.send_integer(count)
    end

    private def handle_exists(args : Array(RespValue), client : Connection) : Nil
      if args.empty?
        client.send_error("wrong number of arguments for 'exists' command")
        return
      end

      count = 0_i64
      args.each do |arg|
        key = extract_bytes(arg)
        next unless key
        count += 1 if current_db(client).exists?(key)
      end
      client.send_integer(count)
    end

    private def handle_type(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'type' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      type_str = current_db(client).type_of(key)
      client.send_simple_string(type_str)
    end

    private def handle_append(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)

      if args.size != 2
        client.send_error("wrong number of arguments for 'append' command")
        return
      end

      key = extract_bytes(args[0])
      value = extract_bytes(args[1])
      return client.send_error("Invalid key or value") unless key && value

      new_len = current_db(client).append(key, value)
      client.send_integer(new_len)
    end

    private def handle_strlen(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'strlen' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      len = current_db(client).strlen(key)
      client.send_integer(len)
    end

    private def handle_incr(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)

      if args.size != 1
        client.send_error("wrong number of arguments for 'incr' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        result = current_db(client).incr(key)
        client.send_integer(result)
      rescue ex : ArgumentError
        client.send_error("value is not an integer or out of range")
      rescue ex : OverflowError
        client.send_error("increment or decrement would overflow")
      end
    end

    private def handle_incrby(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)

      if args.size != 2
        client.send_error("wrong number of arguments for 'incrby' command")
        return
      end

      key = extract_bytes(args[0])
      increment_str = extract_string(args[1])
      return client.send_error("Invalid arguments") unless key && increment_str

      increment = increment_str.to_i64?
      unless increment
        client.send_error("value is not an integer or out of range")
        return
      end

      begin
        result = current_db(client).incrby(key, increment)
        client.send_integer(result)
      rescue ex : ArgumentError
        client.send_error("value is not an integer or out of range")
      rescue ex : OverflowError
        client.send_error("increment or decrement would overflow")
      end
    end

    private def handle_incrbyfloat(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)

      if args.size != 2
        client.send_error("wrong number of arguments for 'incrbyfloat' command")
        return
      end

      key = extract_bytes(args[0])
      increment_str = extract_string(args[1])
      return client.send_error("Invalid arguments") unless key && increment_str

      increment = increment_str.to_f64?
      unless increment
        client.send_error("value is not a valid float")
        return
      end

      begin
        result = current_db(client).incrbyfloat(key, increment)
        client.send_bulk_string(result.to_s.to_slice)
      rescue ex : ArgumentError
        client.send_error("value is not a valid float")
      end
    end

    private def handle_decr(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)

      if args.size != 1
        client.send_error("wrong number of arguments for 'decr' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        result = current_db(client).decr(key)
        client.send_integer(result)
      rescue ex : ArgumentError
        client.send_error("value is not an integer or out of range")
      rescue ex : OverflowError
        client.send_error("increment or decrement would overflow")
      end
    end

    private def handle_decrby(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)

      if args.size != 2
        client.send_error("wrong number of arguments for 'decrby' command")
        return
      end

      key = extract_bytes(args[0])
      decrement_str = extract_string(args[1])
      return client.send_error("Invalid arguments") unless key && decrement_str

      decrement = decrement_str.to_i64?
      unless decrement
        client.send_error("value is not an integer or out of range")
        return
      end

      begin
        result = current_db(client).decrby(key, decrement)
        client.send_integer(result)
      rescue ex : ArgumentError
        client.send_error("value is not an integer or out of range")
      rescue ex : OverflowError
        client.send_error("increment or decrement would overflow")
      end
    end

    private def handle_getrange(args : Array(RespValue), client : Connection) : Nil
      if args.size != 3
        client.send_error("wrong number of arguments for 'getrange' command")
        return
      end

      key = extract_bytes(args[0])
      start_str = extract_string(args[1])
      end_str = extract_string(args[2])
      return client.send_error("Invalid arguments") unless key && start_str && end_str

      start_idx = start_str.to_i64?
      end_idx = end_str.to_i64?
      unless start_idx && end_idx
        client.send_error("value is not an integer or out of range")
        return
      end

      result = current_db(client).getrange(key, start_idx, end_idx)
      client.send_bulk_string(result)
    end

    private def handle_setrange(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)

      if args.size != 3
        client.send_error("wrong number of arguments for 'setrange' command")
        return
      end

      key = extract_bytes(args[0])
      offset_str = extract_string(args[1])
      value = extract_bytes(args[2])
      return client.send_error("Invalid arguments") unless key && offset_str && value

      offset = offset_str.to_i64?
      unless offset
        client.send_error("value is not an integer or out of range")
        return
      end

      if offset < 0
        client.send_error("offset is out of range")
        return
      end

      result = current_db(client).setrange(key, offset, value)
      client.send_integer(result)
    end

    private def handle_mget(args : Array(RespValue), client : Connection) : Nil
      if args.empty?
        client.send_error("wrong number of arguments for 'mget' command")
        return
      end

      results = Array(RespValue).new(args.size)
      args.each do |arg|
        key = extract_bytes(arg)
        if key
          value = current_db(client).get(key)
          results << value.as(RespValue)
        else
          results << nil.as(RespValue)
        end
      end
      client.send_array(results)
    end

    private def handle_mset(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)

      if args.size < 2 || args.size.odd?
        client.send_error("wrong number of arguments for 'mset' command")
        return
      end

      pairs = [] of {Bytes, Bytes}
      i = 0
      while i < args.size
        key = extract_bytes(args[i])
        value = extract_bytes(args[i + 1])
        unless key && value
          client.send_error("Invalid key or value")
          return
        end
        pairs << {key, value}
        i += 2
      end

      pairs.each do |pair|
        current_db(client).set(pair[0], pair[1])
      end
      client.send_ok
    end

    private def handle_msetnx(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)

      if args.size < 2 || args.size.odd?
        client.send_error("wrong number of arguments for 'msetnx' command")
        return
      end

      pairs = [] of {Bytes, Bytes}
      idx = 0
      while idx < args.size
        k = extract_bytes(args[idx])
        v = extract_bytes(args[idx + 1])
        unless k && v
          client.send_error("Invalid key or value")
          return
        end
        pairs << {k, v}
        idx += 2
      end

      pairs.each do |pair|
        if current_db(client).exists?(pair[0])
          client.send_integer(0_i64)
          return
        end
      end

      pairs.each do |pair|
        current_db(client).set(pair[0], pair[1])
      end
      client.send_integer(1_i64)
    end

    private def handle_setnx(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)

      if args.size != 2
        client.send_error("wrong number of arguments for 'setnx' command")
        return
      end

      key = extract_bytes(args[0])
      value = extract_bytes(args[1])
      return client.send_error("Invalid key or value") unless key && value

      if current_db(client).exists?(key)
        client.send_integer(0_i64)
      else
        current_db(client).set(key, value)
        client.send_integer(1_i64)
      end
    end

    private def handle_setex(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)

      if args.size != 3
        client.send_error("wrong number of arguments for 'setex' command")
        return
      end

      key = extract_bytes(args[0])
      seconds_str = extract_string(args[1])
      value = extract_bytes(args[2])
      return client.send_error("Invalid arguments") unless key && seconds_str && value

      seconds = seconds_str.to_i64?
      unless seconds
        client.send_error("value is not an integer or out of range")
        return
      end

      if seconds <= 0
        client.send_error("invalid expire time in 'setex' command")
        return
      end

      ttl = Time.utc.to_unix_ms + (seconds * 1000)
      current_db(client).set(key, value, ttl)
      client.send_ok
    end

    private def handle_psetex(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 3
        client.send_error("wrong number of arguments for 'psetex' command")
        return
      end

      key = extract_bytes(args[0])
      ms_str = extract_string(args[1])
      value = extract_bytes(args[2])
      return client.send_error("Invalid arguments") unless key && ms_str && value

      ms = ms_str.to_i64?
      unless ms
        client.send_error("value is not an integer or out of range")
        return
      end

      if ms <= 0
        client.send_error("invalid expire time in 'psetex' command")
        return
      end

      ttl = Time.utc.to_unix_ms + ms
      current_db(client).set(key, value, ttl)
      client.send_ok
    end

    private def handle_getex(args : Array(RespValue), client : Connection) : Nil
      if args.empty?
        client.send_error("wrong number of arguments for 'getex' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      value = current_db(client).get(key)

      if args.size > 1
        opt = extract_string(args[1])
        return client.send_error("syntax error") unless opt
        return if check_frozen(client)

        case opt.upcase
        when "EX"
          return client.send_error("syntax error") unless args.size == 3

          seconds_str = extract_string(args[2])
          return client.send_error("syntax error") unless seconds_str

          seconds = seconds_str.to_i64?
          unless seconds
            client.send_error("value is not an integer or out of range")
            return
          end
          if seconds <= 0
            client.send_error("invalid expire time in 'getex' command")
            return
          end

          if value
            ttl = Time.utc.to_unix_ms + (seconds * 1000)
            current_db(client).set(key, value, ttl)
          end
        when "PX"
          return client.send_error("syntax error") unless args.size == 3

          ms_str = extract_string(args[2])
          return client.send_error("syntax error") unless ms_str

          ms = ms_str.to_i64?
          unless ms
            client.send_error("value is not an integer or out of range")
            return
          end
          if ms <= 0
            client.send_error("invalid expire time in 'getex' command")
            return
          end

          if value
            ttl = Time.utc.to_unix_ms + ms
            current_db(client).set(key, value, ttl)
          end
        when "EXAT"
          return client.send_error("syntax error") unless args.size == 3

          timestamp_str = extract_string(args[2])
          return client.send_error("syntax error") unless timestamp_str

          timestamp = timestamp_str.to_i64?
          unless timestamp
            client.send_error("value is not an integer or out of range")
            return
          end

          if value
            current_db(client).set(key, value, timestamp * 1000)
          end
        when "PXAT"
          return client.send_error("syntax error") unless args.size == 3

          timestamp_str = extract_string(args[2])
          return client.send_error("syntax error") unless timestamp_str

          timestamp = timestamp_str.to_i64?
          unless timestamp
            client.send_error("value is not an integer or out of range")
            return
          end

          if value
            current_db(client).set(key, value, timestamp)
          end
        when "PERSIST"
          return client.send_error("syntax error") unless args.size == 2

          if value
            current_db(client).set(key, value, nil)
          end
        else
          client.send_error("syntax error")
          return
        end
      end

      client.send_bulk_string(value)
    end

    private def handle_getdel(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 1
        client.send_error("wrong number of arguments for 'getdel' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      value = current_db(client).get(key)
      current_db(client).del(key) if value
      client.send_bulk_string(value)
    end

    private def handle_getset(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 2
        client.send_error("wrong number of arguments for 'getset' command")
        return
      end

      key = extract_bytes(args[0])
      new_value = extract_bytes(args[1])
      return client.send_error("Invalid key or value") unless key && new_value

      old_value = current_db(client).get(key)
      current_db(client).set(key, new_value)
      client.send_bulk_string(old_value)
    end

    private def handle_dbsize(args : Array(RespValue), client : Connection) : Nil
      client.send_integer(current_db(client).size.to_i64)
    end

    private def handle_config(args : Array(RespValue), client : Connection) : Nil
      if args.size != 2
        client.send_error("wrong number of arguments for 'config' command")
        return
      end

      subcommand = extract_string(args[0])
      pattern = extract_string(args[1])
      return client.send_error("syntax error") unless subcommand && pattern

      unless subcommand.upcase == "GET"
        client.send_error("CONFIG subcommand must be GET")
        return
      end

      matcher = GlobMatcher.compile(pattern)
      result = [] of Bytes

      {"save" => Bytes.empty, "appendonly" => "no".to_slice}.each do |name, value|
        next unless matcher.matches?(name.to_slice)

        result << name.to_slice
        result << value
      end

      client.send_bytes_array(result)
    end

    private def handle_flushdb(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      current_db(client).clear
      client.send_ok
    end

    private def handle_keys(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'keys' command")
        return
      end

      pattern = extract_string(args[0])
      return client.send_error("Invalid pattern") unless pattern

      keys = if pattern == "*"
               current_db(client).keys
             else
               current_db(client).keys_matching(pattern)
             end
      client.send_bytes_array(keys)
    end

    private def handle_info(args : Array(RespValue), client : Connection) : Nil
      section = if args.empty?
                  nil
                else
                  arg_str = extract_string(args[0])
                  arg_str ? arg_str.downcase : nil
                end

      info = String.build do |io|
        # Server section
        if section.nil? || section == "server" || section == "all"
          io << "# Server\r\n"
          io << "facet_version:#{VERSION}\r\n"
          io << "redis_version:7.0.0\r\n" # Compatibility version
          io << "os:#{Crystal::DESCRIPTION}\r\n"
          io << "arch_bits:64\r\n"
          io << "process_id:#{Process.pid}\r\n"
          io << "uptime_in_seconds:#{(Time.utc - @start_time).total_seconds.to_i}\r\n"
          io << "uptime_in_days:#{(Time.utc - @start_time).total_days.to_i}\r\n"
          io << "\r\n"
        end

        # Keyspace section
        if section.nil? || section == "keyspace" || section == "all"
          io << "# Keyspace\r\n"
          @db_manager.list_databases.each do |db_info|
            if db_info.key_count > 0
              io << "db#{db_info.id}:keys=#{db_info.key_count}\r\n"
            end
          end
          io << "\r\n"
        end

        # Memory section (basic)
        if section.nil? || section == "memory" || section == "all"
          io << "# Memory\r\n"
          io << "used_memory:#{GC.stats.heap_size}\r\n"
          io << "used_memory_human:#{format_bytes(GC.stats.heap_size)}\r\n"
          io << "\r\n"
        end

        # Stats section
        if section.nil? || section == "stats" || section == "all"
          io << "# Stats\r\n"
          total_keys = @db_manager.list_databases.sum(&.key_count)
          io << "total_keys:#{total_keys}\r\n"
          io << "total_databases:#{@db_manager.database_count}\r\n"
          io << "\r\n"
        end
      end

      client.send_bulk_string(info.to_slice)
    end

    private def format_bytes(bytes : UInt64) : String
      if bytes < 1024
        "#{bytes}B"
      elsif bytes < 1024 * 1024
        "#{(bytes / 1024.0).round(2)}K"
      elsif bytes < 1024 * 1024 * 1024
        "#{(bytes / (1024.0 * 1024)).round(2)}M"
      else
        "#{(bytes / (1024.0 * 1024 * 1024)).round(2)}G"
      end
    end

    private def handle_time(args : Array(RespValue), client : Connection) : Nil
      now = Time.utc
      seconds = now.to_unix.to_s
      microseconds = (now.nanosecond // 1000).to_s
      client.send_array([seconds.to_slice.as(RespValue), microseconds.to_slice.as(RespValue)])
    end

    private def handle_lpush(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 2
        client.send_error("wrong number of arguments for 'lpush' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      values = args[1..].compact_map { |arg| extract_bytes(arg) }

      begin
        list = current_db(client).get_or_create_list(key)
        result = list.lpush(values)
        current_db(client).mark_key_modified(key) unless values.empty?
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_rpush(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 2
        client.send_error("wrong number of arguments for 'rpush' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      values = args[1..].compact_map { |arg| extract_bytes(arg) }

      begin
        list = current_db(client).get_or_create_list(key)
        result = list.rpush(values)
        current_db(client).mark_key_modified(key) unless values.empty?
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_lpushx(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 2
        client.send_error("wrong number of arguments for 'lpushx' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        list = current_db(client).get_list(key)
        unless list
          client.send_integer(0_i64)
          return
        end

        values = args[1..].compact_map { |arg| extract_bytes(arg) }
        result = list.lpush(values)
        current_db(client).mark_key_modified(key) unless values.empty?
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_rpushx(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 2
        client.send_error("wrong number of arguments for 'rpushx' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        list = current_db(client).get_list(key)
        unless list
          client.send_integer(0_i64)
          return
        end

        values = args[1..].compact_map { |arg| extract_bytes(arg) }
        result = list.rpush(values)
        current_db(client).mark_key_modified(key) unless values.empty?
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_lpop(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.empty? || args.size > 2
        client.send_error("wrong number of arguments for 'lpop' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      count = 1
      if args.size > 1
        parsed_count = parse_positive_count_argument(args[1]?, client)
        return unless parsed_count
        count = parsed_count
      end

      begin
        list = current_db(client).get_list(key)
        unless list
          client.send_nil
          return
        end

        result = list.lpop(count)
        current_db(client).mark_key_modified(key) unless result.empty?
        current_db(client).cleanup_empty(key)

        if args.size > 1
          arr = result.map { |val| val.as(RespValue) }
          client.send_array(arr)
        elsif result.empty?
          client.send_nil
        else
          client.send_bulk_string(result.first)
        end
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_rpop(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.empty? || args.size > 2
        client.send_error("wrong number of arguments for 'rpop' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      count = 1
      if args.size > 1
        parsed_count = parse_positive_count_argument(args[1]?, client)
        return unless parsed_count
        count = parsed_count
      end

      begin
        list = current_db(client).get_list(key)
        unless list
          client.send_nil
          return
        end

        result = list.rpop(count)
        current_db(client).mark_key_modified(key) unless result.empty?
        current_db(client).cleanup_empty(key)

        if args.size > 1
          arr = result.map { |val| val.as(RespValue) }
          client.send_array(arr)
        elsif result.empty?
          client.send_nil
        else
          client.send_bulk_string(result.first)
        end
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_llen(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'llen' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        list = current_db(client).get_list(key)
        result = list ? list.llen : 0_i64
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_lindex(args : Array(RespValue), client : Connection) : Nil
      if args.size != 2
        client.send_error("wrong number of arguments for 'lindex' command")
        return
      end

      key = extract_bytes(args[0])
      index_str = extract_string(args[1])
      return client.send_error("Invalid arguments") unless key && index_str

      index = index_str.to_i64?
      unless index
        client.send_error("value is not an integer or out of range")
        return
      end

      begin
        list = current_db(client).get_list(key)
        unless list
          client.send_nil
          return
        end

        result = list.lindex(index)
        client.send_bulk_string(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_lset(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 3
        client.send_error("wrong number of arguments for 'lset' command")
        return
      end

      key = extract_bytes(args[0])
      index_str = extract_string(args[1])
      value = extract_bytes(args[2])
      return client.send_error("Invalid arguments") unless key && index_str && value

      index = index_str.to_i64?
      unless index
        client.send_error("value is not an integer or out of range")
        return
      end

      begin
        list = current_db(client).get_list(key)
        unless list
          client.send_error("no such key")
          return
        end

        if list.lset(index, value)
          current_db(client).mark_key_modified(key)
          client.send_ok
        else
          client.send_error("index out of range")
        end
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_lrange(args : Array(RespValue), client : Connection) : Nil
      if args.size != 3
        client.send_error("wrong number of arguments for 'lrange' command")
        return
      end

      key = extract_bytes(args[0])
      start_str = extract_string(args[1])
      end_str = extract_string(args[2])
      return client.send_error("Invalid arguments") unless key && start_str && end_str

      start_idx = start_str.to_i64?
      end_idx = end_str.to_i64?
      unless start_idx && end_idx
        client.send_error("value is not an integer or out of range")
        return
      end

      begin
        list = current_db(client).get_list(key)
        unless list
          client.send_bytes_array([] of Bytes)
          return
        end

        result = list.lrange(start_idx, end_idx)
        client.send_bytes_array(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_linsert(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 4
        client.send_error("wrong number of arguments for 'linsert' command")
        return
      end

      key = extract_bytes(args[0])
      position = extract_string(args[1])
      pivot = extract_bytes(args[2])
      value = extract_bytes(args[3])
      return client.send_error("Invalid arguments") unless key && position && pivot && value

      before = case position.upcase
               when "BEFORE" then true
               when "AFTER"  then false
               else
                 client.send_error("syntax error")
                 return
               end

      begin
        list = current_db(client).get_list(key)
        unless list
          client.send_integer(0_i64)
          return
        end

        result = list.linsert(pivot, value, before)
        current_db(client).mark_key_modified(key) if result > 0
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_lpos(args : Array(RespValue), client : Connection) : Nil
      if args.size < 2
        client.send_error("wrong number of arguments for 'lpos' command")
        return
      end

      key = extract_bytes(args[0])
      element = extract_bytes(args[1])
      return client.send_error("Invalid arguments") unless key && element

      rank = 1_i64
      count = 1_i64
      maxlen = 0_i64
      return_count = false

      idx = 2
      while idx < args.size
        opt = extract_string(args[idx])
        unless opt
          client.send_error("syntax error")
          return
        end

        case opt.upcase
        when "RANK"
          idx += 1
          if idx >= args.size
            client.send_error("syntax error")
            return
          end

          rank_str = extract_string(args[idx])
          unless rank_str
            client.send_error("syntax error")
            return
          end

          parsed_rank = rank_str.to_i64?
          unless parsed_rank
            client.send_error("value is not an integer or out of range")
            return
          end
          if parsed_rank == 0
            client.send_error("RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list")
            return
          end

          rank = parsed_rank
        when "COUNT"
          idx += 1
          if idx >= args.size
            client.send_error("syntax error")
            return
          end

          count_str = extract_string(args[idx])
          unless count_str
            client.send_error("syntax error")
            return
          end

          parsed_count = count_str.to_i64?
          unless parsed_count
            client.send_error("value is not an integer or out of range")
            return
          end
          if parsed_count < 0
            client.send_error("COUNT can't be negative")
            return
          end

          count = parsed_count
          return_count = true
        when "MAXLEN"
          idx += 1
          if idx >= args.size
            client.send_error("syntax error")
            return
          end

          maxlen_str = extract_string(args[idx])
          unless maxlen_str
            client.send_error("syntax error")
            return
          end

          parsed_maxlen = maxlen_str.to_i64?
          unless parsed_maxlen
            client.send_error("value is not an integer or out of range")
            return
          end
          if parsed_maxlen < 0
            client.send_error("MAXLEN can't be negative")
            return
          end

          maxlen = parsed_maxlen
        else
          client.send_error("syntax error")
          return
        end
        idx += 1
      end

      begin
        list = current_db(client).get_list(key)
        unless list
          if return_count
            client.send_array([] of RespValue)
          else
            client.send_nil
          end
          return
        end

        result = list.lpos(element, rank, count, maxlen)

        if return_count
          arr = result.map { |val| val.as(RespValue) }
          client.send_array(arr)
        elsif result.empty?
          client.send_nil
        else
          client.send_integer(result.first)
        end
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_lrem(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 3
        client.send_error("wrong number of arguments for 'lrem' command")
        return
      end

      key = extract_bytes(args[0])
      count_str = extract_string(args[1])
      element = extract_bytes(args[2])
      return client.send_error("Invalid arguments") unless key && count_str && element

      count = count_str.to_i64?
      unless count
        client.send_error("value is not an integer or out of range")
        return
      end

      begin
        list = current_db(client).get_list(key)
        unless list
          client.send_integer(0_i64)
          return
        end

        result = list.lrem(count, element)
        current_db(client).mark_key_modified(key) if result > 0
        current_db(client).cleanup_empty(key)
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_ltrim(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 3
        client.send_error("wrong number of arguments for 'ltrim' command")
        return
      end

      key = extract_bytes(args[0])
      start_str = extract_string(args[1])
      end_str = extract_string(args[2])
      return client.send_error("Invalid arguments") unless key && start_str && end_str

      start_idx = start_str.to_i64?
      end_idx = end_str.to_i64?
      unless start_idx && end_idx
        client.send_error("value is not an integer or out of range")
        return
      end

      begin
        list = current_db(client).get_list(key)
        unless list
          client.send_ok
          return
        end

        list.ltrim(start_idx, end_idx)
        current_db(client).mark_key_modified(key)
        current_db(client).cleanup_empty(key)
        client.send_ok
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_lmove(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 4
        client.send_error("wrong number of arguments for 'lmove' command")
        return
      end

      src_key = extract_bytes(args[0])
      dst_key = extract_bytes(args[1])
      wherefrom_str = extract_string(args[2])
      whereto_str = extract_string(args[3])
      return client.send_error("Invalid arguments") unless src_key && dst_key && wherefrom_str && whereto_str

      wherefrom = case wherefrom_str.upcase
                  when "LEFT"  then :left
                  when "RIGHT" then :right
                  else
                    client.send_error("syntax error")
                    return
                  end

      whereto = case whereto_str.upcase
                when "LEFT"  then :left
                when "RIGHT" then :right
                else
                  client.send_error("syntax error")
                  return
                end

      begin
        src_list = current_db(client).get_list(src_key)
        unless src_list
          client.send_nil
          return
        end

        dst_list = current_db(client).get_or_create_list(dst_key)
        result = src_list.lmove(dst_list, wherefrom, whereto)
        if result
          current_db(client).mark_key_modified(src_key)
          current_db(client).mark_key_modified(dst_key) unless src_key == dst_key
        end
        current_db(client).cleanup_empty(src_key)

        client.send_bulk_string(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_hset(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 3 || (args.size - 1).odd?
        client.send_error("wrong number of arguments for 'hset' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        hash = current_db(client).get_or_create_hash(key)
        added = 0_i64

        idx = 1
        while idx < args.size
          field = extract_bytes(args[idx])
          value = extract_bytes(args[idx + 1])
          if field && value
            added += 1 if hash.hset(field, value)
          end
          idx += 2
        end

        current_db(client).mark_key_modified(key)
        client.send_integer(added)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_hget(args : Array(RespValue), client : Connection) : Nil
      if args.size != 2
        client.send_error("wrong number of arguments for 'hget' command")
        return
      end

      key = extract_bytes(args[0])
      field = extract_bytes(args[1])
      return client.send_error("Invalid arguments") unless key && field

      begin
        hash = current_db(client).get_hash(key)
        unless hash
          client.send_nil
          return
        end

        result = hash.hget(field)
        client.send_bulk_string(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_hmset(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 3 || (args.size - 1).odd?
        client.send_error("wrong number of arguments for 'hmset' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        hash = current_db(client).get_or_create_hash(key)

        idx = 1
        while idx < args.size
          field = extract_bytes(args[idx])
          value = extract_bytes(args[idx + 1])
          hash.hset(field, value) if field && value
          idx += 2
        end

        current_db(client).mark_key_modified(key)
        client.send_ok
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_hmget(args : Array(RespValue), client : Connection) : Nil
      if args.size < 2
        client.send_error("wrong number of arguments for 'hmget' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        hash = current_db(client).get_hash(key)
        results = Array(RespValue).new(args.size - 1)

        args[1..].each do |arg|
          field = extract_bytes(arg)
          if field && hash
            val = hash.hget(field)
            results << val.as(RespValue)
          else
            results << nil.as(RespValue)
          end
        end

        client.send_array(results)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_hdel(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 2
        client.send_error("wrong number of arguments for 'hdel' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        hash = current_db(client).get_hash(key)
        unless hash
          client.send_integer(0_i64)
          return
        end

        fields = args[1..].compact_map { |arg| extract_bytes(arg) }
        result = hash.hdel(fields)
        current_db(client).mark_key_modified(key) if result > 0
        current_db(client).cleanup_empty(key)
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_hexists(args : Array(RespValue), client : Connection) : Nil
      if args.size != 2
        client.send_error("wrong number of arguments for 'hexists' command")
        return
      end

      key = extract_bytes(args[0])
      field = extract_bytes(args[1])
      return client.send_error("Invalid arguments") unless key && field

      begin
        hash = current_db(client).get_hash(key)
        result = hash && hash.hexists?(field) ? 1_i64 : 0_i64
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_hlen(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'hlen' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        hash = current_db(client).get_hash(key)
        result = hash ? hash.hlen : 0_i64
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_hkeys(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'hkeys' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        hash = current_db(client).get_hash(key)
        unless hash
          client.send_array([] of RespValue)
          return
        end

        result = hash.hkeys.map { |k| k.as(RespValue) }
        client.send_array(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_hvals(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'hvals' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        hash = current_db(client).get_hash(key)
        unless hash
          client.send_array([] of RespValue)
          return
        end

        result = hash.hvals.map { |v| v.as(RespValue) }
        client.send_array(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_hgetall(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'hgetall' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        hash = current_db(client).get_hash(key)
        unless hash
          client.send_array([] of RespValue)
          return
        end

        result = hash.hgetall.map { |v| v.as(RespValue) }
        client.send_array(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_hincrby(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 3
        client.send_error("wrong number of arguments for 'hincrby' command")
        return
      end

      key = extract_bytes(args[0])
      field = extract_bytes(args[1])
      incr_str = extract_string(args[2])
      return client.send_error("Invalid arguments") unless key && field && incr_str

      increment = incr_str.to_i64?
      unless increment
        client.send_error("value is not an integer or out of range")
        return
      end

      begin
        hash = current_db(client).get_or_create_hash(key)
        result = hash.hincrby(field, increment)
        current_db(client).mark_key_modified(key)
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      rescue
        client.send_error("hash value is not an integer")
      end
    end

    private def handle_hincrbyfloat(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 3
        client.send_error("wrong number of arguments for 'hincrbyfloat' command")
        return
      end

      key = extract_bytes(args[0])
      field = extract_bytes(args[1])
      incr_str = extract_string(args[2])
      return client.send_error("Invalid arguments") unless key && field && incr_str

      increment = incr_str.to_f64?
      unless increment
        client.send_error("value is not a valid float")
        return
      end

      begin
        hash = current_db(client).get_or_create_hash(key)
        result = hash.hincrbyfloat(field, increment)
        current_db(client).mark_key_modified(key)
        client.send_bulk_string(result.to_s.to_slice)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      rescue
        client.send_error("hash value is not a float")
      end
    end

    private def handle_hsetnx(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 3
        client.send_error("wrong number of arguments for 'hsetnx' command")
        return
      end

      key = extract_bytes(args[0])
      field = extract_bytes(args[1])
      value = extract_bytes(args[2])
      return client.send_error("Invalid arguments") unless key && field && value

      begin
        hash = current_db(client).get_or_create_hash(key)
        result = hash.hsetnx(field, value) ? 1_i64 : 0_i64
        current_db(client).mark_key_modified(key) if result == 1_i64
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_hstrlen(args : Array(RespValue), client : Connection) : Nil
      if args.size != 2
        client.send_error("wrong number of arguments for 'hstrlen' command")
        return
      end

      key = extract_bytes(args[0])
      field = extract_bytes(args[1])
      return client.send_error("Invalid arguments") unless key && field

      begin
        hash = current_db(client).get_hash(key)
        result = hash ? hash.hstrlen(field) : 0_i64
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_sadd(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 2
        client.send_error("wrong number of arguments for 'sadd' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      members = args[1..].compact_map { |arg| extract_bytes(arg) }

      begin
        set = current_db(client).get_or_create_set(key)
        result = set.sadd(members)
        current_db(client).mark_key_modified(key) if result > 0
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_srem(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 2
        client.send_error("wrong number of arguments for 'srem' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        set = current_db(client).get_set(key)
        unless set
          client.send_integer(0_i64)
          return
        end

        members = args[1..].compact_map { |arg| extract_bytes(arg) }
        result = set.srem(members)
        current_db(client).mark_key_modified(key) if result > 0
        current_db(client).cleanup_empty(key)
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_sismember(args : Array(RespValue), client : Connection) : Nil
      if args.size != 2
        client.send_error("wrong number of arguments for 'sismember' command")
        return
      end

      key = extract_bytes(args[0])
      member = extract_bytes(args[1])
      return client.send_error("Invalid arguments") unless key && member

      begin
        set = current_db(client).get_set(key)
        result = set && set.sismember?(member) ? 1_i64 : 0_i64
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_smismember(args : Array(RespValue), client : Connection) : Nil
      if args.size < 2
        client.send_error("wrong number of arguments for 'smismember' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        set = current_db(client).get_set(key)
        members = args[1..].compact_map { |arg| extract_bytes(arg) }

        results = if set
                    set.smismember(members).map { |is_member| (is_member ? 1_i64 : 0_i64).as(RespValue) }
                  else
                    members.map { |_| 0_i64.as(RespValue) }
                  end

        client.send_array(results)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_smembers(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'smembers' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        set = current_db(client).get_set(key)
        unless set
          client.send_array([] of RespValue)
          return
        end

        result = set.smembers.map { |member| member.as(RespValue) }
        client.send_array(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_scard(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'scard' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        set = current_db(client).get_set(key)
        result = set ? set.scard : 0_i64
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_spop(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.empty? || args.size > 2
        client.send_error("wrong number of arguments for 'spop' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      count = 1
      if args.size > 1
        parsed_count = parse_positive_count_argument(args[1]?, client)
        return unless parsed_count
        count = parsed_count
      end

      begin
        set = current_db(client).get_set(key)
        unless set
          if args.size > 1
            client.send_array([] of RespValue)
          else
            client.send_nil
          end
          return
        end

        result = set.spop(count)
        current_db(client).mark_key_modified(key) unless result.empty?
        current_db(client).cleanup_empty(key)

        if args.size > 1
          arr = result.map { |member| member.as(RespValue) }
          client.send_array(arr)
        elsif result.empty?
          client.send_nil
        else
          client.send_bulk_string(result.first)
        end
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_srandmember(args : Array(RespValue), client : Connection) : Nil
      if args.empty? || args.size > 2
        client.send_error("wrong number of arguments for 'srandmember' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      count = 1_i64
      return_array = false
      if args.size > 1
        parsed_count = parse_count_argument(args[1]?, client)
        return unless parsed_count
        count = parsed_count
        return_array = true
      end

      begin
        set = current_db(client).get_set(key)
        unless set
          if return_array
            client.send_array([] of RespValue)
          else
            client.send_nil
          end
          return
        end

        result = set.srandmember(count)

        if return_array
          arr = result.map { |member| member.as(RespValue) }
          client.send_array(arr)
        elsif result.empty?
          client.send_nil
        else
          client.send_bulk_string(result.first)
        end
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_sunion(args : Array(RespValue), client : Connection) : Nil
      if args.empty?
        client.send_error("wrong number of arguments for 'sunion' command")
        return
      end

      begin
        result_set = SetType.new

        args.each do |arg|
          k = extract_bytes(arg)
          next unless k
          set = current_db(client).get_set(k)
          if set
            set.smembers.each { |member| result_set.sadd([member]) }
          end
        end

        result = result_set.smembers.map { |member| member.as(RespValue) }
        client.send_array(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_sinter(args : Array(RespValue), client : Connection) : Nil
      if args.empty?
        client.send_error("wrong number of arguments for 'sinter' command")
        return
      end

      begin
        first_key = extract_bytes(args[0])
        return client.send_error("Invalid key") unless first_key

        first_set = current_db(client).get_set(first_key)
        unless first_set
          client.send_array([] of RespValue)
          return
        end

        result_members = first_set.smembers

        args[1..].each do |arg|
          k = extract_bytes(arg)
          next unless k
          set = current_db(client).get_set(k)
          unless set
            result_members = [] of Bytes
            break
          end
          result_members = result_members.select { |member| set.sismember?(member) }
        end

        result = result_members.map { |member| member.as(RespValue) }
        client.send_array(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_sdiff(args : Array(RespValue), client : Connection) : Nil
      if args.empty?
        client.send_error("wrong number of arguments for 'sdiff' command")
        return
      end

      begin
        first_key = extract_bytes(args[0])
        return client.send_error("Invalid key") unless first_key

        first_set = current_db(client).get_set(first_key)
        unless first_set
          client.send_array([] of RespValue)
          return
        end

        result_members = first_set.smembers

        args[1..].each do |arg|
          k = extract_bytes(arg)
          next unless k
          set = current_db(client).get_set(k)
          next unless set
          result_members = result_members.reject { |member| set.sismember?(member) }
        end

        result = result_members.map { |member| member.as(RespValue) }
        client.send_array(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_sunionstore(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 2
        client.send_error("wrong number of arguments for 'sunionstore' command")
        return
      end

      dest_key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless dest_key

      begin
        result_members = [] of Bytes

        args[1..].each do |arg|
          k = extract_bytes(arg)
          next unless k
          set = current_db(client).get_set(k)
          if set
            set.smembers.each { |member| result_members << member unless result_members.includes?(member) }
          end
        end

        current_db(client).del(dest_key)
        if result_members.empty?
          client.send_integer(0_i64)
          return
        end

        result_set = current_db(client).get_or_create_set(dest_key)
        result_set.sadd(result_members)
        client.send_integer(result_set.scard)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_sinterstore(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 2
        client.send_error("wrong number of arguments for 'sinterstore' command")
        return
      end

      dest_key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless dest_key

      begin
        first_key = extract_bytes(args[1])
        return client.send_error("Invalid key") unless first_key

        first_set = current_db(client).get_set(first_key)
        unless first_set
          current_db(client).del(dest_key)
          client.send_integer(0_i64)
          return
        end

        result_members = first_set.smembers

        args[2..].each do |arg|
          k = extract_bytes(arg)
          next unless k
          set = current_db(client).get_set(k)
          unless set
            result_members = [] of Bytes
            break
          end
          result_members = result_members.select { |member| set.sismember?(member) }
        end

        current_db(client).del(dest_key)
        if result_members.empty?
          client.send_integer(0_i64)
          return
        end

        dest_set = current_db(client).get_or_create_set(dest_key)
        dest_set.sadd(result_members)
        client.send_integer(dest_set.scard)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_sdiffstore(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 2
        client.send_error("wrong number of arguments for 'sdiffstore' command")
        return
      end

      dest_key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless dest_key

      begin
        first_key = extract_bytes(args[1])
        return client.send_error("Invalid key") unless first_key

        first_set = current_db(client).get_set(first_key)
        unless first_set
          current_db(client).del(dest_key)
          client.send_integer(0_i64)
          return
        end

        result_members = first_set.smembers

        args[2..].each do |arg|
          k = extract_bytes(arg)
          next unless k
          set = current_db(client).get_set(k)
          next unless set
          result_members = result_members.reject { |member| set.sismember?(member) }
        end

        current_db(client).del(dest_key)
        if result_members.empty?
          client.send_integer(0_i64)
          return
        end

        dest_set = current_db(client).get_or_create_set(dest_key)
        dest_set.sadd(result_members)
        client.send_integer(dest_set.scard)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_smove(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 3
        client.send_error("wrong number of arguments for 'smove' command")
        return
      end

      src_key = extract_bytes(args[0])
      dest_key = extract_bytes(args[1])
      member = extract_bytes(args[2])
      return client.send_error("Invalid arguments") unless src_key && dest_key && member

      begin
        src_set = current_db(client).get_set(src_key)
        unless src_set
          client.send_integer(0_i64)
          return
        end

        dest_set = current_db(client).get_or_create_set(dest_key)
        result = src_set.smove(dest_set, member) ? 1_i64 : 0_i64
        if result == 1_i64
          current_db(client).mark_key_modified(src_key)
          current_db(client).mark_key_modified(dest_key) unless src_key == dest_key
        end
        current_db(client).cleanup_empty(src_key)
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zadd(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 3
        client.send_error("wrong number of arguments for 'zadd' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      nx = false
      xx = false
      gt = false
      lt = false
      ch = false
      incr = false
      idx = 1

      while idx < args.size
        opt = extract_string(args[idx])
        break unless opt
        case opt.upcase
        when "NX"   then nx = true
        when "XX"   then xx = true
        when "GT"   then gt = true
        when "LT"   then lt = true
        when "CH"   then ch = true
        when "INCR" then incr = true
        else             break
        end
        idx += 1
      end

      remaining = args.size - idx
      if remaining < 2 || remaining.odd?
        client.send_error("syntax error")
        return
      end

      if incr && remaining > 2
        client.send_error("INCR option supports a single increment-element pair")
        return
      end

      begin
        zset = current_db(client).get_or_create_sorted_set(key)
        added = 0_i64

        while idx < args.size
          score_str = extract_string(args[idx])
          member = extract_bytes(args[idx + 1])

          unless score_str && member
            idx += 2
            next
          end

          score = score_str.to_f64?
          unless score
            client.send_error("value is not a valid float")
            return
          end

          if incr
            handle_zadd_incr(key, zset, member, score, nx, xx, client)
            return
          end

          result, changed = zset.zadd(member, score, nx, xx, gt, lt, ch)
          current_db(client).mark_key_modified(key) if changed
          added += result
          idx += 2
        end

        client.send_integer(added)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zadd_incr(key : Bytes, zset : SortedSetType, member : Bytes, score : Float64, nx : Bool, xx : Bool, client : Connection) : Nil
      member_exists = zset.zscore(member) != nil
      if nx && member_exists
        client.send_nil
        return
      end
      if xx && !member_exists
        client.send_nil
        return
      end
      result = zset.zincrby(member, score)
      current_db(client).mark_key_modified(key)
      client.send_bulk_string(result.to_s.to_slice)
    end

    private def handle_zrem(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 2
        client.send_error("wrong number of arguments for 'zrem' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        zset = current_db(client).get_sorted_set(key)
        unless zset
          client.send_integer(0_i64)
          return
        end

        members = args[1..].compact_map { |arg| extract_bytes(arg) }
        result = zset.zrem(members)
        current_db(client).mark_key_modified(key) if result > 0
        current_db(client).cleanup_empty(key)
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zscore(args : Array(RespValue), client : Connection) : Nil
      if args.size != 2
        client.send_error("wrong number of arguments for 'zscore' command")
        return
      end

      key = extract_bytes(args[0])
      member = extract_bytes(args[1])
      return client.send_error("Invalid arguments") unless key && member

      begin
        zset = current_db(client).get_sorted_set(key)
        unless zset
          client.send_nil
          return
        end

        score = zset.zscore(member)
        if score
          client.send_bulk_string(score.to_s.to_slice)
        else
          client.send_nil
        end
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zrank(args : Array(RespValue), client : Connection) : Nil
      if args.size != 2
        client.send_error("wrong number of arguments for 'zrank' command")
        return
      end

      key = extract_bytes(args[0])
      member = extract_bytes(args[1])
      return client.send_error("Invalid arguments") unless key && member

      begin
        zset = current_db(client).get_sorted_set(key)
        unless zset
          client.send_nil
          return
        end

        rank = zset.zrank(member)
        if rank
          client.send_integer(rank)
        else
          client.send_nil
        end
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zrevrank(args : Array(RespValue), client : Connection) : Nil
      if args.size != 2
        client.send_error("wrong number of arguments for 'zrevrank' command")
        return
      end

      key = extract_bytes(args[0])
      member = extract_bytes(args[1])
      return client.send_error("Invalid arguments") unless key && member

      begin
        zset = current_db(client).get_sorted_set(key)
        unless zset
          client.send_nil
          return
        end

        rank = zset.zrevrank(member)
        if rank
          client.send_integer(rank)
        else
          client.send_nil
        end
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zcard(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'zcard' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        zset = current_db(client).get_sorted_set(key)
        result = zset ? zset.zcard : 0_i64
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zcount(args : Array(RespValue), client : Connection) : Nil
      if args.size != 3
        client.send_error("wrong number of arguments for 'zcount' command")
        return
      end

      key = extract_bytes(args[0])
      min_str = extract_string(args[1])
      max_str = extract_string(args[2])
      return client.send_error("Invalid arguments") unless key && min_str && max_str

      min_bound = parse_score_bound(min_str)
      max_bound = parse_score_bound(max_str)
      if min_bound[:error] || max_bound[:error]
        client.send_error("min or max is not a float")
        return
      end

      min_val = min_bound[:value] || 0.0
      min_exclusive = min_bound[:exclusive]
      max_val = max_bound[:value] || 0.0
      max_exclusive = max_bound[:exclusive]

      begin
        zset = current_db(client).get_sorted_set(key)
        result = zset ? zset.zcount(min_val, max_val, min_exclusive, max_exclusive) : 0_i64
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zrange(args : Array(RespValue), client : Connection) : Nil
      if args.size < 3
        client.send_error("wrong number of arguments for 'zrange' command")
        return
      end

      key = extract_bytes(args[0])
      start_str = extract_string(args[1])
      end_str = extract_string(args[2])
      return client.send_error("Invalid arguments") unless key && start_str && end_str

      start_idx = start_str.to_i64?
      end_idx = end_str.to_i64?
      unless start_idx && end_idx
        client.send_error("value is not an integer or out of range")
        return
      end

      withscores = false
      if args.size > 3
        opt = extract_string(args[3])
        unless opt && opt.upcase == "WITHSCORES" && args.size == 4
          client.send_error("syntax error")
          return
        end
        withscores = true
      end

      begin
        zset = current_db(client).get_sorted_set(key)
        unless zset
          client.send_array([] of RespValue)
          return
        end

        result = zset.zrange(start_idx, end_idx, false, withscores)
        arr = result.map do |val|
          case val
          when Bytes   then val.as(RespValue)
          when Float64 then val.to_s.to_slice.as(RespValue)
          else              nil.as(RespValue)
          end
        end
        client.send_array(arr)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zrevrange(args : Array(RespValue), client : Connection) : Nil
      if args.size < 3
        client.send_error("wrong number of arguments for 'zrevrange' command")
        return
      end

      key = extract_bytes(args[0])
      start_str = extract_string(args[1])
      end_str = extract_string(args[2])
      return client.send_error("Invalid arguments") unless key && start_str && end_str

      start_idx = start_str.to_i64?
      end_idx = end_str.to_i64?
      unless start_idx && end_idx
        client.send_error("value is not an integer or out of range")
        return
      end

      withscores = false
      if args.size > 3
        opt = extract_string(args[3])
        unless opt && opt.upcase == "WITHSCORES" && args.size == 4
          client.send_error("syntax error")
          return
        end
        withscores = true
      end

      begin
        zset = current_db(client).get_sorted_set(key)
        unless zset
          client.send_array([] of RespValue)
          return
        end

        result = zset.zrange(start_idx, end_idx, true, withscores)
        arr = result.map do |val|
          case val
          when Bytes   then val.as(RespValue)
          when Float64 then val.to_s.to_slice.as(RespValue)
          else              nil.as(RespValue)
          end
        end
        client.send_array(arr)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zrangebyscore(args : Array(RespValue), client : Connection) : Nil
      if args.size < 3
        client.send_error("wrong number of arguments for 'zrangebyscore' command")
        return
      end

      key = extract_bytes(args[0])
      min_str = extract_string(args[1])
      max_str = extract_string(args[2])
      return client.send_error("Invalid arguments") unless key && min_str && max_str

      min_bound = parse_score_bound(min_str)
      max_bound = parse_score_bound(max_str)
      if min_bound[:error] || max_bound[:error]
        client.send_error("min or max is not a float")
        return
      end

      min_val = min_bound[:value] || 0.0
      min_exclusive = min_bound[:exclusive]
      max_val = max_bound[:value] || 0.0
      max_exclusive = max_bound[:exclusive]

      withscores = false
      offset = 0_i64
      count = -1_i64

      idx = 3
      while idx < args.size
        opt = extract_string(args[idx])
        unless opt
          client.send_error("syntax error")
          return
        end

        case opt.upcase
        when "WITHSCORES"
          withscores = true
          idx += 1
        when "LIMIT"
          if idx + 2 >= args.size
            client.send_error("syntax error")
            return
          end

          offset_str = extract_string(args[idx + 1])
          count_str = extract_string(args[idx + 2])
          unless offset_str && count_str
            client.send_error("syntax error")
            return
          end

          parsed_offset = offset_str.to_i64?
          parsed_count = count_str.to_i64?
          unless parsed_offset && parsed_count
            client.send_error("value is not an integer or out of range")
            return
          end

          offset = parsed_offset
          count = parsed_count
          idx += 3
        else
          client.send_error("syntax error")
          return
        end
      end

      begin
        zset = current_db(client).get_sorted_set(key)
        unless zset
          client.send_array([] of RespValue)
          return
        end

        result = zset.zrangebyscore(min_val, max_val, withscores, offset, count, min_exclusive, max_exclusive)
        arr = result.map do |val|
          case val
          when Bytes   then val.as(RespValue)
          when Float64 then val.to_s.to_slice.as(RespValue)
          else              nil.as(RespValue)
          end
        end
        client.send_array(arr)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zincrby(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 3
        client.send_error("wrong number of arguments for 'zincrby' command")
        return
      end

      key = extract_bytes(args[0])
      incr_str = extract_string(args[1])
      member = extract_bytes(args[2])
      return client.send_error("Invalid arguments") unless key && incr_str && member

      increment = incr_str.to_f64?
      unless increment
        client.send_error("value is not a valid float")
        return
      end

      begin
        zset = current_db(client).get_or_create_sorted_set(key)
        result = zset.zincrby(member, increment)
        current_db(client).mark_key_modified(key)
        client.send_bulk_string(result.to_s.to_slice)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zpopmin(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.empty? || args.size > 2
        client.send_error("wrong number of arguments for 'zpopmin' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      count = 1
      if args.size > 1
        parsed_count = parse_positive_count_argument(args[1]?, client)
        return unless parsed_count
        count = parsed_count
      end

      begin
        zset = current_db(client).get_sorted_set(key)
        unless zset
          client.send_array([] of RespValue)
          return
        end

        result = zset.zpopmin(count)
        current_db(client).mark_key_modified(key) unless result.empty?
        current_db(client).cleanup_empty(key)

        arr = [] of RespValue
        result.each do |entry|
          arr << entry[0].as(RespValue)
          arr << entry[1].to_s.to_slice.as(RespValue)
        end
        client.send_array(arr)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zpopmax(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.empty? || args.size > 2
        client.send_error("wrong number of arguments for 'zpopmax' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      count = 1
      if args.size > 1
        parsed_count = parse_positive_count_argument(args[1]?, client)
        return unless parsed_count
        count = parsed_count
      end

      begin
        zset = current_db(client).get_sorted_set(key)
        unless zset
          client.send_array([] of RespValue)
          return
        end

        result = zset.zpopmax(count)
        current_db(client).mark_key_modified(key) unless result.empty?
        current_db(client).cleanup_empty(key)

        arr = [] of RespValue
        result.each do |entry|
          arr << entry[0].as(RespValue)
          arr << entry[1].to_s.to_slice.as(RespValue)
        end
        client.send_array(arr)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zrangestore(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size < 4
        client.send_error("wrong number of arguments for 'zrangestore' command")
        return
      end

      dest_key = extract_bytes(args[0])
      src_key = extract_bytes(args[1])
      start_str = extract_string(args[2])
      end_str = extract_string(args[3])
      return client.send_error("Invalid arguments") unless dest_key && src_key && start_str && end_str

      start_idx = start_str.to_i64?
      end_idx = end_str.to_i64?
      unless start_idx && end_idx
        client.send_error("value is not an integer or out of range")
        return
      end

      byscore = false
      reverse = false

      idx = 4
      while idx < args.size
        opt = extract_string(args[idx])
        break unless opt
        case opt.upcase
        when "REV"
          reverse = true
          idx += 1
        when "BYSCORE"
          byscore = true
          idx += 1
        when "WITHSCORES"
          client.send_error("syntax error: WITHSCORES not allowed in ZRANGESTORE")
          return
        else
          idx += 1
        end
      end

      if byscore
        client.send_error("BYSCORE option not supported yet")
        return
      end

      begin
        src_zset = current_db(client).get_sorted_set(src_key)
        unless src_zset
          current_db(client).del(dest_key)
          client.send_integer(0_i64)
          return
        end

        current_db(client).del(dest_key)
        dest_zset = current_db(client).get_or_create_sorted_set(dest_key)
        result = dest_zset.zrangestore(src_zset, start_idx, end_idx, reverse)
        client.send_integer(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zrandmember(args : Array(RespValue), client : Connection) : Nil
      if args.empty?
        client.send_error("wrong number of arguments for 'zrandmember' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      count : Int64? = nil
      withscores = false
      count_requested = false

      if args.size > 1
        count = parse_count_argument(args[1]?, client)
        return unless count
        count_requested = true
      end

      if args.size > 2
        opt = extract_string(args[2])
        unless opt && opt.upcase == "WITHSCORES"
          client.send_error("syntax error")
          return
        end
        withscores = true
      end

      if args.size > 3
        client.send_error("syntax error")
        return
      end

      if withscores && count.nil?
        client.send_error("value is not an integer or out of range")
        return
      end

      begin
        zset = current_db(client).get_sorted_set(key)
        unless zset
          if withscores || count_requested
            client.send_array([] of RespValue)
          else
            client.send_nil
          end
          return
        end

        result = zset.zrandmember(count || 1_i64, withscores)

        if withscores
          arr = result.map do |val|
            case val
            when Bytes   then val.as(RespValue)
            when Float64 then val.to_s.to_slice.as(RespValue)
            else              nil.as(RespValue)
            end
          end
          client.send_array(arr)
        elsif count_requested
          arr = result.map { |val| val.as(RespValue) }
          client.send_array(arr)
        elsif result.empty?
          client.send_nil
        else
          client.send_bulk_string(result.first.as(Bytes))
        end
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zmscore(args : Array(RespValue), client : Connection) : Nil
      if args.size < 2
        client.send_error("wrong number of arguments for 'zmscore' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      begin
        zset = current_db(client).get_sorted_set(key)
        members = args[1..].compact_map { |arg| extract_bytes(arg) }

        results = if zset
                    zset.zmscore(members).map do |score|
                      score ? score.to_s.to_slice.as(RespValue) : nil.as(RespValue)
                    end
                  else
                    members.map { |_| nil.as(RespValue) }
                  end

        client.send_array(results)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    # Returns {value, is_exclusive}
    private def parse_score_bound(str : String) : NamedTuple(value: Float64?, exclusive: Bool, error: String?)
      case str
      when "-inf" then {value: -Float64::INFINITY, exclusive: false, error: nil}
      when "+inf" then {value: Float64::INFINITY, exclusive: false, error: nil}
      when "inf"  then {value: Float64::INFINITY, exclusive: false, error: nil}
      else
        if str.starts_with?("(")
          val = str[1..].to_f64?
          return {value: nil, exclusive: true, error: "min or max is not a float"} unless val
          {value: val, exclusive: true, error: nil}
        else
          val = str.to_f64?
          return {value: nil, exclusive: false, error: "min or max is not a float"} unless val
          {value: val, exclusive: false, error: nil}
        end
      end
    end

    # ==================== Database Management Commands ====================

    private def handle_select(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'select' command")
        return
      end

      index_str = extract_string(args[0])
      return client.send_error("Invalid database index") unless index_str

      index = index_str.to_i?
      return client.send_error("invalid DB index") unless index
      return client.send_error("invalid DB index") if index < 0

      # Check if database exists (for numeric indices, it should be auto-created)
      unless @db_manager.database_exists?(index)
        if index < DatabaseManager::DEFAULT_DB_COUNT
          @db_manager.get_or_create(index)
        else
          return client.send_error("invalid DB index")
        end
      end

      client.current_db_id = index
      client.send_ok
    end

    private def handle_dbcreate(args : Array(RespValue), client : Connection) : Nil
      if args.empty?
        client.send_error("wrong number of arguments for 'dbcreate' command")
        return
      end

      name = extract_string(args[0])
      return client.send_error("Invalid database name") unless name

      # Check for IF NOT EXISTS option
      if_not_exists = false
      if args.size >= 4
        arg1 = extract_string(args[1])
        arg2 = extract_string(args[2])
        arg3 = extract_string(args[3])
        if arg1 && arg2 && arg3
          if_not_exists = arg1.upcase == "IF" && arg2.upcase == "NOT" && arg3.upcase == "EXISTS"
        end
      end

      success, error = @db_manager.create_database(name, if_not_exists)
      if success || (if_not_exists && error.nil?)
        client.send_ok
      else
        client.send_error(error || "database creation failed")
      end
    end

    private def handle_dbdrop(args : Array(RespValue), client : Connection) : Nil
      if args.empty?
        client.send_error("wrong number of arguments for 'dbdrop' command")
        return
      end

      name = extract_string(args[0])
      return client.send_error("Invalid database name") unless name

      # Check for IF EXISTS option
      if_exists = false
      if args.size >= 3
        arg1 = extract_string(args[1])
        arg2 = extract_string(args[2])
        if arg1 && arg2
          if_exists = arg1.upcase == "IF" && arg2.upcase == "EXISTS"
        end
      end

      # Parse database identifier
      db_id : DatabaseId = name.to_i? || name

      success, error = @db_manager.drop_database(db_id, if_exists)
      if success
        client.send_ok
      else
        if error
          client.send_error(error)
        else
          client.send_ok # IF EXISTS and doesn't exist
        end
      end
    end

    private def handle_dblist(args : Array(RespValue), client : Connection) : Nil
      pattern = if args.size > 0
                  extract_string(args[0])
                else
                  nil
                end

      databases = @db_manager.list_databases(pattern)

      result = Array(RespValue).new(databases.size)
      databases.each do |info|
        entry = Array(RespValue).new(4) # Each DBINFO entry has exactly 4 fields
        entry << info.id.to_s.to_slice.as(RespValue)
        entry << info.key_count.as(RespValue)
        entry << info.created_at.to_unix.as(RespValue)
        entry << (info.frozen? ? 1_i64 : 0_i64).as(RespValue)
        result << entry.as(RespValue)
      end

      client.send_array(result)
    end

    private def handle_dbinfo(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'dbinfo' command")
        return
      end

      name = extract_string(args[0])
      return client.send_error("Invalid database name") unless name

      # Parse database identifier
      db_id : DatabaseId = name.to_i? || name

      info = @db_manager.database_info(db_id)
      unless info
        client.send_error("database '#{name}' does not exist")
        return
      end

      result = Array(RespValue).new(8) # DBINFO returns exactly 8 fields
      result << "id".to_slice.as(RespValue)
      result << info.id.to_s.to_slice.as(RespValue)
      result << "keys".to_slice.as(RespValue)
      result << info.key_count.as(RespValue)
      result << "created_at".to_slice.as(RespValue)
      result << info.created_at.to_unix.as(RespValue)
      result << "frozen".to_slice.as(RespValue)
      result << (info.frozen? ? 1_i64 : 0_i64).as(RespValue)

      client.send_array(result)
    end

    private def handle_dbselect(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'dbselect' command")
        return
      end

      name = extract_string(args[0])
      return client.send_error("Invalid database name") unless name

      # Parse database identifier (try numeric first, then string)
      db_id : DatabaseId = name.to_i? || name

      unless @db_manager.database_exists?(db_id)
        client.send_error("database '#{name}' does not exist")
        return
      end

      client.current_db_id = db_id
      client.send_ok
    end

    private def handle_dbcopy(args : Array(RespValue), client : Connection) : Nil
      if args.size < 2
        client.send_error("wrong number of arguments for 'dbcopy' command")
        return
      end

      source_name = extract_string(args[0])
      dest_name = extract_string(args[1])
      return client.send_error("Invalid source database name") unless source_name
      return client.send_error("Invalid destination database name") unless dest_name

      # Check for REPLACE option
      replace = false
      if args.size > 2
        opt = extract_string(args[2])
        replace = opt ? opt.upcase == "REPLACE" : false
      end

      # Parse database identifiers
      source_id : DatabaseId = source_name.to_i? || source_name
      dest_id : DatabaseId = dest_name.to_i? || dest_name

      success, error = @db_manager.copy_database(source_id, dest_id, replace)
      if success
        client.send_ok
      else
        client.send_error(error || "database copy failed")
      end
    end

    private def handle_dbcopykeys(args : Array(RespValue), client : Connection) : Nil
      if args.size < 3
        client.send_error("wrong number of arguments for 'dbcopykeys' command")
        return
      end

      source_name = extract_string(args[0])
      dest_name = extract_string(args[1])
      pattern = extract_string(args[2])
      return client.send_error("Invalid source database name") unless source_name
      return client.send_error("Invalid destination database name") unless dest_name
      return client.send_error("Invalid pattern") unless pattern

      # Parse database identifiers
      source_id : DatabaseId = source_name.to_i? || source_name
      dest_id : DatabaseId = dest_name.to_i? || dest_name

      count, error = @db_manager.copy_keys(source_id, dest_id, pattern)
      if error
        client.send_error(error)
      else
        client.send_integer(count)
      end
    end

    private def handle_dbreset(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'dbreset' command")
        return
      end

      name = extract_string(args[0])
      return client.send_error("Invalid database name") unless name

      # Parse database identifier
      db_id : DatabaseId = name.to_i? || name

      success, error = @db_manager.reset_database(db_id)
      if success
        client.send_ok
      else
        client.send_error(error || "database reset failed")
      end
    end

    private def handle_dbfreeze(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'dbfreeze' command")
        return
      end

      name = extract_string(args[0])
      return client.send_error("Invalid database name") unless name

      # Parse database identifier
      db_id : DatabaseId = name.to_i? || name

      success, error = @db_manager.freeze_database(db_id)
      if success
        client.send_ok
      else
        client.send_error(error || "database freeze failed")
      end
    end

    private def handle_dbunfreeze(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'dbunfreeze' command")
        return
      end

      name = extract_string(args[0])
      return client.send_error("Invalid database name") unless name

      # Parse database identifier
      db_id : DatabaseId = name.to_i? || name

      success, error = @db_manager.unfreeze_database(db_id)
      if success
        client.send_ok
      else
        client.send_error(error || "database unfreeze failed")
      end
    end

    private def handle_flushall(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      @db_manager.flush_all
      client.send_ok
    end

    # TTL Command Handlers

    private def handle_expire(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 2
        client.send_error("wrong number of arguments for 'expire' command")
        return
      end

      key = extract_bytes(args[0])
      seconds_str = extract_string(args[1])
      return client.send_error("Invalid arguments") unless key && seconds_str

      seconds = seconds_str.to_i64?
      return client.send_error("value is not an integer or out of range") unless seconds

      ttl_ms = Time.utc.to_unix_ms + (seconds * 1000)
      result = current_db(client).expire(key, ttl_ms)
      client.send_integer(result ? 1_i64 : 0_i64)
    end

    private def handle_expireat(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 2
        client.send_error("wrong number of arguments for 'expireat' command")
        return
      end

      key = extract_bytes(args[0])
      timestamp_str = extract_string(args[1])
      return client.send_error("Invalid arguments") unless key && timestamp_str

      timestamp = timestamp_str.to_i64?
      return client.send_error("value is not an integer or out of range") unless timestamp

      ttl_ms = timestamp * 1000
      result = current_db(client).expire(key, ttl_ms)
      client.send_integer(result ? 1_i64 : 0_i64)
    end

    private def handle_pexpire(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 2
        client.send_error("wrong number of arguments for 'pexpire' command")
        return
      end

      key = extract_bytes(args[0])
      ms_str = extract_string(args[1])
      return client.send_error("Invalid arguments") unless key && ms_str

      milliseconds = ms_str.to_i64?
      return client.send_error("value is not an integer or out of range") unless milliseconds

      ttl_ms = Time.utc.to_unix_ms + milliseconds
      result = current_db(client).expire(key, ttl_ms)
      client.send_integer(result ? 1_i64 : 0_i64)
    end

    private def handle_pexpireat(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 2
        client.send_error("wrong number of arguments for 'pexpireat' command")
        return
      end

      key = extract_bytes(args[0])
      timestamp_str = extract_string(args[1])
      return client.send_error("Invalid arguments") unless key && timestamp_str

      ttl_ms = timestamp_str.to_i64?
      return client.send_error("value is not an integer or out of range") unless ttl_ms

      result = current_db(client).expire(key, ttl_ms)
      client.send_integer(result ? 1_i64 : 0_i64)
    end

    private def handle_ttl(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'ttl' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      client.send_integer(current_db(client).ttl(key))
    end

    private def handle_pttl(args : Array(RespValue), client : Connection) : Nil
      if args.size != 1
        client.send_error("wrong number of arguments for 'pttl' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      client.send_integer(current_db(client).pttl(key))
    end

    private def handle_persist(args : Array(RespValue), client : Connection) : Nil
      return if check_frozen(client)
      if args.size != 1
        client.send_error("wrong number of arguments for 'persist' command")
        return
      end

      key = extract_bytes(args[0])
      return client.send_error("Invalid key") unless key

      result = current_db(client).persist(key)
      client.send_integer(result ? 1_i64 : 0_i64)
    end

    # Key Management Command Handlers

    private def handle_rename(args : Array(RespValue), client : Connection) : Nil
      if args.size != 2
        client.send_error("wrong number of arguments for 'rename' command")
        return
      end
      return if check_frozen(client)

      old_key = extract_bytes(args[0])
      new_key = extract_bytes(args[1])
      return client.send_error("Invalid arguments") unless old_key && new_key

      _, error = current_db(client).rename(old_key, new_key)
      if error
        client.send_error(error)
      else
        client.send_ok
      end
    end

    private def handle_renamenx(args : Array(RespValue), client : Connection) : Nil
      if args.size != 2
        client.send_error("wrong number of arguments for 'renamenx' command")
        return
      end
      return if check_frozen(client)

      old_key = extract_bytes(args[0])
      new_key = extract_bytes(args[1])
      return client.send_error("Invalid arguments") unless old_key && new_key

      result, error = current_db(client).renamenx(old_key, new_key)
      if error
        client.send_error(error)
      else
        client.send_integer(result)
      end
    end

    # SCAN Command Handlers

    private def handle_scan(args : Array(RespValue), client : Connection) : Nil
      if args.empty?
        client.send_error("wrong number of arguments for 'scan' command")
        return
      end

      cursor_str = extract_string(args[0])
      return client.send_error("Invalid cursor") unless cursor_str

      cursor = parse_scan_cursor(cursor_str, client)
      return unless cursor

      options = parse_scan_options(args, 1, client)
      if error = options[:error]
        client.send_error(error) unless error == "handled"
        return
      end

      next_cursor, keys = current_db(client).scan(cursor, options[:pattern], options[:count])

      client.send_cursor_bytes_array(next_cursor.to_s.to_slice, keys)
    end

    private def handle_hscan(args : Array(RespValue), client : Connection) : Nil
      if args.size < 2
        client.send_error("wrong number of arguments for 'hscan' command")
        return
      end

      key = extract_bytes(args[0])
      cursor_str = extract_string(args[1])
      return client.send_error("Invalid arguments") unless key && cursor_str

      cursor = parse_scan_cursor(cursor_str, client)
      return unless cursor

      options = parse_scan_options(args, 2, client)
      if error = options[:error]
        client.send_error(error) unless error == "handled"
        return
      end

      begin
        hash = current_db(client).get_hash(key)
        unless hash
          client.send_cursor_bytes_array("0".to_slice, [] of Bytes)
          return
        end

        next_cursor, items = hash.hscan(cursor, options[:pattern], options[:count])
        client.send_cursor_bytes_array(next_cursor.to_s.to_slice, items)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_sscan(args : Array(RespValue), client : Connection) : Nil
      if args.size < 2
        client.send_error("wrong number of arguments for 'sscan' command")
        return
      end

      key = extract_bytes(args[0])
      cursor_str = extract_string(args[1])
      return client.send_error("Invalid arguments") unless key && cursor_str

      cursor = parse_scan_cursor(cursor_str, client)
      return unless cursor

      options = parse_scan_options(args, 2, client)
      if error = options[:error]
        client.send_error(error) unless error == "handled"
        return
      end

      begin
        set = current_db(client).get_set(key)
        unless set
          client.send_cursor_bytes_array("0".to_slice, [] of Bytes)
          return
        end

        next_cursor, members = set.sscan(cursor, options[:pattern], options[:count])
        client.send_cursor_bytes_array(next_cursor.to_s.to_slice, members)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    private def handle_zscan(args : Array(RespValue), client : Connection) : Nil
      if args.size < 2
        client.send_error("wrong number of arguments for 'zscan' command")
        return
      end

      key = extract_bytes(args[0])
      cursor_str = extract_string(args[1])
      return client.send_error("Invalid arguments") unless key && cursor_str

      cursor = parse_scan_cursor(cursor_str, client)
      return unless cursor

      options = parse_scan_options(args, 2, client)
      if error = options[:error]
        client.send_error(error) unless error == "handled"
        return
      end

      begin
        zset = current_db(client).get_sorted_set(key)
        unless zset
          result = Array(RespValue).new(2) # Empty ZSCAN returns 2 elements
          result << "0".to_slice.as(RespValue)
          result << ([] of RespValue).as(RespValue)
          client.send_array(result)
          return
        end

        next_cursor, items = zset.zscan(cursor, options[:pattern], options[:count])

        result = Array(RespValue).new(2) # ZSCAN returns exactly 2 elements
        result << next_cursor.to_s.to_slice.as(RespValue)
        # Convert items to RespValue (member and score strings)
        item_arr = items.map do |item|
          case item
          when Bytes   then item.as(RespValue)
          when Float64 then item.to_s.to_slice.as(RespValue)
          else              item.as(RespValue)
          end
        end
        result << item_arr.as(RespValue)
        client.send_array(result)
      rescue ex : WrongTypeError
        client.send_error(ex.message)
      end
    end

    # Transaction Command Handlers

    private def handle_multi(args : Array(RespValue), client : Connection) : Nil
      if client.in_transaction?
        client.send_error("MULTI calls can not be nested")
        return
      end
      client.start_transaction
      client.send_ok
    end

    private def handle_exec(args : Array(RespValue), client : Connection) : Nil
      unless client.in_transaction?
        client.send_error("EXEC without MULTI")
        return
      end

      # Check if any watched keys have been modified
      if client.has_watched_keys?
        watch_failed = client.watched_keys.any? do |key_tuple, watched_version|
          db_id, key = key_tuple
          db = @db_manager.get(db_id)
          if db
            current_version = db.get_key_version(key)
            current_version != watched_version
          else
            true # Database no longer exists, consider it modified
          end
        end

        if watch_failed
          # Transaction aborted due to WATCH condition failure
          client.finish_transaction # Clear transaction state and watched keys
          client.send_nil
          return
        end
      end

      queued = client.finish_transaction
      results = Array(RespValue).new(queued.size)

      queued.each do |commands|
        result = execute_and_capture(commands, client)
        results << result
      end

      client.send_array(results)
    end

    private def handle_discard(args : Array(RespValue), client : Connection) : Nil
      unless client.in_transaction?
        client.send_error("DISCARD without MULTI")
        return
      end
      client.discard_transaction
      client.send_ok
    end

    private def handle_watch(args : Array(RespValue), client : Connection) : Nil
      if args.empty?
        client.send_error("wrong number of arguments for 'watch' command")
        return
      end

      if client.in_transaction?
        client.send_error("WATCH inside MULTI is not allowed")
        return
      end

      db = current_db(client)
      db_id = client.current_db_id

      args.each do |arg|
        key = extract_bytes(arg)
        next unless key
        version = db.get_key_version(key)
        client.watch_key(db_id, key, version)
      end

      client.send_ok
    end

    private def handle_unwatch(args : Array(RespValue), client : Connection) : Nil
      client.unwatch_all
      client.send_ok
    end
  end
end
