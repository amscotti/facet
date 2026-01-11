# CrystalDB: Redis-Compatible Database with Dynamic Database Management

## Project Overview

A Redis-compatible in-memory database server written in Crystal, featuring dynamic database creation/copying for testing and general-purpose use.

### Unique Selling Points
- **Dynamic Database Creation**: Create isolated databases on-the-fly via commands
- **Database Cloning**: Copy entire databases atomically for testing snapshots
- **Redis Protocol Compatibility**: Works with existing Redis clients and tools
- **Crystal Performance**: Leverages Crystal's speed and concurrency features

---

## Phase 1: Foundation (Weeks 1-2)

### 1.1 Project Setup
```
Tasks:
- [ ] Initialize Crystal project with `crystal init app crystaldb`
- [ ] Set up directory structure:
    src/
      crystaldb.cr           # Main entry point
      server/
        tcp_server.cr        # TCP server using Crystal's Socket
        connection.cr        # Client connection handler
      protocol/
        resp_parser.cr       # RESP2/RESP3 protocol parser
        resp_serializer.cr   # Response serialization
      commands/
        base_command.cr      # Command interface
        registry.cr          # Command dispatcher
      storage/
        database.cr          # Single database instance
        database_manager.cr  # Multi-database management
        data_types/
          string_type.cr
          list_type.cr
          hash_type.cr
          set_type.cr
          sorted_set_type.cr
      utils/
        expiry_manager.cr    # TTL handling with Crystal's Time
    spec/
      # Mirror structure for tests
- [ ] Configure shard.yml with dependencies
- [ ] Set up GitHub Actions CI
```

### 1.2 RESP Protocol Implementation
```
Tasks:
- [ ] Implement RESP2 parser (Simple Strings, Errors, Integers, Bulk Strings, Arrays)
- [ ] Implement RESP3 extensions (Maps, Sets, Nulls, Booleans, Doubles)
- [ ] Create streaming parser for large payloads using IO
- [ ] Write comprehensive protocol tests
- [ ] Benchmark against redis-benchmark tool

Crystal Features to Use:
- IO::Memory for buffer management
- Bytes and Slice for zero-copy parsing
- Crystal's powerful pattern matching with case/when
```

### 1.3 TCP Server Foundation
```
Tasks:
- [ ] Implement non-blocking TCP server using TCPServer
- [ ] Use Crystal's Fiber-based concurrency (spawn) for client connections
- [ ] Implement connection pooling with Channel(Connection)
- [ ] Add graceful shutdown with Signal::INT handling
- [ ] Support Unix socket connections

Crystal Features to Use:
- TCPServer and TCPSocket from Socket module
- Fibers for lightweight concurrency (millions of concurrent connections)
- Channels for communication between fibers
- select statement for multiplexing
```

---

## Phase 2: Core Data Structures (Weeks 3-4)

### 2.1 String Operations
```
Commands to Implement:
- [ ] SET (with EX, PX, NX, XX, KEEPTTL, GET, EXAT, PXAT, IFEQ, IFGT)
- [ ] GET, GETEX, GETDEL, GETSET
- [ ] MGET, MSET, MSETNX
- [ ] APPEND, STRLEN
- [ ] INCR, INCRBY, INCRBYFLOAT, DECR, DECRBY
- [ ] SETRANGE, GETRANGE
- [ ] SETNX, SETEX, PSETEX

Implementation Notes:
- Store as Bytes internally for binary safety
- Use Crystal's BigFloat for INCRBYFLOAT precision
- Implement copy-on-write for large strings
```

### 2.2 List Operations
```
Commands to Implement:
- [ ] LPUSH, RPUSH, LPUSHX, RPUSHX
- [ ] LPOP, RPOP, BLPOP, BRPOP (blocking with Fiber.yield)
- [ ] LRANGE, LINDEX, LSET, LLEN
- [ ] LINSERT, LPOS, LREM
- [ ] LMOVE, BLMOVE, LMPOP, BLMPOP
- [ ] LTRIM

Implementation Notes:
- Use Deque(Bytes) for O(1) operations at both ends
- Implement blocking operations with Channel and select
- Support timeout with spawn and sleep
```

### 2.3 Hash Operations
```
Commands to Implement:
- [ ] HSET, HGET, HMSET, HMGET
- [ ] HDEL, HEXISTS, HLEN
- [ ] HKEYS, HVALS, HGETALL
- [ ] HINCRBY, HINCRBYFLOAT
- [ ] HSETNX, HSTRLEN
- [ ] HSCAN, HRANDFIELD

Implementation Notes:
- Use Hash(Bytes, Bytes) internally
- Implement progressive rehashing for large hashes
```

### 2.4 Set Operations
```
Commands to Implement:
- [ ] SADD, SREM, SMEMBERS, SISMEMBER, SMISMEMBER
- [ ] SCARD, SPOP, SRANDMEMBER
- [ ] SDIFF, SDIFFSTORE, SINTER, SINTERSTORE, SUNION, SUNIONSTORE
- [ ] SMOVE, SSCAN
- [ ] SINTERCARD

Implementation Notes:
- Use Set(Bytes) from Crystal stdlib
- Implement intset optimization for small integer-only sets
```

### 2.5 Sorted Set Operations
```
Commands to Implement:
- [ ] ZADD (with NX, XX, GT, LT, CH, INCR)
- [ ] ZREM, ZCARD, ZCOUNT, ZLEXCOUNT
- [ ] ZRANGE, ZRANGEBYLEX, ZRANGEBYSCORE, ZREVRANGE
- [ ] ZRANK, ZREVRANK, ZSCORE, ZMSCORE
- [ ] ZINCRBY, ZPOPMIN, ZPOPMAX, BZPOPMIN, BZPOPMAX
- [ ] ZUNION, ZINTER, ZDIFF (with STORE variants)
- [ ] ZRANGESTORE, ZSCAN, ZRANDMEMBER

Implementation Notes:
- Implement skip list for O(log N) operations
- Use Crystal's generics: SkipList(T, Score)
- Maintain both score->member and member->score mappings
```

---

## Phase 3: Dynamic Database Management (Weeks 5-6) ⭐ KEY DIFFERENTIATOR

### 3.1 Multi-Database Architecture
```
Tasks:
- [ ] Implement DatabaseManager class with thread-safe operations
- [ ] Support Redis's SELECT command (databases 0-15 by default)
- [ ] Make database limit configurable (--databases N flag)

Crystal Implementation:
class DatabaseManager
  @databases : Hash(Int32 | String, Database)
  @mutex : Mutex
  
  def get_or_create(id : Int32 | String) : Database
    @mutex.synchronize do
      @databases[id] ||= Database.new(id)
    end
  end
end
```

### 3.2 Dynamic Database Creation (CUSTOM COMMANDS)
```
New Commands:
- [ ] DBCREATE <name> [OPTIONS]
    - Creates a named database (beyond numeric IDs)
    - Options: MAXMEMORY, EVICTION_POLICY, PERSIST
    - Returns: OK or error if exists (unless IF NOT EXISTS)
    
- [ ] DBLIST [PATTERN]
    - Lists all databases with stats
    - Returns: Array of [name, key_count, memory_usage, created_at]
    
- [ ] DBINFO <name>
    - Detailed stats for a specific database
    - Returns: Hash with comprehensive metrics
    
- [ ] DBDROP <name> [IF EXISTS]
    - Deletes a database and all its data
    - Returns: OK or error

- [ ] DBSELECT <name|number>
    - Extended SELECT that works with named databases
    - Maintains backward compatibility with SELECT <number>
```

### 3.3 Database Cloning (CUSTOM COMMANDS) ⭐
```
New Commands:
- [ ] DBCOPY <source> <destination> [OPTIONS]
    - Atomically copies all data from source to destination
    - Options:
      - REPLACE: Overwrite if destination exists
      - ASYNC: Return immediately, copy in background
      - SHALLOW: Copy-on-write (memory efficient)
    - Returns: OK or status for ASYNC
    
- [ ] DBCOPYKEYS <source> <destination> <pattern> [OPTIONS]
    - Copy only keys matching pattern
    - Useful for partial test data setup
    
- [ ] DBSNAPSHOT <name> -> <snapshot_name>
    - Creates a point-in-time snapshot
    - Uses copy-on-write semantics
    - Perfect for test isolation

Crystal Implementation:
def copy_database(source : Database, dest_name : String, options : CopyOptions) : Nil
  dest = create_database(dest_name)
  
  if options.shallow?
    # Copy-on-write: Share data until modified
    dest.share_data_from(source)
  else
    # Deep copy using Crystal's clone
    source.each_entry do |key, value, ttl|
      dest.set(key.clone, value.deep_clone, ttl)
    end
  end
end
```

### 3.4 Test-Oriented Features
```
New Commands:
- [ ] DBRESET <name>
    - Clears database but keeps structure/config
    - Fast cleanup between tests
    
- [ ] DBFREEZE <name>
    - Makes database read-only
    - Useful for reference datasets
    
- [ ] DBUNFREEZE <name>
    - Restores write capability

- [ ] DBTRANSACTION <commands...>
    - Execute commands atomically with rollback on failure
    - Useful for test setup that must succeed entirely
```

---

## Phase 4: Essential Redis Commands (Weeks 7-8)

### 4.1 Key Management
```
Commands:
- [ ] DEL, UNLINK (async delete)
- [ ] EXISTS, TYPE, KEYS, SCAN
- [ ] EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT, EXPIRETIME, PEXPIRETIME
- [ ] TTL, PTTL, PERSIST
- [ ] RENAME, RENAMENX
- [ ] TOUCH, OBJECT (ENCODING, REFCOUNT, IDLETIME, FREQ)
- [ ] COPY (Redis 6.2+), DUMP, RESTORE
- [ ] RANDOMKEY, DBSIZE, FLUSHDB, FLUSHALL
```

### 4.2 Pub/Sub System
```
Commands:
- [ ] SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE
- [ ] PUBLISH, PUBSUB (CHANNELS, NUMSUB, NUMPAT)

Crystal Implementation:
class PubSub
  @channels : Hash(String, Array(Channel(Message)))
  @patterns : Hash(Regex, Array(Channel(Message)))
  
  def subscribe(client : Connection, channel : String)
    ch = Channel(Message).new
    @channels[channel] << ch
    spawn do
      loop do
        msg = ch.receive
        client.send(msg)
      end
    end
  end
end
```

### 4.3 Transactions
```
Commands:
- [ ] MULTI, EXEC, DISCARD
- [ ] WATCH, UNWATCH

Implementation Notes:
- Queue commands during MULTI
- Implement optimistic locking for WATCH
- Use Crystal's Mutex for transaction isolation
```

### 4.4 Scripting (Lua Compatibility Layer)
```
Options:
1. Embed Lua interpreter (complex but compatible)
2. Implement subset in Crystal DSL (simpler, less compatible)
3. Support EVAL with Crystal scripts (innovative but breaking)

Recommended: Option 2 with migration path
- [ ] EVAL, EVALSHA (basic Lua subset)
- [ ] SCRIPT LOAD, EXISTS, FLUSH
- [ ] FUNCTION LOAD (Redis 7.0+ style)
```

---

## Phase 5: Persistence & Replication (Weeks 9-10)

### 5.1 RDB Persistence
```
Tasks:
- [ ] Implement RDB file format parser
- [ ] Implement RDB file format writer
- [ ] BGSAVE using Process.fork (Crystal supports this)
- [ ] SAVE (blocking save)
- [ ] Automatic saving based on configuration
- [ ] RDB compression with Compress::Zlib

Crystal Feature:
Process.fork do
  # Child process for background save
  save_rdb_snapshot(path)
end
```

### 5.2 AOF Persistence
```
Tasks:
- [ ] Implement append-only file logging
- [ ] fsync policies: always, everysec, no
- [ ] AOF rewrite to compact the log
- [ ] Mixed persistence (RDB + AOF)

Crystal Implementation:
class AOFWriter
  @file : File
  @buffer : IO::Memory
  @sync_policy : SyncPolicy
  
  def append(command : Array(Bytes))
    @buffer << serialize_resp(command)
    
    case @sync_policy
    when .always?
      flush_and_sync
    when .everysec?
      schedule_sync_if_needed
    end
  end
end
```

### 5.3 Replication
```
Tasks:
- [ ] REPLICAOF command
- [ ] Full synchronization protocol
- [ ] Partial resynchronization with replication backlog
- [ ] Replica read-only mode
- [ ] WAIT command for synchronous replication
```

---

## Phase 6: Production Features (Weeks 11-12)

### 6.1 Memory Management
```
Tasks:
- [ ] Implement memory tracking per database
- [ ] Eviction policies: noeviction, allkeys-lru, volatile-lru, allkeys-random, volatile-random, volatile-ttl, allkeys-lfu, volatile-lfu
- [ ] MEMORY command (USAGE, DOCTOR, STATS)
- [ ] Per-database memory limits

Crystal Implementation:
# Use GC stats and manual tracking
struct MemoryTracker
  @allocated : Atomic(Int64)
  @limit : Int64
  
  def track_allocation(size : Int64) : Bool
    loop do
      current = @allocated.get
      return false if current + size > @limit
      break if @allocated.compare_and_set(current, current + size)
    end
    true
  end
end
```

### 6.2 Security
```
Tasks:
- [ ] AUTH command with password
- [ ] ACL system (users, permissions, categories)
- [ ] TLS support using OpenSSL shard
- [ ] Command renaming/disabling
- [ ] Protected mode
```

### 6.3 Cluster Mode (Optional/Future)
```
Tasks:
- [ ] Consistent hashing with hash slots
- [ ] CLUSTER commands
- [ ] Gossip protocol for node discovery
- [ ] Automatic failover
```

---

## Phase 7: Observability & DevEx (Week 13)

### 7.1 Monitoring
```
Tasks:
- [ ] INFO command (all sections)
- [ ] CLIENT command (LIST, KILL, SETNAME, etc.)
- [ ] SLOWLOG
- [ ] LATENCY commands
- [ ] DEBUG commands (for testing)
- [ ] MONITOR command (real-time command stream)
```

### 7.2 Configuration
```
Tasks:
- [ ] CONFIG GET/SET/REWRITE
- [ ] Command-line argument parsing
- [ ] Configuration file support (redis.conf compatible where possible)
```

### 7.3 Testing Tools
```
Custom Commands for Testing:
- [ ] DBEXPORT <name> <format>
    - Export database to JSON/YAML for inspection
    
- [ ] DBIMPORT <name> <format> <data>
    - Import test fixtures
    
- [ ] DBDIFF <db1> <db2>
    - Compare two databases, return differences
    
- [ ] DBASSERT <name> <key> <expected_value>
    - Assertion command for testing
    - Returns error if assertion fails
```

---

## Crystal-Specific Implementation Notes

### Concurrency Model
```crystal
# Use fibers for connection handling
spawn do
  loop do
    client = server.accept
    spawn handle_client(client)
  end
end

# Use channels for pub/sub and blocking operations
channel = Channel(Message).new(buffer: 100)

select
when msg = channel.receive
  process(msg)
when timeout(5.seconds)
  handle_timeout
end
```

### Memory Efficiency
```crystal
# Use Bytes instead of String for binary safety
struct Entry
  getter key : Bytes
  getter value : Bytes
  getter expires_at : Time?
end

# Use object pools for frequently allocated objects
class ConnectionPool
  @pool : Channel(Connection)
  
  def borrow(&)
    conn = @pool.receive
    begin
      yield conn
    ensure
      @pool.send(conn)
    end
  end
end
```

### Performance Optimizations
```crystal
# Use macros for command dispatch
macro define_commands(*names)
  {% for name in names %}
    when {{name.upcase.stringify}}
      execute_{{name.id}}(args)
  {% end %}
end

# Compile-time command registration
COMMANDS = {
  "GET" => GetCommand,
  "SET" => SetCommand,
  # ...
}
```

---

## Testing Strategy

### Unit Tests
```
- Protocol parsing/serialization
- Each data structure operation
- Expiry logic
- Database management operations
```

### Integration Tests
```
- Use redis-cli for compatibility testing
- Test with popular Redis client libraries
- Benchmark with redis-benchmark
- Test database copy/clone operations
```

### Compatibility Tests
```
- Run Redis test suite where applicable
- Test with real applications:
  - Sidekiq (job queue)
  - Rails cache
  - Session stores
```

---

## Suggested Dependencies (shard.yml)

```yaml
dependencies:
  option_parser:
    github: crystal-lang/crystal  # stdlib
  socket:
    github: crystal-lang/crystal  # stdlib
    
  # Optional, for Lua scripting
  lua:
    github: veelenga/lua.cr
    
  # For metrics/monitoring  
  crometheus:
    github: darwinnn/crometheus
    
dev_dependencies:
  spectator:
    github: icy-arctic-fox/spectator
  ameba:
    github: crystal-ameba/ameba
```

---

## Command Priority Matrix

### Must Have (MVP)
| Category | Commands |
|----------|----------|
| Strings | GET, SET, DEL, EXISTS |
| Lists | LPUSH, RPUSH, LPOP, RPOP, LRANGE |
| Hashes | HGET, HSET, HDEL, HGETALL |
| Sets | SADD, SREM, SMEMBERS |
| Keys | KEYS, EXPIRE, TTL, TYPE |
| Server | PING, ECHO, INFO, SELECT |
| **Custom** | **DBCREATE, DBCOPY, DBLIST, DBRESET** |

### Should Have
| Category | Commands |
|----------|----------|
| Strings | INCR, MGET, MSET, APPEND |
| Lists | BLPOP, BRPOP, LLEN, LINDEX |
| Sorted Sets | ZADD, ZRANGE, ZRANK, ZSCORE |
| Transactions | MULTI, EXEC, DISCARD |
| Pub/Sub | SUBSCRIBE, PUBLISH |
| Persistence | SAVE, BGSAVE |

### Nice to Have
| Category | Commands |
|----------|----------|
| Scripting | EVAL, EVALSHA |
| Cluster | CLUSTER commands |
| Streams | XADD, XREAD |

---

## Success Metrics

1. **Compatibility**: Pass 80%+ of Redis commands commonly used
2. **Performance**: Within 2x of Redis for common operations
3. **Memory**: Comparable or better memory efficiency
4. **Unique Features**: Database operations work reliably
5. **Adoption**: Works as drop-in replacement for test environments

---

## Getting Started Commands for Agent

```bash
# Create project
crystal init app crystaldb
cd crystaldb

# Initial file structure
mkdir -p src/{server,protocol,commands,storage/data_types,utils}
mkdir -p spec/{server,protocol,commands,storage}

# Start with these files in order:
# 1. src/protocol/resp_parser.cr
# 2. src/protocol/resp_serializer.cr  
# 3. src/storage/database.cr
# 4. src/server/tcp_server.cr
# 5. src/commands/base_command.cr
# 6. src/crystaldb.cr (main)

# Run tests
crystal spec

# Run server
crystal run src/crystaldb.cr -- --port 6379

# Test with redis-cli
redis-cli -p 6379 PING
```

---

## Estimated Timeline

| Phase | Duration | Deliverable |
|-------|----------|-------------|
| 1. Foundation | 2 weeks | Working RESP server, accepts connections |
| 2. Data Structures | 2 weeks | String, List, Hash, Set, Sorted Set |
| 3. Database Management | 2 weeks | DBCREATE, DBCOPY, DBLIST ⭐ |
| 4. Essential Commands | 2 weeks | 80% command coverage |
| 5. Persistence | 2 weeks | RDB + AOF |
| 6. Production Features | 2 weeks | Memory management, security |
| 7. Polish | 1 week | Docs, testing tools |

**Total: ~13 weeks for production-ready MVP**
