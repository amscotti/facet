# Facet

Facet is a high-performance, Redis-compatible in-memory database server written in Crystal. Designed as a drop-in replacement for Redis, Facet implements the RESP2/RESP3 protocol and provides unique features tailored for testing and development workflows.

## Key Features

- **Drop-in Redis Replacement**: Compatible with existing Redis clients and tools including `redis-cli`
- **Database Copy and Clone**: Create isolated copies of databases for test isolation without data corruption
- **Dynamic Database Management**: Create, freeze, and manage named databases at runtime
- **High Performance**: Fiber-based concurrency with configurable worker pools
- **Binary Safe**: All keys and values handled as raw bytes
- **TTL Support**: Full key expiration with millisecond precision

## Use Cases

### Test Isolation

Facet's database copy feature makes it ideal for integration testing:

```
# Set up baseline test data once
redis-cli DBCREATE baseline
redis-cli DBSELECT baseline
redis-cli SET user:1 "test_user"
redis-cli DBFREEZE baseline

# Before each test, create an isolated copy
redis-cli DBCOPY baseline test_run_1
redis-cli DBSELECT test_run_1
# Run tests against isolated data...

# Clean up after test
redis-cli DBDROP test_run_1
```

Each test runs against its own database copy, preventing test pollution and enabling parallel test execution.

### Local Development

Use Facet as a lightweight Redis alternative during development without the overhead of running a full Redis installation.

## Installation

### Docker (Recommended)

```bash
docker pull ghcr.io/amscotti/facet:latest
docker run -p 6379:6379 ghcr.io/amscotti/facet:latest
```

### Download Binary

Download pre-built binaries from the [Releases](https://github.com/amscotti/facet/releases) page:

- `facet-linux-amd64.tar.gz` - Linux (static binary)
- `facet-macos-amd64.tar.gz` - macOS Intel
- `facet-macos-arm64.tar.gz` - macOS Apple Silicon

### Building from Source

Prerequisites:
- Crystal 1.10 or later
- Shards (Crystal's dependency manager)

```bash
git clone https://github.com/amscotti/facet.git
cd facet
shards install
crystal build src/facet.cr -o facet --release
```

## Usage

### Starting the Server

```bash
# Binary
./facet

# Docker
docker run -p 6379:6379 ghcr.io/amscotti/facet:latest

# Docker with custom workers
docker run -p 6379:6379 -e FACET_WORKERS=8 ghcr.io/amscotti/facet:latest
```

```bash
# Verify it's running
redis-cli ping
# PONG
```

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FACET_WORKERS` | 4 | Number of worker fibers for handling connections |

### Connecting with Redis Clients

Facet works with any Redis client library:

```python
# Python
import redis
r = redis.Redis(host='localhost', port=6379)
r.set('key', 'value')
```

```javascript
// Node.js
import { createClient } from 'redis';
const client = createClient();
await client.connect();
await client.set('key', 'value');
```

```ruby
# Ruby
require 'redis'
redis = Redis.new
redis.set('key', 'value')
```

## Supported Commands

### String Commands
`GET`, `SET`, `SETEX`, `SETNX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`, `GETRANGE`, `SETRANGE`, `STRLEN`, `APPEND`, `MGET`, `MSET`, `MSETNX`, `INCR`, `INCRBY`, `INCRBYFLOAT`, `DECR`, `DECRBY`

### Key Commands
`DEL`, `EXISTS`, `KEYS`, `TYPE`, `RENAME`, `RENAMENX`, `SCAN`, `EXPIRE`, `EXPIREAT`, `PEXPIRE`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`

### List Commands
`LPUSH`, `LPUSHX`, `RPUSH`, `RPUSHX`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LINSERT`, `LREM`, `LTRIM`, `LMOVE`, `LPOS`

### Hash Commands
`HSET`, `HSETNX`, `HGET`, `HMSET`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HINCRBY`, `HINCRBYFLOAT`, `HSTRLEN`, `HSCAN`

### Set Commands
`SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SPOP`, `SRANDMEMBER`, `SMOVE`, `SDIFF`, `SDIFFSTORE`, `SINTER`, `SINTERSTORE`, `SUNION`, `SUNIONSTORE`, `SSCAN`

### Sorted Set Commands
`ZADD`, `ZREM`, `ZSCORE`, `ZMSCORE`, `ZRANK`, `ZREVRANK`, `ZCARD`, `ZCOUNT`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZRANGESTORE`, `ZINCRBY`, `ZPOPMIN`, `ZPOPMAX`, `ZRANDMEMBER`, `ZSCAN`

### Transaction Commands
`MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

### Server Commands
`PING`, `ECHO`, `QUIT`, `SELECT`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `COMMAND`, `CONFIG`

### Facet Extensions

These commands extend Redis functionality for database management:

| Command | Description |
|---------|-------------|
| `DBCREATE <name>` | Create a new named database |
| `DBDROP <name>` | Delete a named database |
| `DBSELECT <name>` | Switch to a named database |
| `DBLIST` | List all databases |
| `DBCOPY <source> <dest>` | Copy database to new name |
| `DBCOPYKEYS <source> <dest> <pattern>` | Copy matching keys between databases |
| `DBFREEZE <name>` | Make database read-only |
| `DBUNFREEZE <name>` | Make database writable |
| `DBINFO [name]` | Get database information |
| `DBRESET <name>` | Clear all data in database |

## Development

### Running Tests

```bash
# Run all tests
crystal spec

# Run specific test file
crystal spec spec/commands/string_commands_spec.cr

# Run tests matching a pattern
crystal spec --example "SET command"
```

### Linting

```bash
bin/ameba
```

### Type Checking

```bash
crystal build src/facet.cr --no-codegen
```

## Architecture

Facet uses a fiber-based concurrency model:

1. **TCP Server**: Accepts connections and distributes to worker pool
2. **Worker Fibers**: Process client connections concurrently
3. **RESP Parser**: Streaming protocol parser for Redis wire format
4. **Command Handler**: Dispatches commands to appropriate handlers
5. **Storage Layer**: Hash-based storage with lazy TTL expiration

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -am 'Add my feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Create a Pull Request

Please ensure all tests pass and code follows the existing style.

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

## Author

Anthony Scotti
