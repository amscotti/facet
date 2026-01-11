require "./lib"

VERSION = "0.1.0"

if ARGV.includes?("--version") || ARGV.includes?("-v")
  puts "Facet #{VERSION}"
  exit 0
end

if ARGV.includes?("--help") || ARGV.includes?("-h")
  puts <<-HELP
  Facet - Redis-compatible in-memory database server

  Usage: facet [options]

  Options:
    -v, --version  Show version
    -h, --help     Show this help

  Environment Variables:
    FACET_WORKERS  Number of worker fibers (default: 4)

  The server listens on 0.0.0.0:6379 by default.
  HELP
  exit 0
end

server = Redis::Server.new
server.start

Signal::INT.trap do
  server.stop
end

sleep
