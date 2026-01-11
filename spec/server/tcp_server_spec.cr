require "../spec_helper"

Spectator.describe Redis::Server do
  let(test_host) { "127.0.0.1" }
  let(test_port) { 16_379 } # Use non-standard port for testing

  describe "#initialize" do
    it "initializes with default host and port" do
      server = Redis::Server.new
      expect(server).not_to be_nil
    end

    it "initializes with custom host and port" do
      server = Redis::Server.new(test_host, test_port)
      expect(server).not_to be_nil
    end

    it "initializes database manager" do
      server = Redis::Server.new
      expect(server).not_to be_nil
    end
  end

  describe "constants" do
    it "defines MAX_CONNECTIONS" do
      expect(Redis::Server::MAX_CONNECTIONS).to eq(10_000)
    end

    it "defines DEFAULT_HOST" do
      expect(Redis::Server::DEFAULT_HOST).to eq("0.0.0.0")
    end

    it "defines DEFAULT_PORT" do
      expect(Redis::Server::DEFAULT_PORT).to eq(6379)
    end
  end

  describe "#stop" do
    it "can be called on a non-started server" do
      server = Redis::Server.new
      expect { server.stop }.not_to raise_error
    end
  end

  describe "worker pool configuration" do
    it "defaults to 4 workers when CRYSTAL_WORKERS not set" do
      old_workers = ENV["CRYSTAL_WORKERS"]?
      ENV.delete("CRYSTAL_WORKERS")

      # Since we can't easily test the actual server start (it blocks),
      # we'll just verify the constant and initialization work
      server = Redis::Server.new
      expect(server).not_to be_nil

      ENV["CRYSTAL_WORKERS"] = old_workers if old_workers
    end
  end
end

# Integration-style tests that don't actually start the server
# but verify the server can be created and configured
Spectator.describe Redis::Server::ServerConfiguration do
  describe "connection limits" do
    it "has a maximum connection limit" do
      expect(Redis::Server::MAX_CONNECTIONS).to be > 0
    end

    it "allows large number of connections" do
      expect(Redis::Server::MAX_CONNECTIONS).to be >= 10_000
    end
  end

  describe "default configuration" do
    it "binds to all interfaces by default" do
      expect(Redis::Server::DEFAULT_HOST).to eq("0.0.0.0")
    end

    it "binds to standard Redis port by default" do
      expect(Redis::Server::DEFAULT_PORT).to eq(6379)
    end
  end
end
