require "../spec_helper"

Spectator.describe Redis::RespParser do
  describe "#parse" do
    context "simple strings" do
      it "parses simple string" do
        parser = Redis::RespParser.new(resp_io("+OK\r\n"))
        expect(parser.parse).to eq("OK")
      end

      it "parses empty simple string" do
        parser = Redis::RespParser.new(resp_io("+\r\n"))
        expect(parser.parse).to eq("")
      end

      it "parses simple string with spaces" do
        parser = Redis::RespParser.new(resp_io("+Hello World\r\n"))
        expect(parser.parse).to eq("Hello World")
      end
    end

    context "errors" do
      it "parses error message" do
        parser = Redis::RespParser.new(resp_io("-ERR unknown command\r\n"))
        expect(parser.parse).to eq("ERR unknown command")
      end

      it "parses error with type prefix" do
        parser = Redis::RespParser.new(resp_io("-WRONGTYPE Operation against a key\r\n"))
        expect(parser.parse).to eq("WRONGTYPE Operation against a key")
      end
    end

    context "integers" do
      it "parses positive integer" do
        parser = Redis::RespParser.new(resp_io(":1000\r\n"))
        expect(parser.parse).to eq(1000_i64)
      end

      it "parses negative integer" do
        parser = Redis::RespParser.new(resp_io(":-500\r\n"))
        expect(parser.parse).to eq(-500_i64)
      end

      it "parses zero" do
        parser = Redis::RespParser.new(resp_io(":0\r\n"))
        expect(parser.parse).to eq(0_i64)
      end
    end

    context "bulk strings" do
      it "parses bulk string" do
        parser = Redis::RespParser.new(resp_io("$5\r\nhello\r\n"))
        result = parser.parse
        expect(result).to be_a(Bytes)
        expect(String.new(result.as(Bytes))).to eq("hello")
      end

      it "parses empty bulk string" do
        parser = Redis::RespParser.new(resp_io("$0\r\n\r\n"))
        result = parser.parse
        expect(result).to be_a(Bytes)
        expect(result.as(Bytes).size).to eq(0)
      end

      it "parses null bulk string" do
        parser = Redis::RespParser.new(resp_io("$-1\r\n"))
        expect(parser.parse).to be_nil
      end

      it "parses bulk string with binary data" do
        parser = Redis::RespParser.new(resp_io("$6\r\nhel\x00lo\r\n"))
        result = parser.parse
        expect(result).to be_a(Bytes)
        expect(result.as(Bytes).size).to eq(6)
      end

      it "rejects malformed bulk string terminators" do
        parser = Redis::RespParser.new(resp_io("$5\r\nhello\n\n"))
        expect { parser.parse }.to raise_error(Redis::ParseError, /CRLF/)
      end
    end

    context "arrays" do
      it "parses empty array" do
        parser = Redis::RespParser.new(resp_io("*0\r\n"))
        result = parser.parse
        expect(result).to be_a(Array(Redis::RespValue))
        expect(result.as(Array).size).to eq(0)
      end

      it "parses array of bulk strings" do
        parser = Redis::RespParser.new(resp_io("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
        result = parser.parse
        expect(result).to be_a(Array(Redis::RespValue))
        arr = result.as(Array)
        expect(arr.size).to eq(2)
        expect(String.new(arr[0].as(Bytes))).to eq("foo")
        expect(String.new(arr[1].as(Bytes))).to eq("bar")
      end

      it "parses array of integers" do
        parser = Redis::RespParser.new(resp_io("*3\r\n:1\r\n:2\r\n:3\r\n"))
        result = parser.parse
        expect(result).to be_a(Array(Redis::RespValue))
        arr = result.as(Array)
        expect(arr).to eq([1_i64, 2_i64, 3_i64])
      end

      it "parses null array" do
        parser = Redis::RespParser.new(resp_io("*-1\r\n"))
        expect(parser.parse).to be_nil
      end

      it "parses nested array" do
        parser = Redis::RespParser.new(resp_io("*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n"))
        result = parser.parse
        expect(result).to be_a(Array(Redis::RespValue))
        arr = result.as(Array)
        expect(arr.size).to eq(2)
        expect(arr[0].as(Array)).to eq([1_i64, 2_i64])
        expect(arr[1].as(Array)).to eq([3_i64, 4_i64])
      end

      it "parses mixed type array" do
        parser = Redis::RespParser.new(resp_io("*3\r\n:1\r\n$3\r\nfoo\r\n+OK\r\n"))
        result = parser.parse
        expect(result).to be_a(Array(Redis::RespValue))
        arr = result.as(Array)
        expect(arr[0]).to eq(1_i64)
        expect(String.new(arr[1].as(Bytes))).to eq("foo")
        expect(arr[2]).to eq("OK")
      end
    end

    context "booleans" do
      it "parses true" do
        parser = Redis::RespParser.new(resp_io("#t\r\n"))
        expect(parser.parse).to eq(true)
      end

      it "parses false" do
        parser = Redis::RespParser.new(resp_io("#f\r\n"))
        expect(parser.parse).to eq(false)
      end
    end

    context "doubles" do
      it "parses positive double" do
        parser = Redis::RespParser.new(resp_io(",3.14\r\n"))
        expect(parser.parse).to eq(3.14)
      end

      it "parses negative double" do
        parser = Redis::RespParser.new(resp_io(",-2.5\r\n"))
        expect(parser.parse).to eq(-2.5)
      end

      it "parses infinity" do
        parser = Redis::RespParser.new(resp_io(",inf\r\n"))
        result = parser.parse
        expect(result).to be_a(Float64)
        expect(result.as(Float64).infinite?).to eq(1)
      end

      it "parses negative infinity" do
        parser = Redis::RespParser.new(resp_io(",-inf\r\n"))
        result = parser.parse
        expect(result).to be_a(Float64)
        expect(result.as(Float64).infinite?).to eq(-1)
      end

      it "parses nan" do
        parser = Redis::RespParser.new(resp_io(",nan\r\n"))
        result = parser.parse
        expect(result).to be_a(Float64)
        expect(result.as(Float64).nan?).to be_true
      end
    end

    context "null" do
      it "parses RESP3 null" do
        parser = Redis::RespParser.new(resp_io("_\r\n"))
        expect(parser.parse).to be_nil
      end
    end

    context "maps" do
      it "parses empty map" do
        parser = Redis::RespParser.new(resp_io("%0\r\n"))
        result = parser.parse
        expect(result).to be_a(Hash(Redis::RespValue, Redis::RespValue))
        expect(result.as(Hash).size).to eq(0)
      end

      it "parses map with string keys" do
        parser = Redis::RespParser.new(resp_io("%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n"))
        result = parser.parse
        expect(result).to be_a(Hash(Redis::RespValue, Redis::RespValue))
        hash = result.as(Hash)
        expect(hash["key1"]).to eq(1_i64)
        expect(hash["key2"]).to eq(2_i64)
      end
    end

    context "sets" do
      it "parses empty set" do
        parser = Redis::RespParser.new(resp_io("~0\r\n"))
        result = parser.parse
        expect(result).to be_a(Set(Redis::RespValue))
        expect(result.as(Set).size).to eq(0)
      end

      it "parses set with values" do
        parser = Redis::RespParser.new(resp_io("~3\r\n:1\r\n:2\r\n:3\r\n"))
        result = parser.parse
        expect(result).to be_a(Set(Redis::RespValue))
        set = result.as(Set)
        expect(set.size).to eq(3)
        expect(set.includes?(1_i64)).to be_true
        expect(set.includes?(2_i64)).to be_true
        expect(set.includes?(3_i64)).to be_true
      end
    end

    context "big numbers" do
      it "parses big number as string" do
        parser = Redis::RespParser.new(resp_io("(3492890328409238509324850943850943825024385\r\n"))
        result = parser.parse
        expect(result).to eq("3492890328409238509324850943850943825024385")
      end
    end

    context "inline commands" do
      it "parses single-word inline command" do
        parser = Redis::RespParser.new(resp_io("PING\r\n"))
        result = parser.parse
        expect(result).to be_a(Array(Redis::RespValue))
        arr = result.as(Array)
        expect(arr.size).to eq(1)
        expect(arr[0]).to eq("PING".to_slice)
      end

      it "parses multi-word inline command" do
        parser = Redis::RespParser.new(resp_io("SET foo bar\r\n"))
        result = parser.parse
        expect(result).to be_a(Array(Redis::RespValue))
        arr = result.as(Array)
        expect(arr.size).to eq(3)
        expect(arr[0]).to eq("SET".to_slice)
        expect(arr[1]).to eq("foo".to_slice)
        expect(arr[2]).to eq("bar".to_slice)
      end

      it "handles multiple spaces in inline command" do
        parser = Redis::RespParser.new(resp_io("SET  key   value\r\n"))
        result = parser.parse
        arr = result.as(Array)
        expect(arr.size).to eq(3) # Empty strings are removed
      end
    end

    context "error handling" do
      it "raises ParseError on invalid boolean" do
        parser = Redis::RespParser.new(resp_io("#x\r\n"))
        expect { parser.parse }.to raise_error(Redis::ParseError)
      end

      it "returns nil on empty input" do
        parser = Redis::RespParser.new(resp_io(""))
        expect(parser.parse).to be_nil
      end
    end
  end
end
