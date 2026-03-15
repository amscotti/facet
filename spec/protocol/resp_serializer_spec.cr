require "../spec_helper"

Spectator.describe Redis::RespSerializer do
  describe ".serialize" do
    context "simple strings" do
      it "serializes simple string" do
        result = Redis::RespSerializer.serialize("OK")
        expect(String.new(result)).to eq("+OK\r\n")
      end

      it "serializes empty string" do
        result = Redis::RespSerializer.serialize("")
        expect(String.new(result)).to eq("+\r\n")
      end

      it "serializes RESP errors" do
        result = Redis::RespSerializer.serialize(Redis::RespError.new("boom"))
        expect(String.new(result)).to eq("-ERR boom\r\n")
      end
    end

    context "integers" do
      it "serializes positive integer" do
        result = Redis::RespSerializer.serialize(1000_i64)
        expect(String.new(result)).to eq(":1000\r\n")
      end

      it "serializes negative integer" do
        result = Redis::RespSerializer.serialize(-500_i64)
        expect(String.new(result)).to eq(":-500\r\n")
      end

      it "serializes zero" do
        result = Redis::RespSerializer.serialize(0_i64)
        expect(String.new(result)).to eq(":0\r\n")
      end
    end

    context "bulk strings (bytes)" do
      it "serializes bulk string" do
        result = Redis::RespSerializer.serialize(b("hello"))
        expect(String.new(result)).to eq("$5\r\nhello\r\n")
      end

      it "serializes empty bulk string" do
        result = Redis::RespSerializer.serialize(Bytes.empty)
        expect(String.new(result)).to eq("$0\r\n\r\n")
      end
    end

    context "arrays" do
      it "serializes empty array" do
        arr = [] of Redis::RespValue
        result = Redis::RespSerializer.serialize(arr)
        expect(String.new(result)).to eq("*0\r\n")
      end

      it "serializes array of integers" do
        arr = [1_i64, 2_i64, 3_i64] of Redis::RespValue
        result = Redis::RespSerializer.serialize(arr)
        expect(String.new(result)).to eq("*3\r\n:1\r\n:2\r\n:3\r\n")
      end

      it "serializes array of bulk strings" do
        arr = [b("foo"), b("bar")] of Redis::RespValue
        result = Redis::RespSerializer.serialize(arr)
        expect(String.new(result)).to eq("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
      end

      it "serializes mixed array" do
        arr = [1_i64, b("foo"), "OK"] of Redis::RespValue
        result = Redis::RespSerializer.serialize(arr)
        expect(String.new(result)).to eq("*3\r\n:1\r\n$3\r\nfoo\r\n+OK\r\n")
      end

      it "serializes nested array" do
        inner1 = [1_i64, 2_i64] of Redis::RespValue
        inner2 = [3_i64, 4_i64] of Redis::RespValue
        arr = [inner1, inner2] of Redis::RespValue
        result = Redis::RespSerializer.serialize(arr)
        expect(String.new(result)).to eq("*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n")
      end
    end

    context "booleans" do
      it "serializes true" do
        result = Redis::RespSerializer.serialize(true)
        expect(String.new(result)).to eq("#t\r\n")
      end

      it "serializes false" do
        result = Redis::RespSerializer.serialize(false)
        expect(String.new(result)).to eq("#f\r\n")
      end
    end

    context "doubles" do
      it "serializes positive double" do
        result = Redis::RespSerializer.serialize(3.14)
        expect(String.new(result)).to eq(",3.14\r\n")
      end

      it "serializes negative double" do
        result = Redis::RespSerializer.serialize(-2.5)
        expect(String.new(result)).to eq(",-2.5\r\n")
      end

      it "serializes infinity" do
        result = Redis::RespSerializer.serialize(Float64::INFINITY)
        expect(String.new(result)).to eq(",inf\r\n")
      end

      it "serializes negative infinity" do
        result = Redis::RespSerializer.serialize(-Float64::INFINITY)
        expect(String.new(result)).to eq(",-inf\r\n")
      end

      it "serializes nan" do
        result = Redis::RespSerializer.serialize(Float64::NAN)
        expect(String.new(result)).to eq(",nan\r\n")
      end
    end

    context "nil" do
      it "serializes nil as null bulk string" do
        result = Redis::RespSerializer.serialize(nil)
        expect(String.new(result)).to eq("$-1\r\n")
      end
    end

    context "maps" do
      it "serializes empty map" do
        hash = Hash(Redis::RespValue, Redis::RespValue).new
        result = Redis::RespSerializer.serialize(hash)
        # Map format: %{count}\r\n
        expect(String.new(result)).to start_with("%0")
      end

      it "serializes map with values" do
        hash = Hash(Redis::RespValue, Redis::RespValue).new
        hash["key1"] = 1_i64
        result = Redis::RespSerializer.serialize(hash)
        expect(String.new(result)).to start_with("%1")
        expect(String.new(result)).to contain("+key1")
        expect(String.new(result)).to contain(":1")
      end
    end

    context "sets" do
      it "serializes empty set" do
        set = Set(Redis::RespValue).new
        result = Redis::RespSerializer.serialize(set)
        expect(String.new(result)).to start_with("~0")
      end

      it "serializes set with values" do
        set = Set(Redis::RespValue).new
        set.add(1_i64)
        set.add(2_i64)
        result = Redis::RespSerializer.serialize(set)
        expect(String.new(result)).to start_with("~2")
      end
    end
  end

  describe "round trip" do
    it "round trips simple string" do
      original = "Hello World"
      serialized = Redis::RespSerializer.serialize(original)
      parser = Redis::RespParser.new(IO::Memory.new(serialized))
      expect(parser.parse).to eq(original)
    end

    it "round trips integer" do
      original = 42_i64
      serialized = Redis::RespSerializer.serialize(original)
      parser = Redis::RespParser.new(IO::Memory.new(serialized))
      expect(parser.parse).to eq(original)
    end

    it "round trips bulk string" do
      original = b("hello")
      serialized = Redis::RespSerializer.serialize(original)
      parser = Redis::RespParser.new(IO::Memory.new(serialized))
      result = parser.parse
      expect(result).to be_a(Bytes)
      expect(result.as(Bytes)).to eq(original)
    end

    it "round trips array" do
      original = [1_i64, 2_i64, 3_i64] of Redis::RespValue
      serialized = Redis::RespSerializer.serialize(original)
      parser = Redis::RespParser.new(IO::Memory.new(serialized))
      result = parser.parse
      expect(result).to eq(original)
    end

    it "round trips boolean" do
      serialized = Redis::RespSerializer.serialize(true)
      parser = Redis::RespParser.new(IO::Memory.new(serialized))
      expect(parser.parse).to eq(true)
    end

    it "round trips double" do
      original = 3.141_59
      serialized = Redis::RespSerializer.serialize(original)
      parser = Redis::RespParser.new(IO::Memory.new(serialized))
      result = parser.parse
      expect(result).to be_a(Float64)
      expect(result.as(Float64)).to be_close(original, 0.000_01)
    end

    it "round trips nil" do
      serialized = Redis::RespSerializer.serialize(nil)
      parser = Redis::RespParser.new(IO::Memory.new(serialized))
      expect(parser.parse).to be_nil
    end
  end
end
