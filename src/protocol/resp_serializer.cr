module Redis
  class RespSerializer
    CRLF = "\r\n".bytes

    # Pre-allocated static Bytes constants to avoid repeated allocations
    NULL_BULK_STRING = "$-1\r\n".to_slice
    TRUE_BOOLEAN     = "#t\r\n".to_slice
    FALSE_BOOLEAN    = "#f\r\n".to_slice
    POSITIVE_INF     = ",inf\r\n".to_slice
    NEGATIVE_INF     = ",-inf\r\n".to_slice
    NAN_VALUE        = ",nan\r\n".to_slice

    def self.serialize(value : RespValue) : Bytes
      case value
      when String
        serialize_simple_string(value)
      when Int64
        serialize_integer(value)
      when Bytes
        serialize_bulk_string(value)
      when Array(RespValue)
        serialize_array(value)
      when Bool
        serialize_boolean(value)
      when Float64
        serialize_double(value)
      when Nil
        serialize_null
      when Hash(RespValue, RespValue)
        serialize_map(value)
      when Set(RespValue)
        serialize_set(value)
      else
        raise ParseError.new("Cannot serialize value type: #{value.class}")
      end
    end

    private def self.serialize_simple_string(str : String) : Bytes
      ("+" + str + "\r\n").to_slice
    end

    private def self.serialize_error(str : String) : Bytes
      "-#{str}\r\n".to_slice
    end

    private def self.serialize_integer(num : Int64) : Bytes
      (":" + num.to_s + "\r\n").to_slice
    end

    private def self.serialize_bulk_string(bytes : Bytes) : Bytes
      # Pre-allocate with estimated size to reduce reallocations
      estimated_size = 16 + bytes.size # "$" + size digits + CRLF + data + CRLF
      result = IO::Memory.new(estimated_size)
      result << "$#{bytes.size}\r\n"
      result.write(bytes)
      result << "\r\n"
      result.to_slice
    end

    private def self.serialize_array(arr : Array(RespValue)) : Bytes
      # Pre-allocate with estimated size (rough estimation based on average element size)
      estimated_size = 16 + arr.size * 32 # "*" + count + CRLF + avg element size
      result = IO::Memory.new(estimated_size)
      result << "*#{arr.size}\r\n"

      arr.each do |value|
        result.write(serialize(value))
      end

      result.to_slice
    end

    private def self.serialize_null : Bytes
      NULL_BULK_STRING
    end

    private def self.serialize_boolean(b : Bool) : Bytes
      b ? TRUE_BOOLEAN : FALSE_BOOLEAN
    end

    private def self.serialize_double(f : Float64) : Bytes
      case
      when f.infinite? && f > 0
        POSITIVE_INF
      when f.infinite? && f < 0
        NEGATIVE_INF
      when f.nan?
        NAN_VALUE
      else
        ("," + f.to_s + "\r\n").to_slice
      end
    end

    private def self.serialize_map(hash : Hash(RespValue, RespValue)) : Bytes
      # Pre-allocate with estimated size
      estimated_size = 16 + hash.size * 64 # "%" + count + CRLF + avg kv-pair size
      result = IO::Memory.new(estimated_size)
      result << "%#{hash.size}#{CRLF}"

      hash.each do |key, value|
        result.write(serialize(key))
        result.write(serialize(value))
      end

      result.to_slice
    end

    private def self.serialize_set(set : Set(RespValue)) : Bytes
      # Pre-allocate with estimated size
      estimated_size = 16 + set.size * 32 # "~" + count + CRLF + avg element size
      result = IO::Memory.new(estimated_size)
      result << "~#{set.size}#{CRLF}"

      set.each do |value|
        result.write(serialize(value))
      end

      result.to_slice
    end

    private def self.serialize_push(arr : Array(RespValue)) : Bytes
      # Pre-allocate with estimated size
      estimated_size = 16 + arr.size * 32 # ">" + count + CRLF + avg element size
      result = IO::Memory.new(estimated_size)
      result << ">#{arr.size}#{CRLF}"

      arr.each do |value|
        result.write(serialize(value))
      end

      result.to_slice
    end

    private def self.serialize_attribute(hash : Hash(RespValue, RespValue)) : Bytes
      # Pre-allocate with estimated size
      estimated_size = 16 + hash.size * 64 # "|" + count + CRLF + avg kv-pair size
      result = IO::Memory.new(estimated_size)
      result << "|#{hash.size}#{CRLF}"

      hash.each do |key, value|
        result.write(serialize(key))
        result.write(serialize(value))
      end

      result.to_slice
    end

    private def self.serialize_blob_error(str : String) : Bytes
      # Pre-allocate with exact size
      estimated_size = 16 + str.bytesize # "!" + size + CRLF + str + CRLF
      result = IO::Memory.new(estimated_size)
      result << "!#{str.bytesize}\r\n#{str}\r\n"
      result.to_slice
    end

    private def self.serialize_verbatim_string(str : String, format = "txt") : Bytes
      # Pre-allocate with exact size
      estimated_size = 16 + str.bytesize + format.size # "=" + size + CRLF + format + ":" + str + CRLF
      result = IO::Memory.new(estimated_size)
      result << "=#{str.bytesize + 4}\r\n#{format}:#{str}\r\n"
      result.to_slice
    end
  end
end
