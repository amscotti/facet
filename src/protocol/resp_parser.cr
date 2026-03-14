module Redis
  struct RespError
    getter message : String

    def initialize(@message : String)
    end
  end

  alias RespValue = String | Int64 | Bytes | Array(RespValue) | Bool | Float64 | Nil | Hash(RespValue, RespValue) | Set(RespValue) | RespError

  class ParseError < Exception
  end

  class RespParser
    def initialize(@io : IO)
    end

    def parse : RespValue
      type_byte = @io.read_byte
      return nil unless type_byte

      case type_byte.chr
      when '+'
        parse_simple_string
      when '-'
        parse_error
      when ':'
        parse_integer
      when '$'
        parse_bulk_string
      when '*'
        parse_array
      when '_'
        parse_null
      when '#'
        parse_boolean
      when ','
        parse_double
      when '%'
        parse_map
      when '~'
        parse_set
      when '>'
        parse_push
      when '|'
        parse_attribute
      when '('
        parse_big_number
      when '!'
        parse_blob_error
      when '='
        parse_verbatim_string
      else
        # Inline command support - treat as space-separated arguments
        parse_inline_command(type_byte)
      end
    end

    # Parse inline commands (e.g., "PING\r\n" or "SET key value\r\n")
    # The first byte has already been read, so we need to prepend it
    private def parse_inline_command(first_byte : UInt8) : Array(RespValue)
      # Read the rest of the line
      rest = @io.gets
      return [] of RespValue unless rest

      # Build the full line with the first byte
      line = String.new(Bytes[first_byte]) + rest.chomp

      # Skip empty lines
      return [] of RespValue if line.empty?

      # Split by spaces and convert to array of Bytes
      parts = line.split(' ', remove_empty: true)
      result = Array(RespValue).new(parts.size)
      parts.each do |part|
        result << part.to_slice.dup
      end
      result
    end

    private def read_line : String
      line = @io.gets
      raise ParseError.new("Unexpected EOF") unless line
      line.chomp
    end

    private def parse_simple_string : String
      read_line
    end

    private def parse_error : String
      read_line
    end

    private def parse_integer : Int64
      read_line.to_i64
    end

    private def parse_bulk_string : Bytes?
      length = read_line.to_i64
      return nil if length == -1

      if length < 0
        raise ParseError.new("Invalid bulk string length: #{length}")
      end

      data = Bytes.new(length)
      @io.read_fully(data)
      read_crlf

      data
    end

    private def parse_array : Array(RespValue)?
      count = read_line.to_i64
      return nil if count == -1

      if count < 0
        raise ParseError.new("Invalid array count: #{count}")
      end

      result = Array(RespValue).new(count.to_i)
      count.times { result << parse }
      result
    end

    private def parse_null : Nil
      read_crlf
      nil
    end

    private def read_crlf : Nil
      cr = @io.read_byte
      lf = @io.read_byte

      unless cr == '\r'.ord.to_u8 && lf == '\n'.ord.to_u8
        raise ParseError.new("Expected CRLF")
      end
    end

    private def parse_boolean : Bool
      line = read_line
      case line
      when "t"
        true
      when "f"
        false
      else
        raise ParseError.new("Invalid boolean value: #{line}")
      end
    end

    private def parse_double : Float64
      line = read_line
      case line
      when "inf"
        Float64::INFINITY
      when "-inf"
        -Float64::INFINITY
      when "nan"
        Float64::NAN
      else
        line.to_f64
      end
    end

    private def parse_map : Hash(RespValue, RespValue)
      count = read_line.to_i64
      if count < 0
        raise ParseError.new("Invalid map size: #{count}")
      end

      result = Hash(RespValue, RespValue).new
      count.times do
        key = parse
        value = parse
        result[key] = value
      end
      result
    end

    private def parse_set : Set(RespValue)
      count = read_line.to_i64
      if count < 0
        raise ParseError.new("Invalid set size: #{count}")
      end

      result = Set(RespValue).new
      count.times { result.add(parse) }
      result
    end

    private def parse_push : Array(RespValue)?
      parse_array
    end

    private def parse_attribute : Hash(RespValue, RespValue)
      parse_map
    end

    private def parse_big_number : String
      read_line
    end

    private def parse_blob_error : Bytes?
      parse_bulk_string
    end

    private def parse_verbatim_string : Bytes?
      parse_bulk_string
    end
  end
end
