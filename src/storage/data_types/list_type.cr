module Redis
  class ListType
    @data : Deque(Bytes)

    def initialize
      @data = Deque(Bytes).new
    end

    def lpush(values : Array(Bytes)) : Int64
      values.reverse_each { |val| @data.unshift(val) }
      @data.size.to_i64
    end

    def rpush(values : Array(Bytes)) : Int64
      values.each { |val| @data.push(val) }
      @data.size.to_i64
    end

    def lpop(count : Int32 = 1) : Array(Bytes)
      result = [] of Bytes
      count.times do
        break if @data.empty?
        result << @data.shift
      end
      result
    end

    def rpop(count : Int32 = 1) : Array(Bytes)
      result = [] of Bytes
      count.times do
        break if @data.empty?
        result << @data.pop
      end
      result
    end

    def llen : Int64
      @data.size.to_i64
    end

    def lindex(index : Int64) : Bytes?
      normalized = normalize_index(index)
      return nil if normalized < 0 || normalized >= @data.size
      @data[normalized]
    end

    def lset(index : Int64, value : Bytes) : Bool
      normalized = normalize_index(index)
      return false if normalized < 0 || normalized >= @data.size
      @data[normalized] = value
      true
    end

    def lrange(start_idx : Int64, end_idx : Int64) : Array(Bytes)
      len = @data.size.to_i64
      start_norm = normalize_range_index(start_idx, len)
      end_norm = normalize_range_index(end_idx, len)

      return [] of Bytes if start_norm > end_norm || start_norm >= len

      end_norm = len - 1 if end_norm >= len

      result = Array(Bytes).new((end_norm - start_norm + 1).to_i)
      (start_norm.to_i..end_norm.to_i).each do |idx|
        result << @data[idx]
      end
      result
    end

    def linsert(pivot : Bytes, value : Bytes, before : Bool) : Int64
      @data.each_with_index do |elem, idx|
        if elem == pivot
          if before
            @data.insert(idx, value)
          else
            @data.insert(idx + 1, value)
          end
          return @data.size.to_i64
        end
      end
      -1_i64
    end

    def lpos(element : Bytes, rank : Int64 = 1, count : Int64 = 1, maxlen : Int64 = 0) : Array(Int64)
      result = [] of Int64
      matches = 0_i64
      max = maxlen > 0 ? Math.min(maxlen.to_i, @data.size) : @data.size

      if rank > 0
        (0...max).each do |idx|
          if @data[idx] == element
            matches += 1
            if matches >= rank
              result << idx.to_i64
              break if count > 0 && result.size.to_i64 >= count
            end
          end
        end
      else
        start_idx = @data.size - 1
        end_idx = maxlen > 0 ? Math.max(0, @data.size - maxlen.to_i) : 0
        (start_idx).downto(end_idx) do |idx|
          if @data[idx] == element
            matches += 1
            if matches >= rank.abs
              result << idx.to_i64
              break if count > 0 && result.size.to_i64 >= count
            end
          end
        end
      end
      result
    end

    def lrem(count : Int64, element : Bytes) : Int64
      removed = 0_i64

      if count == 0
        @data.reject! { |elem| elem == element && (removed += 1; true) }
      elsif count > 0
        @data.reject! do |elem|
          if elem == element && removed < count
            removed += 1
            true
          else
            false
          end
        end
      else
        indices_to_remove = [] of Int32
        abs_count = count.abs
        (@data.size - 1).downto(0) do |idx|
          if @data[idx] == element && removed < abs_count
            indices_to_remove << idx
            removed += 1
          end
        end
        indices_to_remove.each { |idx| @data.delete_at(idx) }
      end
      removed
    end

    def ltrim(start_idx : Int64, end_idx : Int64) : Nil
      len = @data.size.to_i64
      start_norm = normalize_range_index(start_idx, len)
      end_norm = normalize_range_index(end_idx, len)

      if start_norm > end_norm || start_norm >= len
        @data.clear
        return
      end

      end_norm = len - 1 if end_norm >= len

      new_data = Deque(Bytes).new
      (start_norm.to_i..end_norm.to_i).each do |idx|
        new_data.push(@data[idx])
      end
      @data = new_data
    end

    def lmove(destination : ListType, wherefrom : Symbol, whereto : Symbol) : Bytes?
      element = case wherefrom
                when :left  then @data.shift?
                when :right then @data.pop?
                else             nil
                end

      return nil unless element

      case whereto
      when :left  then destination.lpush([element])
      when :right then destination.rpush([element])
      end

      element
    end

    def empty? : Bool
      @data.empty?
    end

    def size : Int32
      @data.size
    end

    private def normalize_index(idx : Int64) : Int32
      if idx < 0
        (@data.size + idx.to_i).clamp(0, @data.size - 1)
      else
        idx.to_i
      end
    end

    private def normalize_range_index(idx : Int64, len : Int64) : Int64
      if idx < 0
        normalized = len + idx
        normalized < 0 ? 0_i64 : normalized
      else
        idx
      end
    end
  end
end
