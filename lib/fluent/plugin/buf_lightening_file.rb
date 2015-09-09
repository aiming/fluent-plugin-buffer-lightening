require 'fluent/plugin/buf_file'

module Fluent
  class LighteningFileBufferChunk < FileBufferChunk
    attr_reader :record_counter

    def initialize(key, path, unique_id, mode="a+", symlink_path = nil)
      super
      @record_counter = 0
    end

    def <<(data)
      super
      @record_counter += 1
    end
  end

  class LighteningFileBuffer < FileBuffer
    Fluent::Plugin.register_buffer('lightening_file', self)

    config_param :buffer_chunk_records_limit, :integer, :default => nil

    def configure(conf)
      super
    end

    def new_chunk(key)
      encoded_key = encode_key(key)
      path, tsuffix = make_path(encoded_key, "b")
      unique_id = tsuffix_to_unique_id(tsuffix)
      LighteningFileBufferChunk.new(key, path, unique_id, "a+", @symlink_path)
    end

    def resume
      maps = []
      queues = []

      Dir.glob("#{@buffer_path_prefix}*#{@buffer_path_suffix}") {|path|
        identifier_part = chunk_identifier_in_path(path)
        if m = PATH_MATCH.match(identifier_part)
          key = decode_key(m[1])
          bq = m[2]
          tsuffix = m[3]
          timestamp = m[3].to_i(16)
          unique_id = tsuffix_to_unique_id(tsuffix)

          if bq == 'b'
            chunk = LighteningFileBufferChunk.new(key, path, unique_id, "a+")
            maps << [timestamp, chunk]
          elsif bq == 'q'
            chunk = LighteningFileBufferChunk.new(key, path, unique_id, "r")
            queues << [timestamp, chunk]
          end
        end
      }

      map = {}
      maps.sort_by {|(timestamp,chunk)|
        timestamp
      }.each {|(timestamp,chunk)|
        map[chunk.key] = chunk
      }

      queue = queues.sort_by {|(timestamp,chunk)|
        timestamp
      }.map {|(timestamp,chunk)|
        chunk
      }

      return queue, map
    end

    def chunk_identifier_in_path(path)
      pos_after_prefix = @buffer_path_prefix.length
      pos_before_suffix = @buffer_path_suffix.length + 1 # from tail of path

      path.slice(pos_after_prefix..-pos_before_suffix)
    end

    def storable?(chunk, data)
      return false if chunk.size + data.bytesize > @buffer_chunk_limit
      return false if @buffer_chunk_records_limit && chunk.record_counter >= @buffer_chunk_records_limit
      true
    end
  end
end
