module PartitionedTable
  class Base  
    class << self
      def name(value = "")
        if value.blank?
          @name ||= ""
        else
          @name = value
        end
      end
    
      def fields(*args)
        if args.blank?
          @fields ||= []
        else
          @fields = args
        end
      end
    
      def partitions(*args)
        if args.blank?
          @partitions ||= []
        else
          @partitions = args
        end
      end
      
      def caching(value = "")
        if value.blank?
          @caching ||= :local
        else
          @caching = value
        end
      end
      
      def storage
        @storage ||= Redis.new
      end
      
      def cache
        @cache ||= caching == :local ? {} : @storage
      end
      
      #
      # Insert rows
      #
      def load(rows)
        buffer = {}
        rows.each do |row|
          parts = partitions.collect{|p| row[fields.index(p)]}
          part_key = key(parts)
          buffer[part_key] ||= []
          buffer[part_key] << row
        end
        buffer.keys.each do |part_key|
          storage.push_tail(part_key, buffer[part_key])
        end
      end
      def <<(row); load([row]); end

      #
      # Search rows
      #  {:field_name => 'value', :other_field => 'other_value'}
      #
      def find(partitioned = {})
        cached(glob_key(partitioned)) || nil
      end
      
      #
      # Enumerate rows
      #
      def each(partitioned = {})
        find(partitioned).each do |row|
          yield row
        end
      end
      
      #
      # Query caching
      # 
      def cached(key)
        values = cache_get(key)
        returning values do
          cache_set(key, values)
        end
      end

      def cache_get(key)
        cached = cache[cache_key(key)]
        cached.blank? ? retrieve(key) : cached
      end

      def cache_set(key, value)
        return if value.blank?
        cache[cache_key(key)] = value
      end

      def cache_key(key)
        "cache:#{key}"
      end

    protected
      #
      # Key construction (partitioned storage strategy)
      #
      def key(values)
        "#{name}:#{values.join(':')}"
      end

      def glob_key(partitioned)
        "#{name}:#{partitions.collect{|p| partitioned[p] || '*'}.join(':')}"
      end
      
      #
      # Fetch rows
      #
      def retrieve(glob)
        values = []
        storage.keys(glob).each do |key|
          storage.list_range(key, 0, -1).collect do |list|
            values.concat(list)
          end
        end
        values
      end
    end
  end
end