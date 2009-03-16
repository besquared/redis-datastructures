#
# Experimental
#  Needs to be sped up by an order of magnitude to be feasible
#
module IndexedTable
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
    
      def indices(*args)
        if args.blank?
          @indices ||= []
        else
          @indices = args
        end
      end
      
      def storage
        @storage ||= Redis.new
      end
      
      #
      # Insert rows
      #
      def <<(row)
        indices.each do |field|        
          storage.set_add(
            key(field, row[@fields.index(field)]), Marshal.dump(row)
          )
        end
      end
      
      #
      # Search rows
      #  {:field_name => 'value', :other_field => 'other_value'}
      #
      def find(query = {})
        keys = []
        query.keys.each do |field|
          if query[field].is_a?(Array)
            query[field].each do |value|
              keys << key(field, value)
            end
          else
            keys << key(field, query[field])
          end
        end
        rows = storage.set_intersect(*keys)
        rows.collect{|row| Marshal.load(row)} unless rows.nil?
      end
    protected
      #
      # Key construction (index storage strategy)
      #
      def key(field, value)
        "#{@name}:#{field}:#{value}"
      end    
    end
  end
end