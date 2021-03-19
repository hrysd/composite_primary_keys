module CompositePrimaryKeys
  module ActiveRecord
    module Calculations
      def calculate(operation, column_name)
        if has_include?(column_name)
          relation = apply_join_dependency

          if operation.to_s.downcase == "count"
            unless distinct_value || distinct_select?(column_name || select_for_count)
              relation.distinct!
              # CPK
              # relation.select_values = [ klass.primary_key || table[Arel.star] ]
              if klass.primary_key.present? && klass.primary_key.is_a?(Array)
                relation.select_values = klass.primary_key.map do |k|
                  "#{connection.quote_table_name(klass.table_name)}.#{connection.quote_column_name(k)}"
                end
              else
                relation.select_values = [ klass.primary_key || table[Arel.star] ]
              end
            end
            # PostgreSQL: ORDER BY expressions must appear in SELECT list when using DISTINCT
            relation.order_values = [] if group_values.empty?
          end

          relation.calculate(operation, column_name)
        else
          perform_calculation(operation, column_name)
        end
      end
    end
  end
end
