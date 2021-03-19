module CompositePrimaryKeys
  module ActiveRecord
    module Calculations
      def build_count_subquery(relation, column_name, distinct)
        if column_name == :all
          column_alias = Arel.star
          # CPK
          # relation.select_values = [ Arel.sql(FinderMethods::ONE_AS_ONE) ] unless distinct
          relation.select_values = [ Arel.sql(::ActiveRecord::FinderMethods::ONE_AS_ONE) ] unless distinct
        elsif column_name.is_a?(Array)
          column_alias = Arel.star
          relation.select_values = column_name.map do |column|
            Arel::Attribute.new(@klass.unscoped.table, column)
          end
        else
          column_alias = Arel.sql("count_column")
          relation.select_values = [ aggregate_column(column_name).as(column_alias) ]
        end

        subquery_alias = Arel.sql("subquery_for_count")
        select_value = operation_over_aggregate_column(column_alias, "count", false)

        relation.build_subquery(subquery_alias, select_value)
      end

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
