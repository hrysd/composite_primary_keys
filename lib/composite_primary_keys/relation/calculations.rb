module CompositePrimaryKeys
  module ActiveRecord
    module Calculations
      def aggregate_column(column_name)
        # CPK
        if column_name.kind_of?(Array)
          column_name.map do |column|
            @klass.arel_attribute(column_name)
          end
        elsif @klass.has_attribute?(column_name) || @klass.attribute_alias?(column_name)
          @klass.arel_attribute(column_name)
        else
          Arel.sql(column_name == :all ? "*" : column_name.to_s)
        end
      end

      def build_count_subquery(relation, column_name, distinct)
        if column_name == :all
          relation.select_values = [ Arel.sql(::ActiveRecord::FinderMethods::ONE_AS_ONE) ] unless distinct
          if relation.select_values.first.is_a?(Array)
            relation.select_values = relation.select_values.first.map do |column|
              Arel::Attribute.new(@klass.unscoped.table, column)
            end
          end
        elsif column_name.is_a?(Array)
          relation.select_values = column_name.map do |column|
            Arel::Attribute.new(@klass.unscoped.table, column)
          end
        else
          column_alias = Arel.sql("count_column")
          relation.select_values = [ aggregate_column(column_name).as(column_alias) ]
        end

        subquery = relation.arel.as(Arel.sql("subquery_for_count"))
        select_value = operation_over_aggregate_column(column_alias || Arel.star, "count", false)

        Arel::SelectManager.new(subquery).project(select_value)
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
            relation.order_values = []
          end

          relation.calculate(operation, column_name)
        else
          perform_calculation(operation, column_name)
        end
      end
    end
  end
end
