module ActiveRecordExt
  module CalculationsExtension
    def pluck_by_arrow(*column_names)
      if loaded? && (column_names.map(&:to_s) - @klass.attribute_names - @klass.attribute_aliases.keys).empty?
        return records.pluck(*column_names)
      end

      if has_include?(column_names.first)
        relation = apply_join_dependency
        relation.pluck_by_arrow(*column_names)
      else
        enforce_raw_sql_whitelist(column_names)
        relation = spawn
        relation.select_values = column_names.map { |cn|
          @klass.has_attribute?(cn) || @klass.attribute_alias?(cn) ? arel_attribute(cn) : cn
        }
        result = skip_query_cache_if_necessary {
          klass.connection.select_all_by_arrow(relation.arel, nil)
        }
        result.cast_values(klass.attribute_types)
      end
    end
  end
end
