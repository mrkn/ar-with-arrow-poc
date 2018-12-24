require 'mysql2-arrow'
require 'record_batch_ext'

module ActiveRecordExt
  class ArrowResult < ActiveRecord::Result
    def initialize(record_batch)
      @record_batch = record_batch
      @columns = nil
      @column_types = {}
      @rows = nil
      @hash_rows = nil
    end

    def columns
      @columns ||= generate_columns
    end

    def column_types
      @column_types ||= generate_column_types
    end

    def rows
      @rows ||= generate_rows
    end

    def length
      @record_batch.n_rows
    end

    def each
      if block_given?
        hash_rows.each(&proc)
      else
        hash_rows.to_enum { length }
      end
    end

    def to_hash
      hash_rows
    end

    alias :map! :map
    alias :collect! :map

    def empty?
      @record_batch.empty?
    end

    def to_ary
      hash_rows
    end

    def [](idx)
      hash_rows[idx]
    end

    def first
      return nil if @record_batch.empty?
      Hash[columns.zip(rows.first)] # TODO
    end

    def last
      return nil if @record_batch.empty?
      Hash[columns.zip(rows.last)] # TODO
    end

    def cast_values(type_overrides = {})
      if type_overrides.empty? || 
           (column_types == column_types.merge(type_overrides))
        result = rows
        columns.one? ? result.map!(:first) : result
      else
        super
      end
    end

    private

      def hash_rows
        @hash_rows ||=
          begin
            columns = self.columns.map { |c| c.dup.freeze }
            rows.map do |row|
              {}.tap do |hash|
                index = 0
                length = columns.length

                while index < length
                  hash[columns[index]] = row[index]
                  index += 1
                end
              end
            end
          end
      end

      def generate_columns
        @record_batch.schema.fields.map do |field|
          field.name
        end
      end

      def generate_rows
        @record_batch.to_a
      end
  end
end
