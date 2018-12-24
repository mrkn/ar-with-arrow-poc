require 'active_record'
require 'active_record/connection_adapters/mysql2_adapter'
require 'active_record_ext/arrow_result'

module ActiveRecord
  module ConnectionHandling
    def arrow_mysql2_connection(config)
      config = config.symbolize_keys
      config[:flags] ||= 0

      if config[:flags].kind_of? Array
        config[:flags].push "FOUND_ROWS".freeze
      else
        config[:flags] |= Mysql2::Client::FOUND_ROWS
      end

      client = Mysql2::Client.new(config)
      ActiveRecordExt::ArrowMysql2Adapter.new(client, logger, nil, config)
    rescue Mysql2::Error => error
      if error.message.include?("Unknown database")
        raise ActiveRecord::NoDatabaseError
      else
        raise
      end
    end
  end
end

module ActiveRecordExt
  class ArrowMysql2Adapter < ActiveRecord::ConnectionAdapters::Mysql2Adapter
    def initialize(*args, **kwargs)
      super
      @arrow_result = false
    end

    def exec_query(sql, name = "SQL", binds = [], prepare: false)
      return super unless @arrow_result
      if without_prepared_statement?(binds)
        execute_and_free(sql, name) do |result|
          ArrowResult.new(result.to_arrow) if result
        end
      else
        exec_stmt_and_free(sql, name, binds, cache_stmt: prepare) do |_, result|
          ArrowResult.new(result.to_arrow) if result
        end
      end
    end

    def select_all_by_arrow(arel, name = nil, binds = [], preparable: nil)
      with_arrow_result do
        select_all(arel, name, binds, preparable: preparable)
      end
    end

    private

    def with_arrow_result
      begin
        old_value, @arrow_result = @arrow_result, true
        yield
      ensure
        @arrow_result = old_value
      end
    end
  end
end
