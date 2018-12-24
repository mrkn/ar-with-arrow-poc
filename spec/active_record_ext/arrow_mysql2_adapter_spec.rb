require 'spec_helper'
require 'active_record_ext/arrow_mysql2_adapter'

RSpec.describe ActiveRecord::Base do
  describe '.arrow_mysql2_connection' do
    specify do
      expect(ActiveRecord::Base).to receive(:arrow_mysql2_connection).and_call_original
      ActiveRecord::Base.establish_connection(
        host: 'localhost',
        username: 'root',
        database: 'test',
        adapter: 'arrow_mysql2'
      )
      conn = ActiveRecord::Base.connection
      expect(conn).to be_kind_of(ActiveRecordExt::ArrowMysql2Adapter)
    end
  end
end

RSpec.describe ActiveRecordExt::ArrowMysql2Adapter do
  subject(:conn) do
    ActiveRecord::Base.establish_connection(
      host: 'localhost',
      username: 'root',
      database: 'test',
      adapter: 'arrow_mysql2'
    )
    ActiveRecord::Base.connection
  end

  let(:query_limit) { 10 }

  let(:query_columns) { %i[int_test double_test varchar_test text_test] }

  let(:query_stmt) do
    <<~SQL
      SELECT
        #{query_columns.join(', ')}
      FROM mysql2_test LIMIT #{query_limit}
    SQL
  end

  describe '.select_all_by_arrow' do
    specify do
      result = conn.select_all_by_arrow(query_stmt)
      expect(result).to be_kind_of(ActiveRecordExt::ArrowResult)
    end
  end
end
