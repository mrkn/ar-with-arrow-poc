require 'spec_helper'
require 'active_record_ext'

module ActiveRecordExt
  module Testing
    def self.table_name_prefix
      ''
    end

    class Mysql2Test < ActiveRecord::Base
      self.table_name = 'mysql2_test'
    end
  end
end

RSpec.describe ActiveRecordExt::Testing::Mysql2Test do
  before do
    ActiveRecord::Base.establish_connection(
      host: 'localhost',
      username: 'root',
      database: 'test',
      adapter: 'arrow_mysql2'
    )
  end

  let(:model_class) { described_class }

  describe '.pluck_by_arrow' do
    let(:query_columns) { %i[int_test double_test varchar_test text_test] }

    let(:query_limit) { 10 }

    let(:relation) do
      model_class.limit(query_limit)
    end

    specify do
      connection = model_class.connection
      allow(model_class).to receive(:connection).and_return(connection)
      expect(connection).to receive(:select_all_by_arrow).and_call_original

      result = relation.pluck_by_arrow(*query_columns)
      expect(result).to be_kind_of(Array)
      expect(result).to eq(relation.pluck(*query_columns))
    end
  end
end
