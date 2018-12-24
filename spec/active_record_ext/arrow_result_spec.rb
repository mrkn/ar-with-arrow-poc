require 'spec_helper'
require 'active_record_ext'

RSpec.describe ActiveRecordExt::ArrowResult do
  let(:mysql2_client) do
    Mysql2::Client.new(host: 'localhost', username: 'root', database: 'test').tap do |client|
      client.query_options[:as] = :array
    end
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

  let(:result_record_batch) do
    mysql2_client.query(query_stmt).to_arrow
  end

  subject(:result) do
    ActiveRecordExt::ArrowResult.new(result_record_batch)
  end

  let(:ar_result) do
    r = mysql2_client.query(query_stmt)
    ActiveRecord::Result.new(r.fields, r.to_a)
  end

  describe '.columns' do
    specify do
      expect(result.columns).to eq(ar_result.columns)
    end
  end

  describe '.column_types' do
    specify do
      expect(result.column_types).to eq(ar_result.column_types)
    end
  end

  describe '.length' do
    specify do
      expect(result.length).to eq(ar_result.length)
    end
  end

  describe '.rows' do
    specify do
      rows = result.rows
      expect(rows).to be_kind_of(Array)
      expect(rows).to be(result.rows)

      expect(rows).to eq(ar_result.rows)
    end
  end

  describe '.cast_values' do
    specify do
      values = result.cast_values
      expect(values).to be_kind_of(Array)
      expect(values).to eq(ar_result.cast_values)
    end
  end
end
