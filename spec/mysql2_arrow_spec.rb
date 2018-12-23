require 'spec_helper'
require 'mysql2-arrow'
require 'record_batch_ext'

RSpec.describe Mysql2::Result do
  let(:client) do
    Mysql2::Client.new(host: 'localhost', username: 'root', database: 'test')
  end

  subject(:result) do
    client.query <<~SQL
      SELECT
        tiny_int_test
        , small_int_test
        , medium_int_test
        , int_test
        , big_int_test
        , float_test
        , double_test
        , decimal_test
        , varchar_test
        , binary_test
        , tiny_text_test
        , text_test
        , medium_text_test
        , long_text_test
      FROM mysql2_test LIMIT 30000
    SQL
  end

  describe 'to_a' do
    specify do
      s = Time.now
      rows = result.to_a
      puts "#{Time.now - s} [sec]"
      expect(rows.length).to eq(30_000)
    end
  end

  describe '.to_arrow' do
    specify do
      s = Time.now
      record_batch = result.to_arrow
      puts "#{Time.now - s} [sec]"
      expect(record_batch.n_rows).to eq(30_000)
    end

    specify 'with to_a' do
      s = Time.now
      record_batch = result.to_arrow
      puts "#{Time.now - s} [sec]"
      expect(record_batch.n_rows).to eq(30_000)
      s = Time.now
      ary = record_batch.to_a
      puts "#{Time.now - s} [sec]"
      expect(ary.length).to eq(30_000)
    end
  end
end
