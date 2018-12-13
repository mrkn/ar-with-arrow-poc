require 'spec_helper'
require 'mysql2-arrow'

RSpec.describe Mysql2::Result do
  describe '.to_arrow' do
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
        FROM mysql2_test LIMIT 10000
      SQL
    end

    specify 'to_arrow' do
      s = Time.now
      record_batch = result.to_arrow
      puts "#{Time.now - s} [sec]"
      expect(record_batch.n_rows).to eq(10_000)
    end

    specify 'to_a' do
      s = Time.now
      rows = result.to_a
      puts "#{Time.now - s} [sec]"
      expect(rows.length).to eq(10_000)
    end
  end
end
