require 'active_record'

module ActiveRecordExt
end

require_relative 'active_record_ext/arrow_result'
require_relative 'active_record_ext/calculations_extension'

ActiveRecord::Relation.include ActiveRecordExt::CalculationsExtension
