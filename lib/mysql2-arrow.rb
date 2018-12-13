require "mysql2"
require "arrow"
require "mysql2_arrow.so"

Mysql2::Result.include Mysql2Arrow::ResultExtension
