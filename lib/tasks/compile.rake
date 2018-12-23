require 'rake/extensiontask'

Rake::ExtensionTask.new('record_batch_ext')
Rake::ExtensionTask.new('mysql2_arrow')
Rake::ExtensionTask.new('pg_arrow')
