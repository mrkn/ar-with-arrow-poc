namespace :db do
  task :setup do
    require_relative 'db'
    Mysql2Arrow::Task::DB.setup
  end
end
