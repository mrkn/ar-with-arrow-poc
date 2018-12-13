require 'mkmf-gnome2'

unless required_pkg_config_package("arrow")
  exit(false)
end

unless required_pkg_config_package("arrow-glib")
  exit(false)
end

[
  ["glib2", "ext/glib2"],
].each do |name, source_dir|
  spec = find_gem_spec(name)
  source_dir = File.join(spec.full_gem_path, source_dir)
  build_dir = source_dir
  add_depend_package_path(name, source_dir, build_dir)
end

mysql_includedir, mysql_libdir = dir_config('mysql')
unless mysql_includedir && mysql_libdir
  mysql_config = with_config('mysql-config')
  mysql_config = 'mysql_config' if mysql_config.nil? || mysql_config == true
  mysql_incflags = `#{mysql_config} --include`.chomp
  mysql_libs = `#{mysql_config} --libs_r`.chomp
  $INCFLAGS += " #{mysql_incflags}"
  $libs = "#{mysql_libs} #{$libs}"
end

unless have_header('mysql.h') || have_header('mysql/mysql.h')
  $stderr.puts "Unable to find mysql.h"
  abort
end

checking_for(checking_message("mysql2"), "%s") do
  mysql2_spec = Gem::Specification.find_by_name("mysql2")
  $INCFLAGS += " -I#{mysql2_spec.gem_dir}/ext"
  mysql2_spec.version
end

$CXXFLAGS += ' -std=c++11 -Wno-deprecated-register'

create_makefile('mysql2_arrow')
