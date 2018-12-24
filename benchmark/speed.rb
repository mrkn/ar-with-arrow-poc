require_relative 'prelude'

# init
warmup = Integer(ENV.fetch('WARMUP', '0'))
iterations = Integer(ENV.fetch('ITERS', '100'))

# warm-up
if warmup > 0
  i = 1
  while i < warmup
    print "warmup: #{i}/#{warmup}\r"
    i += 1
  end
  puts
end

def benchmark(iterations, limit = 10_000)
  # pluck_by_arrow
  i = 0
  time_arrow = 0.0
  while i <= iterations
    before = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    Mysql2Test.test_pluck_by_arrow(limit)
    after = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  
    time_arrow += after - before
    print "pluck_by_arrow(#{limit}): #{i}/#{iterations}\r"
  
    i += 1
  end
  puts

  i = 0
  time_ar = 0.0
  while i <= iterations
    before = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    Mysql2Test.test_pluck(limit)
    after = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  
    time_ar += after - before
    print "pluck(#{limit}): #{i}/#{iterations}\r"
  
    i += 1
  end
  puts

  return time_arrow, time_ar
end

limits = [
   1_000,
   2_000,
   3_000,
   5_000,
  10_000,
  20_000,
  30_000,
  50_000,
]

results = []
limits.each do |limit|
  time_arrow, time_ar = benchmark(iterations, limit)

  arrow_ips = iterations.to_f / time_arrow
  ar_ips = iterations.to_f / time_ar
  ratio = arrow_ips / ar_ips
  results << [limit, arrow_ips, ar_ips, ratio]

  # report
  puts "Arrow:        #{"%.3f" % arrow_ips} ips (#{"%.3f" % ratio}x #{ratio < 1 ? "slower" : "faster"})"
  puts "ActiveRecord: #{"%.3f" % ar_ips} ips"
end

# Header
puts "limit\tarrow_ips\tar_ips\tarrow/ar"
results.each do |limit, arrow_ips, ar_ips, ratio|
  puts "#{limit}\t#{"%.3f" % arrow_ips}\t#{"%.3f" % ar_ips}\t#{"%.3f" % ratio}"
end
