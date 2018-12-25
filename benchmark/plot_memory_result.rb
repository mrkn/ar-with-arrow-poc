require 'matplotlib/pyplot'
require 'pandas'

input = ARGV[0] || 'memory.log'
records = []

open(input) do |f|
  limit = nil
  comparison = false
  while line = f.gets
    line.chomp!
    line.strip!
    next if line.empty?

    case line
    when /\A=+\s+LIMIT=(\d+)\s+=+\z/
      limit = Integer($1)
      comparison = false

    when /\AComparison:\z/
      comparison = true

    when /\A\s*(\w[\w_]+):\s+(\d+\.\d+)\s+bytes/
      next unless comparison
      method, bytes = $1, Float($2)
      records << [limit, method, bytes]

    when /\A\s*Mysql2Test\.(\w[\w_]+)\(n\):\s+(\d+\.\d+)\s+bytes/
      next unless comparison
      method, bytes = $1, Float($2)
      method = method.end_with?('by_arrow') ? 'by_arrow' : 'original'
      records << [limit, method, bytes]
    end
  end
end

data = Pandas::DataFrame.new(records, columns: %w[limit method size])
data['size'] /= 1e+6 # bytes -> megabytes
puts data

plt = Matplotlib::Pyplot
plt.figure(figsize: [5, 5])
sns = PyCall.import_module('seaborn')
ax = sns.lineplot(x: 'limit', y: 'size', data: data,
                  hue: 'method', style: 'method',
                  markers: true, dashes: false, ci: 68)
ax.set(xlabel: "Batch size [records]", ylabel: "Memory consumption [MB]")
plt.title('Memory consumption comparison')
plt.tight_layout()
plt.savefig('memory.png', dpi: 75)
