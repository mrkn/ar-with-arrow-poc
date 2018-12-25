require 'matplotlib/pyplot'
require 'pandas'

input = ARGV[0] || 'speed.log'
records = []

open(input) do |f|
  limit = nil
  calculating = false
  while line = f.gets
    line.chomp!
    line.strip!
    next if line.empty?

    case line
    when /\A=+\s+LIMIT=(\d+)\s+=+\z/
      limit = Integer($1)
      calculating = false

    when /\ACalculating -+\z/
      calculating = true

    when /\A\s*(?<method>\w[\w_]+)\s+(?<duration>\d+\.\d+)\s+s\s+-\s+(?<n>\d+\.\d+)\s+times in\s+\S+\s+\((?<duration_ms>\d+\.\d+)ms\/i\)/
      next unless calculating
      md = Regexp.last_match
      records << [limit, md[:method], Float(md[:duration_ms])]
    end
  end
end

data = Pandas::DataFrame.new(records, columns: %w[limit method duration])
puts data

plt = Matplotlib::Pyplot
plt.figure(figsize: [5, 5])
sns = PyCall.import_module('seaborn')
ax = sns.lineplot(x: 'limit', y: 'duration', data: data,
                  hue: 'method', style: 'method',
                  markers: true, dashes: false, ci: 68)
ax.set(xlabel: "Batch size [records]", ylabel: "Computation time per iteration [ms]")
plt.title('Computation time comparison')
plt.tight_layout()
plt.savefig('speed.png', dpi: 75)
