# Reducing memory footprint of `pluck` method in ActiveRecord using Apache Arrow

- Author: Kenta Murata at Speee, Inc.
- Date: 2018-12-25

I'm not English-native.  If you find incorrect English usage in this article, please give me your feedback as issues.

## tl;dr

- The proof-of-concept optimization of AR's `pluck` method is demonstrated
- Apache Arrow's RecordBatch type is used as the internal representation of AR::Result
- The memory consumption is tremendously reduced when compared with that of the original implementation
- The computation time is slightly reduced compared with that of the original implementation

## Background

### `pluck` method of ActiveRecord

ActiveRecord provides the `pluck` method _a platform_ to obtain one or few field values as an array or arrays from a table.  The `pluck` method, compared with finder methods, can considerably reduce memory consumption because it does not generate model instances.

The example usage of the `pluck` method is given below:

```ruby
User.where(admin: false).limit(10).pluck(:id, :name)
```

This code obtains arrays of pairs of ID and name fields of the first 10 non-admin users.  The result obtained is similar to the below array:

```ruby
[[3, "Felton Lakin"], [4, "Harriette Metz"], [6, "Ronnie Wisozk"], ...]
```

This result can be obtained using the finder methods and chaining `map` method similar to the following code:

```ruby
User.where(admin: false).limit(10).map {|u| [u.id, u.name] }
```

However, this consumes more memory and computation time than the `pluck` version. The reason is this generates instances of User class for each row of the result. Moreover, this takes all the columns in the `users` table from the DB for generating the instances even though the result needs only `id` and `name`.

Reducing the number of taking columns can be achieved using the `select` method:

```ruby
User.where(admin: false).select(:id, :name).limit(10).map {|u| [u.id, u.name] }
```

However, this is not as memory efficient as the `pluck` version.  This means the `pluck` method is very important in Rails application to reduce memory consumption.

### RecordBatch of Apache Arrow

`RecordBatch` class is the core of Apache Arrow's data structure for representing table-like data.  A `RecordBatch` object consists of a `Schema` object and a vector of `Array` object.  `Schema` class is used for representing a table schema.  Its object consists of a vector of `Field` class, which consists of the name and data type of the field.  `Array` class is used for representing a column of the table.  It has the number of rows, number of null values, and actual values for each cell of the column.

`RecordBatch` class represents a table-like data as the column-major format.  Because MySQL's client library (libmysqlclient) provides access that is limited to APIs in row-major format, we need to convert the data from row-major into column-major to produce `RecordBatch` objects from MySQL's query result.

## Method

### Optimization strategy

In the original pluck method, the query result that comes from the connection adapter is converted to two arrays for generating `ActiveRecord::Result` object.  One is the array of fields, that consists of the pairs of the column name and data type; the other is the array of rows.

The optimized version cancels this array generation by using `RecordBatch` object of Apache Arrow.

### Generating RecordBatch from a query result

For optimization, a `RecordBatch` object needs to be generated from a query result that comes from the connection adapter.  For that purpose, the `to_arrow` method is provided as an instance method of `Mysql2::Result` class.  The implementation of `to_arrow` method is written in C++, and it is incomplete, but minimal for the experiments performed below.

The supported RDBMS in this repository is only MySQL, but the other RDBMS such as PostgreSQL can be supported in theory.

### Converting RecordBatch to Array of records

To generate the result of the optimized version of `pluck` method, the conversion of a `RecordBatch` object to Ruby's array is necessary.  `Arrow::RecordBatch` class is provided by [red-arrow gem](https://github.com/apache/arrow/tree/master/ruby), and this class has `to_a` method.  However, this `to_a` method is very slow because it is actually `Enumerable` class's method and uses `each` method.

To avoid the use of `each` method, the special version of `to_a` method written in C++ is supplied.  The implementation of `to_a` method is incomplete but it has enough functionalities for performing the experiments described below as `to_arrow` described above.

## Experiments

The following two experiments will be performed here:

1. Memory consumption comparison
2. Computation time comparison

All the measurements are performed by the `benchmark-driver` command provided by [benchmark\_driver gem](https://github.com/benchmark-driver/benchmark-driver).

### Experimental condition

#### Execution environment

All the experiments are performed on the author's MacBook Pro (15-inch, 2016), which consists of 2.9 GHz Core i7 processor and 16 GB memory.

The prerequisits of all the experiments are listed below:

- Ruby 2.3.5
- MySQL 8.0.12
- Apache Arrow 0.11.1
- Apache Arrow GLIB 0.11.1
- Gem libraries listed in [Gemfile.lock](../Gemfile.lock)

#### benchmark-driver's setting

For each experiment, the following YAML data is used for the setting of benchmark-driver, which is stored as [driver.yml](../benchmark/driver.yml).

```yaml
prelude: |
  $LOAD_PATH.unshift Dir.pwd
  require 'prelude'
  n = Integer(ENV.fetch('LIMIT', '10000'))

benchmark:
  by_arrow: Mysql2Test.test_pluck_by_arrow(n)
  original: Mysql2Test.test_pluck(n)

loop_count: 100
```

### Memory consumption comparison experiment

Memory consumption is measured by the `memory` runner of the benchmark-driver.  This runner uses the maximum RSS value reported by `time` command with the `-l` option on macOS that reports the rusage structure (See getrusage(3)).

The benchmark can be executed using the following command line.

```
bundle exec benchmark-driver --rbenv '2.5.3' --bundler -r memory driver.yml
```

In the actual experiment, the following script (saved as [memory\_runner.sh](../benchmark/memory_runner.sh)) is used to observe the trends for the changes of the batch size between 1,000 and 50,000.

```sh
for i in 1000 2000 3000 5000 10000 20000 30000 50000; do
  echo ===== LIMIT=$i =====
  LIMIT=$i bundle exec benchmark-driver --rbenv '2.5.3' --bundler -r memory driver.yml
done
```

#### Result

The raw result is saved as [memory.log](../benchmark/memory.log) in the repository.  The following figure is drawn by the [plot\_memory\_result.rb](../benchmark/plot_memory_result.rb) script.

![](memory.png)

In this figure, the curve with circle marks, with the legend `by_arrow`, illustrates the trend of the Apache Arrow version of the `pluck` method.  On the other hand, the curve with cross marks, with the legend `original`, illustrates the trend of the original `pluck` method.

The figure clearly shows that the Apache Arrow version tremendously reduces the memory consumption.  The following table shows the average memory consumption for each pair of the batch size and method.  The rightmost column in the table shows the efficiency of the memory consumption of the Arrow version compared with the original version.  The efficiency of the Arrow version is higher than 2x for all the cases, and the most efficient case is higher than 12x.

**The average memory consumption**

| limit | by\_arrow [MB] | original [MB] | ratio |
| ----- | ----------:| -----------:| -----:|
| 1000  |  65.739162 |  155.206451 | 2.361x |
| 2000  |  74.008166 |  254.853120 | 3.444x |
| 3000  |  86.625485 |  341.478605 | 3.942x |
| 5000  | 111.945318 |  537.368986 | 4.800x |
| 10000 | 119.114138 |  976.176742 | 8.195x |
| 20000 | 168.812544 | 1880.996250 | 11.14x |
| 30000 | 226.839757 | 2774.773760 | 12.23x |
| 50000 | 337.079501 | 3618.711962 | 10.74x |

### Computation time comparison experiment

Computation time is measured by the `time` runner of the benchmark-driver.  The example command line is given below:

```
bundle exec benchmark-driver --rbenv '2.5.3' --bundler -r time driver.yml
```

In the actual experiment, the following script (saved as [speed\_runner.sh](../benchmark/speed_runner.sh)) is used to observe the trends for the changes of the batch size between 1,000 and 50,000 as the memory experiment above.

```sh
for i in 1000 2000 3000 5000 10000 20000 30000 50000; do
  echo ===== LIMIT=$i =====
  LIMIT=$i bundle exec benchmark-driver --rbenv '2.5.3' --bundler -r time driver.yml
done
```

#### Result

The raw result is saved as [speed.log](../benchmark/speed.log) in the repository.  The following figure is drawn by [plot\_speed\_result.rb](../benchmark/plot_speed_result.rb) script.

![](speed.png)

In this figure, the curve with circle marks, with the legend `by_arrow`, illustrates the trend of the Apache Arrow version of `pluck` method.  On the other hand, the curve with cross marks, with the legend `original`, illustrates the trend of the original `pluck` method.

The figure shows that the Apache Arrow version is not slower than the original version for all the cases.  The difference among the methods increases as the batch size increases.  The cause of the difference is still not investigated.  The author guesses the cause is the garbage collector's load based on the increase in the number of objects in the original version, which is hinted at in the figure of the memory consumption above.  Conclusively, the author does not consider that the optimized version of pluck is faster than the original version even for a large batch size.

## Conclusion

The proof-of-concept optimization of the `pluck` method using Apache Arrow is demonstrated.  The memory and computation time consumptions are compared with the original version of the `pluck` method.  The result verifies that the optimized version is highly and slightly efficient regarding memory and computation time consumption, respectively.

Apache Arrow community intends to add the new protocols in the major RDBMS, such as MySQL and PostgreSQL, that returns the query result in the Arrow format.  We can get rid of the conversion phase---from the query result to a `RecordBatch` object---after such protocols are production ready.  It should reduce both the memory consumption and computation time.
