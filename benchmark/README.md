# Benchmark

## Speed benchmark

Speed benchmark can be performed by executing `speed.rb`:

```
bundle exec ruby speed.rb
```

### Result examples

The following result is produced on mrkn's MacBook (Retina, 12-inch, 2017).

| limit | arrow_ips | ar_ips | arrow/ar |
| --- | ---:| ---:| ---:|
| 1000  | 19.715    | 17.552 | 1.123 |
| 2000  | 15.731    | 13.963 | 1.127 |
| 3000  | 9.706     | 7.656  | 1.268 |
| 5000  | 5.850     | 5.133  | 1.140 |
| 10000 | 3.098     | 2.861  | 1.083 |
| 20000 | 1.327     | 1.220  | 1.087 |
| 30000 | 0.622     | 0.627  | 0.991 |
| 50000 | 0.474     | 0.375  | 1.265 |

## Memory benchmark

Memory benchmark can be performed by following command:

```
bundle exec benchmark-driver --rbenv '2.5.3' --bundler -r memory driver.yml
```

You can specify the batch size by specifying `LIMIT` environment variable.  The default batch size is 2,000.  For example, the following command uses 10,000 rows in a batch.

```
LIMIT=10000 bundle exec benchmark-driver --rbenv '2.5.3' --bundler -r memory driver.yml
```
