# Benchmark

## Speed benchmark

Speed benchmark can be performed by following command:

```
bundle exec benchmark-driver --rbenv '2.5.3' --bundler -r time driver.yml
```

## Memory benchmark

Memory benchmark can be performed by following command:

```
bundle exec benchmark-driver --rbenv '2.5.3' --bundler -r memory driver.yml
```

## Specifying the batch size

You can specify the batch size by specifying `LIMIT` environment variable.  The default batch size is 2,000.  For example, the following command uses 10,000 rows in a batch.

```
LIMIT=10000 bundle exec benchmark-driver --rbenv '2.5.3' --bundler -r time driver.yml
```
