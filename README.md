### Usage

``` sh
❯ ./explain.py --help
usage: explain.py [-h] [--host HOST] [--secure] [--password PASSWORD] [--cluster CLUSTER] sql_query

Execute ClickHouse query with profiling and generate an enriched DOT graph. Distributed queries are not really supported.

positional arguments:
  sql_query            SQL query to profile

options:
  -h, --help           show this help message and exit
  --host HOST          ClickHouse server host (default: localhost)
  --secure             Use secure connection to ClickHouse
  --password PASSWORD  ClickHouse server password (default: empty)
  --cluster CLUSTER    ClickHouse cluster name
```

### Example

Command:

``` sh
./explain.py 'select * from numbers_mt(1e6) t1 join numbers_mt(1e5) t2 using(number) settings max_threads=4'
```

Output:

![Example2](./query_heatmap.svg)
