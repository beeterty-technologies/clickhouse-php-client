# beeterty/clickhouse-php-client

[![CI](https://github.com/beeterty/clickhouse-php-client/actions/workflows/ci.yml/badge.svg)](https://github.com/beeterty/clickhouse-php-client/actions/workflows/ci.yml)
[![PHP](https://img.shields.io/badge/PHP-8.2%2B-blue)](https://www.php.net)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A lightweight, zero-dependency ClickHouse HTTP client for PHP 8.2+.

- Fluent query builder with ClickHouse-specific clauses (`PREWHERE`)
- Full DDL support via a Blueprint/Grammar pattern — create, alter, drop tables and materialized views
- Multiple wire formats: `JsonEachRow`, `CSVWithNames`, `TabSeparatedWithNames`
- Parallel queries via `curl_multi`
- Memory-efficient file streaming inserts
- Async fire-and-forget execution with `query_id` tracking
- Retry logic and gzip compression built into `Config`
- PHPStan level 8, 262 tests

---

## Requirements

| Requirement | Version |
|---|---|
| PHP | ≥ 8.2 |
| ext-curl | any |
| ext-json | any |
| ClickHouse | any recent version |

---

## Installation

```bash
composer require beeterty/clickhouse-php-client
```

---

## Quick start

```php
use Beeterty\ClickHouse\Client;
use Beeterty\ClickHouse\Config;

$client = new Client(new Config(
    host: '127.0.0.1',
    port: 8123,
    database: 'default',
    username: 'default',
    password: '',
));

$client->ping(); // true

// Insert rows
$client->insert('events', [
    ['id' => 1, 'type' => 'click', 'score' => 42],
    ['id' => 2, 'type' => 'view',  'score' => 10],
]);

// Fluent SELECT
$rows = $client->table('events')
    ->where('type', 'click')
    ->orderByDesc('score')
    ->limit(10)
    ->get()
    ->rows();
```

---

## Configuration

```php
$config = new Config(
    host:           '127.0.0.1',
    port:           8123,
    database:       'default',
    username:       'default',
    password:       '',
    https:          false,
    timeout:        30,       // seconds
    connectTimeout: 5,        // seconds
    retries:        3,        // extra attempts on connection failure
    retryDelay:     200,      // ms between retries
    compression:    true,     // gzip INSERT bodies
);

// Or from an array (e.g. loaded from a config file)
$config = Config::fromArray([
    'host'            => '127.0.0.1',
    'port'            => 8123,
    'database'        => 'analytics',
    'username'        => 'default',
    'password'        => 'secret',
    'https'           => false,
    'timeout'         => 30,
    'connect_timeout' => 5,
    'retries'         => 3,
    'retry_delay'     => 200,
    'compression'     => true,
]);
```

---

## Query builder

Obtain a builder via `$client->table('name')`.

### SELECT clauses

```php
$client->table('events')
    ->select('id', 'type', 'score')      // backtick-quoted automatically
    ->addSelect('created_at')            // append to existing list
    ->selectRaw('count() AS n')          // raw expression, replaces list
    ->addSelectRaw('avg(score) AS avg')  // append raw expression
```

### WHERE

```php
->where('type', 'click')                // = shorthand
->where('score', '>=', 80)             // any operator
->whereRaw('toDate(created_at) = today()')
->whereIn('id', [1, 2, 3])
->whereNotIn('id', [4, 5])
->whereBetween('score', 60, 90)
->whereNull('deleted_at')
->whereNotNull('published_at')
```

### PREWHERE (ClickHouse-specific)

`PREWHERE` is evaluated before `WHERE` and reads only the columns it references, making it highly efficient for filtering on ORDER BY key columns.

```php
->prewhere('event_date', '>=', '2024-01-01')
->prewhere('event_date', $date)          // = shorthand
->prewhereRaw('event_date >= today()')
```

### GROUP BY / HAVING / ORDER BY / LIMIT

```php
->groupBy('type')
->having('count() > 100')
->orderBy('score')                       // ASC by default
->orderBy('score', 'DESC')
->orderByDesc('score')                   // shorthand
->limit(100)
->offset(200)
```

### Terminal methods

```php
// Returns a Statement (all rows)
$statement = $client->table('events')->where('type', 'click')->get();

// First row or null
$row = $client->table('events')->orderBy('id')->first();

// Row count (ignores LIMIT / ORDER BY)
$count = $client->table('events')->where('type', 'click')->count();

// Scalar value from first row, first column
$total = $client->table('events')->selectRaw('count()')->value();

// Flat array of one column
$ids = $client->table('events')->orderBy('id')->pluck('id');

// Paginated iteration — stops when callback returns false
$client->table('events')
    ->orderBy('id')
    ->chunk(1000, function (array $rows): void {
        foreach ($rows as $row) {
            // process $row
        }
    });

// Compile to SQL without executing
$sql = $client->table('events')->where('type', 'click')->toSql();
```

---

## Raw queries

```php
// SELECT — returns a Statement
$stmt = $client->query('SELECT * FROM events WHERE id = :id', ['id' => 42]);

// DDL / DML — returns bool
$client->execute('OPTIMIZE TABLE events FINAL');

// Named placeholders are escaped automatically
$client->query(
    'SELECT * FROM users WHERE name = :name AND age >= :age',
    ['name' => "O'Brien", 'age' => 18],
);
```

---

## Statement API

```php
$stmt = $client->query('SELECT id, type, score FROM events');

$stmt->rows();      // array of associative arrays
$stmt->first();     // first row or null
$stmt->value();     // first column of first row
$stmt->pluck('id'); // flat array of one column
$stmt->count();     // number of rows
$stmt->isEmpty();   // bool
$stmt->raw();       // raw response body

// Execution metadata
$stmt->queryId();   // X-ClickHouse-Query-Id header value
$stmt->summary();   // X-ClickHouse-Summary decoded: read_rows, written_rows, elapsed_ns …

// Iterate rows in batches (splits already-fetched rows in memory)
$stmt->chunk(100, function (array $rows): void {
    // called once per batch
});

// Statement implements Countable and IteratorAggregate
count($stmt);
foreach ($stmt as $row) { ... }
```

---

## Schema / DDL

All schema methods are available via `$client->schema()`.

### Create a table

```php
use Beeterty\ClickHouse\Schema\Blueprint;
use Beeterty\ClickHouse\Schema\Engine\MergeTree;

$client->schema()->create('events', function (Blueprint $table): void {
    $table->uint64('id');
    $table->string('type');
    $table->int32('score');
    $table->dateTime('created_at');
    $table->engine(new MergeTree())->orderBy('id');
});

// Only create if it doesn't already exist
$client->schema()->createIfNotExists('events', function (Blueprint $table): void {
    $table->uint64('id');
    $table->string('type');
    $table->engine(new MergeTree())->orderBy('id');
});
```

### Column types

| Method | ClickHouse type |
|---|---|
| `uint8 / uint16 / uint32 / uint64 / uint128 / uint256` | `UInt8` … `UInt256` |
| `int8 / int16 / int32 / int64 / int128 / int256` | `Int8` … `Int256` |
| `float32 / float64` | `Float32 / Float64` |
| `decimal($name, $precision, $scale)` | `Decimal(P, S)` |
| `string` | `String` |
| `fixedString($name, $length)` | `FixedString(N)` |
| `boolean` | `Bool` |
| `uuid` | `UUID` |
| `date / date32` | `Date / Date32` |
| `dateTime($name, $tz?)` | `DateTime / DateTime('tz')` |
| `dateTime64($name, $precision?, $tz?)` | `DateTime64(P) / DateTime64(P, 'tz')` |
| `ipv4 / ipv6` | `IPv4 / IPv6` |
| `json` | `JSON` |
| `enum8($name, $values)` | `Enum8('a'=1, …)` |
| `enum16($name, $values)` | `Enum16('a'=1, …)` |
| `array($name, $innerType)` | `Array(T)` |
| `map($name, $keyType, $valueType)` | `Map(K, V)` |
| `tuple($name, ...$types)` | `Tuple(T1, T2, …)` |
| `rawColumn($name, $definition)` | raw type string |

Column modifiers (chainable on the returned `ColumnDefinition`):

```php
$table->string('email')->nullable();
$table->uint32('views')->default(0);
$table->string('note')->nullable()->comment('optional note');
```

### Convenience shorthands

```php
$table->id();                 // uint64('id')
$table->timestamps();         // nullable created_at + updated_at DateTime
$table->softDeletes();        // nullable deleted_at DateTime
```

### Table-level options

```php
$table->engine(new MergeTree())
      ->orderBy(['user_id', 'created_at'])
      ->partitionBy('toYYYYMM(created_at)')
      ->primaryKey('user_id')
      ->sampleBy('rand()')
      ->ttl('created_at + INTERVAL 90 DAY')
      ->settings(['index_granularity' => 8192])
      ->comment('User event log');
```

### Available engines

```php
use Beeterty\ClickHouse\Schema\Engine\{
    MergeTree,
    ReplacingMergeTree,
    SummingMergeTree,
    AggregatingMergeTree,
    CollapsingMergeTree,
    Memory,
    Log,
    NullEngine,
};
```

### Alter a table

```php
$client->schema()->table('events', function (Blueprint $table): void {
    $table->string('source');           // ADD COLUMN
    $table->dropColumn('legacy_field'); // DROP COLUMN
    $table->renameColumn('old', 'new'); // RENAME COLUMN
    $table->dropTimestamps();           // drop created_at + updated_at
});
```

### Other DDL

```php
$client->schema()->rename('events', 'events_v2');
$client->schema()->drop('events');
$client->schema()->dropIfExists('events');
```

### Introspection

```php
$client->schema()->hasTable('events');           // bool
$client->schema()->hasColumn('events', 'score'); // bool
$client->schema()->getColumns('events');         // array of column metadata rows
$client->schema()->getTables();                  // array of table metadata rows
```

---

## Materialized views

```php
// Create a materialized view that aggregates into a SummingMergeTree target
$client->schema()->createMaterializedView(
    name:      'daily_totals_mv',
    to:        'daily_totals',
    selectSql: 'SELECT user_id, sum(amount) AS total FROM events GROUP BY user_id',
);

// Idempotent variant
$client->schema()->createMaterializedView(
    name:        'daily_totals_mv',
    to:          'daily_totals',
    selectSql:   '...',
    ifNotExists: true,
);

// Backfill with existing data
$client->schema()->createMaterializedView(
    name:      'daily_totals_mv',
    to:        'daily_totals',
    selectSql: '...',
    populate:  true,
);

$client->schema()->hasView('daily_totals_mv');   // bool
$client->schema()->dropView('daily_totals_mv');
$client->schema()->dropViewIfExists('daily_totals_mv');
```

---

## Inserts

### Array insert

```php
$client->insert('events', [
    ['id' => 1, 'type' => 'click', 'score' => 42],
    ['id' => 2, 'type' => 'view',  'score' => 10],
]);
```

### File streaming insert

Reads the file in 64 kB chunks via `CURLOPT_READFUNCTION` — the file is never fully loaded into memory, making it suitable for multi-gigabyte files.

```php
// Defaults to CSVWithNames
$client->insertFile('events', '/data/events.csv');

// Explicit format
use Beeterty\ClickHouse\Format\TabSeparated;

$client->insertFile('events', '/data/events.tsv', new TabSeparated());
```

---

## Parallel queries

Fire multiple `SELECT` queries simultaneously over independent `curl_multi` handles and collect all results at once. Results are keyed by the same keys you passed in.

```php
$results = $client->parallel([
    'daily'  => $client->table('events')->where('period', 'day'),
    'weekly' => $client->table('events')->where('period', 'week'),
    'total'  => 'SELECT count() AS n FROM events',
]);

$results['daily']->rows();
$results['weekly']->rows();
$results['total']->value();
```

Each value can be either a `QueryBuilder` instance or a raw SQL string.

---

## Async execution

Fire a DDL or DML query without waiting for it to complete. Returns a `query_id` that you can use to track or cancel the query.

```php
$queryId = $client->executeAsync(
    'INSERT INTO archive SELECT * FROM events WHERE created_at < :date',
    ['date' => '2024-01-01'],
);

// Poll until done
while ($client->isRunning($queryId)) {
    sleep(1);
}

// Or cancel it
$client->kill($queryId);
```

> **Note:** Best suited for long-running writes, `OPTIMIZE TABLE`, and `ALTER TABLE`. SELECT queries may be cancelled on disconnect depending on the server's `cancel_http_readonly_queries_on_client_close` setting.

---

## Formats

Pass any `Format` instance to `query()`, `insert()`, `parallel()`, or `insertFile()`.

```php
use Beeterty\ClickHouse\Format\JsonEachRow;    // default for query/insert
use Beeterty\ClickHouse\Format\Csv;            // CSVWithNames, default for insertFile
use Beeterty\ClickHouse\Format\TabSeparated;   // TabSeparatedWithNames
```

Implement `Beeterty\ClickHouse\Format\Contracts\Format` to add your own.

---

## Exceptions

```
Beeterty\ClickHouse\Exception\ClickHouseException  (base)
├── ConnectionException   cURL error or no response
└── QueryException        HTTP 4xx/5xx from ClickHouse — includes the original SQL
```

```php
use Beeterty\ClickHouse\Exception\{ConnectionException, QueryException};

try {
    $client->query('SELECT * FROM nonexistent_table');
} catch (QueryException $e) {
    echo $e->getMessage(); // ClickHouse query failed [404]: ...
    echo $e->getSql();     // SELECT * FROM nonexistent_table FORMAT JSONEachRow
} catch (ConnectionException $e) {
    echo $e->getMessage(); // ClickHouse connection failed: ...
}
```

---

## License

MIT — see [LICENSE](LICENSE).
