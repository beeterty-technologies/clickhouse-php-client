# ClickHouse PHP Client

[![CI](https://github.com/beeterty-technologies/clickhouse-php-client/actions/workflows/ci.yml/badge.svg)](https://github.com/beeterty-technologies/clickhouse-php-client/actions/workflows/ci.yml)
[![Latest Version](https://img.shields.io/packagist/v/beeterty/clickhouse-php-client)](https://packagist.org/packages/beeterty/clickhouse-php-client)
[![PHP](https://img.shields.io/badge/PHP-8.2%2B-blue)](https://www.php.net)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A lightweight, zero-dependency ClickHouse HTTP client for PHP 8.2+.

- Fluent query builder — `JOIN`, `ARRAY JOIN`, `WITH`/CTEs, `UNION`, subqueries, `PREWHERE`, `FINAL`, `SAMPLE`
- Full DDL support — Blueprint/Grammar pattern, materialized and regular views, ATTACH/DETACH, partition ops
- Five wire formats — `JsonEachRow`, `JSONCompactEachRow` (+ with names/types), `CSVWithNames`, `TabSeparatedWithNames`
- HTTP features — sessions, settings passthrough, roles, profile, quota key, server-side params, progress tracking, external data
- Production-ready client — connection pooling, read-replica routing, parallel queries, async execution, streaming inserts
- PHPStan level 10, 323 tests, benchmark suite

---

## Requirements

| Requirement | Version            |
| ----------- | ------------------ |
| PHP         | ≥ 8.2              |
| ext-curl    | any                |
| ext-json    | any                |
| ClickHouse  | any recent version |

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
    timeout:        30,       // cURL transfer timeout (seconds)
    connectTimeout: 5,        // cURL connect timeout (seconds)
    retries:        3,        // extra attempts on connection failure
    retryDelay:     200,      // ms between retries
    compression:    true,     // gzip INSERT bodies
    settings:       ['max_threads' => 4],     // ClickHouse settings on every request
    roles:          ['analyst'],              // roles activated per request (CH 24.4+)
    profile:        'readonly',              // settings profile per request
    quotaKey:       'tenant-123',            // quota key for rate limiting
);

// Or from an array (e.g. loaded from a config file)
$config = Config::fromArray([
    'host'      => '127.0.0.1',
    'port'      => 8123,
    'database'  => 'analytics',
    'username'  => 'default',
    'password'  => 'secret',
    'settings'  => ['max_execution_time' => 60],
    'quota_key' => 'tenant-abc',
]);

// Immutable mutators — each returns a new Config
$config->withHost('ch.example.com')
       ->withPort(8443)
       ->withHttps()
       ->withDatabase('analytics')
       ->withCredentials('user', 'pass')
       ->withTimeout(60)
       ->withRetries(3, delayMs: 500)
       ->withCompression()
       ->withSettings(['max_threads' => 8])
       ->withRole('analyst', 'reader')
       ->withProfile('readonly')
       ->withQuotaKey('tenant-xyz');
```

### Connection pooling and read replicas

```php
// Pre-create 5 reusable cURL handles (useful for long-running processes)
$client = new Client(new Config(...), poolSize: 5);

// Route SELECT queries round-robin to replicas, writes always to primary
$client = new Client(
    config:   new Config(host: 'primary.db'),
    replicas: [
        new Config(host: 'replica1.db'),
        new Config(host: 'replica2.db'),
    ],
    poolSize: 3,
);
```

---

## Query builder

Obtain a builder via `$client->table('name')`.

### SELECT

```php
$client->table('events')
    ->select('id', 'type', 'score')      // backtick-quoted automatically
    ->addSelect('created_at')            // append to list
    ->selectRaw('count() AS n')          // raw expression, replaces list
    ->addSelectRaw('avg(score) AS avg')  // append raw expression
```

### WHERE

```php
->where('type', 'click')                // = shorthand
->where('score', '>=', 80)             // any operator
->whereRaw('toDate(created_at) = today()')
->whereIn('status', ['active', 'pending'])
->whereIn('user_id', $client->table('admins')->select('id'))  // subquery
->whereNotIn('id', [4, 5])
->whereNotIn('id', $subqueryBuilder)
->whereBetween('score', 60, 90)
->whereNull('deleted_at')
->whereNotNull('published_at')
```

### PREWHERE (ClickHouse-specific)

Evaluated before `WHERE`, reads only the referenced columns — efficient for ORDER BY key columns.

```php
->prewhere('event_date', '>=', '2024-01-01')
->prewhereRaw('event_date >= today()')
```

### JOIN

```php
// Simple form (implied =)
->join('orders', 'users.id', 'orders.user_id')
->leftJoin('profiles', 'users.id', 'profiles.user_id')
->rightJoin('events', 'users.id', 'events.user_id')
->fullJoin('b', 'a.id', 'b.id')
->crossJoin('dimensions')
->innerJoin('orders', 'users.id', '=', 'orders.user_id')

// ClickHouse join strictness — ANY, ALL (default, omitted), SEMI, ANTI, ASOF
->join('orders', 'users.id', 'orders.user_id', strictness: 'ANY')
->leftJoin('ticks', 'prices.symbol', 'ticks.symbol', strictness: 'ASOF')

// Closure for multiple ON conditions
->join('orders', function (JoinClause $join): void {
    $join->on('users.id', '=', 'orders.user_id')
         ->on('users.tenant_id', '=', 'orders.tenant_id');
})
```

### ARRAY JOIN (ClickHouse-specific)

Flattens array-typed columns so each element becomes a separate row.

```php
->arrayJoin('tags')            // ARRAY JOIN `tags`
->arrayJoin('tags', 'scores') // ARRAY JOIN `tags`, `scores`
->leftArrayJoin('tags')        // preserve rows with empty arrays
```

### FINAL / SAMPLE

```php
->final()          // force deduplication (ReplacingMergeTree / CollapsingMergeTree)
->sample(0.1)      // read ~10% of rows (MergeTree tables with SAMPLE BY)
```

### WITH / CTEs

```php
->with('recent', $client->table('events')->where('ts', '>=', '2024-01-01'))
->table('recent')
->get()
// → WITH recent AS (SELECT * FROM `events` WHERE ...) SELECT * FROM `recent`

->with('summary', 'SELECT user_id, count() AS n FROM events GROUP BY user_id')
```

### UNION

```php
$a = $client->table('events_2023')->select('id', 'name');
$b = $client->table('events_2024')->select('id', 'name');

$a->unionAll($b)->get();
$a->unionDistinct($b)->get();
```

### GROUP BY / HAVING / ORDER BY / LIMIT

```php
->groupBy('type')
->having('count() > 100')
->orderBy('score')              // ASC by default
->orderByDesc('score')
->limit(100)->offset(200)
```

### Terminal methods

```php
->get()           // Statement (all rows)
->first()         // first row or null
->count()         // row count, ignores LIMIT/ORDER BY
->value()         // scalar from first row, first column
->pluck('id')     // flat array of one column
->chunk(1000, fn) // paginated iteration
->toSql()         // compile without executing
```

---

## Raw queries

```php
// SELECT → Statement
$stmt = $client->query('SELECT * FROM events WHERE id = :id', ['id' => 42]);

// DDL / DML → bool
$client->execute('OPTIMIZE TABLE events FINAL');

// Per-request settings override
$stmt = $client->query('SELECT count() FROM big_table', settings: ['max_threads' => 8]);

// Server-side parameterized queries — {name:Type} syntax
$stmt = $client->query(
    'SELECT * FROM events WHERE user_id = {uid:UInt64} AND type = {t:String}',
    params: ['uid' => 42, 't' => 'click'],
);

// Progress callback — fires for each X-ClickHouse-Progress header
$client->query('SELECT count() FROM huge_table', onProgress: function (array $p): void {
    echo "Read {$p['read_rows']} / {$p['total_rows_to_read']} rows\n";
});
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
$stmt->queryId();   // X-ClickHouse-Query-Id
$stmt->summary();   // X-ClickHouse-Summary decoded: read_rows, written_rows, elapsed_ns …

$stmt->chunk(100, function (array $rows): void {
    // called once per batch
});

// Countable and IteratorAggregate
count($stmt);
foreach ($stmt as $row) { ... }
```

---

## Sessions

Sessions tie requests together with a shared `session_id`, enabling temporary tables and stateful operations.

```php
$session = $client->session('my-session', timeout: 300);

$session->execute('CREATE TEMPORARY TABLE tmp (id UInt64) ENGINE = Memory');
$session->execute('INSERT INTO tmp VALUES (1), (2), (3)');
$rows = $session->query('SELECT * FROM tmp')->rows();

// Session::query() / execute() / insert() all accept the same options as Client
$session->query('SELECT * FROM tmp', onProgress: fn($p) => ..., settings: [...]);
```

---

## Schema / DDL

All schema methods are available via `$client->schema()`.

### Tables

```php
$client->schema()->create('events', function (Blueprint $table): void {
    $table->uint64('id');
    $table->string('type');
    $table->int32('score');
    $table->dateTime('created_at');
    $table->engine(new MergeTree())->orderBy('id');
});

$client->schema()->createIfNotExists('events', fn(Blueprint $t) => ...);
$client->schema()->rename('events', 'events_v2');
$client->schema()->drop('events');
$client->schema()->dropIfExists('events');
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
| `dateTime64($name, $precision?, $tz?)` | `DateTime64(P, 'tz')` |
| `ipv4 / ipv6` | `IPv4 / IPv6` |
| `json` | `JSON` |
| `enum8($name, $values) / enum16(...)` | `Enum8(...) / Enum16(...)` |
| `array($name, $innerType)` | `Array(T)` |
| `map($name, $keyType, $valueType)` | `Map(K, V)` |
| `tuple($name, ...$types)` | `Tuple(T1, T2, …)` |

Column modifiers: `->nullable()`, `->default($value)`, `->lowCardinality()`, `->comment('...')`, `->codec('...')`, `->after('col')`.

Shorthands: `id()`, `timestamps()`, `softDeletes()`.

### Alter a table

```php
$client->schema()->table('events', function (Blueprint $table): void {
    $table->string('source');
    $table->string('source')->change();      // MODIFY COLUMN
    $table->dropColumn('legacy_field');
    $table->renameColumn('old', 'new');
});
```

### Views

```php
// Regular view (SELECT evaluated at read time)
$client->schema()->createView('v_active', 'SELECT * FROM users WHERE active = 1');
$client->schema()->createViewIfNotExists('v_active', 'SELECT ...');

// Materialized view
$client->schema()->createMaterializedView(
    name:        'daily_totals_mv',
    to:          'daily_totals',
    selectSql:   'SELECT user_id, sum(amount) AS total FROM events GROUP BY user_id',
    ifNotExists: true,
    populate:    false,
);

$client->schema()->hasView('daily_totals_mv');
$client->schema()->dropView('daily_totals_mv');
$client->schema()->dropViewIfExists('daily_totals_mv');
```

### ATTACH / DETACH

```php
$client->schema()->attach('events');
$client->schema()->attachIfNotExists('events');
$client->schema()->detach('events');
$client->schema()->detachIfExists('events');
```

### Partition operations

```php
// FREEZE — create a local backup snapshot
$client->schema()->freeze('events');                       // all partitions
$client->schema()->freeze('events', '202401');             // specific partition
$client->schema()->freeze('events', '202401', 'jan_bak'); // with backup name

// MOVE — relocate a partition
$client->schema()->movePartitionToTable('events', '202401', 'events_archive');
$client->schema()->movePartitionToDisk('events', '202401', 'hot_disk');
$client->schema()->movePartitionToVolume('events', '202401', 'cold_volume');
```

### Dictionaries

```php
// CREATE DICTIONARY requires raw SQL (complex SOURCE/LAYOUT/LIFETIME syntax)
$client->execute('CREATE DICTIONARY my_dict (...) SOURCE(...) LAYOUT(...) LIFETIME(...)');

$client->schema()->dropDictionary('my_dict');
$client->schema()->dropDictionaryIfExists('my_dict');
```

### Introspection

```php
$client->schema()->hasTable('events');
$client->schema()->hasColumn('events', 'score');
$client->schema()->getColumns('events');  // name, type, default_kind, comment …
$client->schema()->getTables();           // name, engine, total_rows, total_bytes …
```

---

## Inserts

```php
// Array insert
$client->insert('events', [
    ['id' => 1, 'type' => 'click', 'score' => 42],
]);

// File streaming — 64 kB chunked, never loads the file into memory
$client->insertFile('events', '/data/events.csv');                      // CSVWithNames
$client->insertFile('events', '/data/events.tsv', new TabSeparated());

// Stream from a resource or Generator
$fh = fopen('/data/events.ndjson', 'rb');
$client->insertStream('events', $fh, new JsonEachRow());

$client->insertStream('events', (function (): \Generator {
    foreach ($rows as $row) { yield $row; }
})(), new JsonEachRow());
```

---

## Formats

| Class | FORMAT name | Decoded row shape |
|---|---|---|
| `JsonEachRow` | `JSONEachRow` | `array<string, mixed>` |
| `JsonCompactEachRow` | `JSONCompactEachRow` | `array<int, mixed>` |
| `JsonCompactEachRowWithNamesAndTypes` | `JSONCompactEachRowWithNamesAndTypes` | `array<string, mixed>` |
| `Csv` | `CSVWithNames` | `array<string, string>` |
| `TabSeparated` | `TabSeparatedWithNames` | `array<string, string>` |

Pass any `Format` instance to `query()`, `insert()`, `parallel()`, `insertFile()`, or `insertStream()`. Implement `Beeterty\ClickHouse\Format\Contracts\Format` to add your own.

---

## Parallel queries

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

---

## Async execution

```php
$queryId = $client->executeAsync(
    'INSERT INTO archive SELECT * FROM events WHERE created_at < :date',
    ['date' => '2024-01-01'],
);

while ($client->isRunning($queryId)) {
    sleep(1);
}

$client->kill($queryId); // cancel
```

---

## External data

Send temporary in-memory tables alongside a query — useful for JOINs against small lookup sets without creating a permanent table.

```php
use Beeterty\ClickHouse\ExternalTable;

$result = $client->queryWithExternalData(
    'SELECT e.ts, l.label FROM events e JOIN labels l ON e.type_id = l.id',
    externalTables: [
        ExternalTable::fromRows('labels', 'id UInt8, label String', [
            ['id' => 1, 'label' => 'click'],
            ['id' => 2, 'label' => 'view'],
        ]),
    ],
);

// Or from a pre-encoded string
new ExternalTable('labels', 'id UInt8, label String', "1\tclick\n2\tview");
```

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

## Benchmarks

```bash
vendor/bin/phpbench run benchmarks/ --report=aggregate
```

Covers query builder compilation (6 shapes), all format encode/decode pairs, and statement iteration at 100 / 1k / 10k rows. Use `--store` and `--ref` to compare across releases.

---

## License

MIT — see [LICENSE](LICENSE).
