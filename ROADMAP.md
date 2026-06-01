# Roadmap

This file tracks planned improvements to the client. Items are checked off as they ship.

Want to work on something? Open an issue first so we can align on the approach before you invest time in a PR. See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development guide.

---

## Query builder

- [x] `JOIN` — `join()`, `innerJoin()`, `leftJoin()`, `rightJoin()`, `fullJoin()`, `crossJoin()` with support for ClickHouse join strictness (`ANY`, `ALL`, `SEMI`, `ANTI`, `ASOF`)
- [x] `FINAL` modifier — `->final()` appended to the FROM clause, used with `ReplacingMergeTree` / `CollapsingMergeTree` to force deduplication at read time
- [x] `SAMPLE` clause — `->sample(0.1)` for random fractional row sampling on MergeTree tables
- [x] `ARRAY JOIN` — `->arrayJoin()` / `->leftArrayJoin()` flatten array columns into rows
- [x] `WITH` / CTEs — `->with('cte_name', $subquery)` for common table expressions
- [x] `UNION ALL` / `UNION DISTINCT` — `->unionAll($query)` / `->unionDistinct($query)` combine two Builder instances
- [x] Subqueries — pass a `Builder` instance as the value in `whereIn()` and `whereNotIn()`

---

## HTTP interface features

- [x] **Settings passthrough** — `Config::withSettings([...])` for global settings; `settings: [...]` named arg on `query()`, `execute()`, `insert()`, `parallel()` for per-request overrides
- [x] **Roles** — `Config::withRole('analyst')` sets the `role` URL parameter (ClickHouse 24.4+, multiple roles supported)
- [x] **Profile** — `Config::withProfile('readonly')` sets the `profile` URL parameter
- [x] **Quota key** — `Config::withQuotaKey('tenant-id')` sets the `quota_key` URL parameter
- [x] **Sessions** — `$client->session('id', timeout: 300)` returns a `Session` instance; all requests carry `session_id` + `session_timeout` URL parameters, enabling temporary tables and stateful queries
- [x] **Server-side parameterized queries** — `params: ['name' => $val]` named arg on `query()` / `execute()` maps to native `{name:Type}` placeholder syntax via `param_name=value` URL parameters
- [x] **Progress tracking** — `onProgress: fn(array $p) => ...` named arg on `query()` / `execute()` / `Session`; automatically enables `send_progress_in_http_headers=1`; callback receives `read_rows`, `read_bytes`, `total_rows_to_read`, `elapsed_ns`, etc.
- [x] **External data** — `queryWithExternalData(sql, externalTables: [...])` sends temporary in-memory tables as multipart POST; `ExternalTable::fromRows()` factory encodes row arrays with any Format

---

## Formats

- [x] **`JSONCompactEachRow`** — rows as JSON arrays; smaller payloads, faster parsing; decoded rows are integer-indexed
- [x] **`JSONCompactEachRowWithNamesAndTypes`** — names + types header rows followed by compact array rows; decoded rows are associative (same shape as JSONEachRow)
- [ ] **`Native`** — ClickHouse's own binary columnar format; zero parsing overhead, highest throughput; requires a binary codec (significant effort)
- [ ] **`Parquet`** — Apache Parquet support for `insertFile()` and `query()`; requires an optional PHP Parquet library
- [ ] **`Arrow` / `ArrowStream`** — Apache Arrow columnar format; great for analytics pipelines

---

## Schema / DDL

- [x] `CREATE VIEW` — `createView()` / `createViewIfNotExists()` for simple SELECT-based views
- [x] `ATTACH` / `DETACH` — `attach()`, `attachIfNotExists()`, `detach()`, `detachIfExists()`
- [x] `FREEZE` partition — `freeze($table, $partition, $backupName)` — freezes specific or all partitions with optional backup name
- [x] `MOVE` partition — `movePartitionToTable()`, `movePartitionToDisk()`, `movePartitionToVolume()`
- [x] Dictionary DDL — `dropDictionary()` / `dropDictionaryIfExists()` (CREATE DICTIONARY requires raw SQL via `execute()` due to its complex SOURCE/LAYOUT/LIFETIME syntax)

---

## Client

- [x] Connection pooling — `new Client(config: ..., poolSize: 5)` pre-creates N reusable cURL handles; pool size defaults to 1 (backward-compatible); handles released back after each request
- [x] Read-replica routing — `new Client(config: $primary, replicas: [$r1, $r2])` round-robins SELECT queries across replicas; writes always go to primary
- [x] `insertStream()` — accepts a PHP `resource` (file handle) or `Generator` (yields row arrays), streamed via chunked-transfer POST

---

## Developer experience

- [x] Update README badge — Packagist version badge added
- [x] PHPStan upgrade to level 10 (v2.x) — upgraded to PHPStan 2.x, all 41 type errors fixed, zero errors at level 10
- [x] Benchmark suite — `vendor/bin/phpbench run benchmarks/ --report=aggregate`; covers QueryBuilder compilation (6 shapes), all 5 Format encode/decode pairs, and Statement iteration/pluck/chunk at 100/1k/10k rows

---

## Shipped ✓

- [x] Fluent query builder (`SELECT`, `WHERE`, `PREWHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, `OFFSET`)
- [x] `PREWHERE` support (ClickHouse-specific pre-filter)
- [x] `whereIn`, `whereNotIn`, `whereBetween`, `whereNull`, `whereNotNull`
- [x] Terminal methods: `get()`, `first()`, `count()`, `value()`, `pluck()`, `chunk()`
- [x] `Statement` — `rows()`, `first()`, `value()`, `pluck()`, `count()`, `isEmpty()`, `raw()`, `queryId()`, `summary()`, `chunk()`
- [x] `Statement` implements `Countable` + `IteratorAggregate`
- [x] Full DDL Blueprint — all integer, float, decimal, string, date/time, boolean, UUID, IP, JSON, enum, array, map, tuple types
- [x] Blueprint shorthands: `id()`, `timestamps()`, `softDeletes()`
- [x] Table-level options: `engine()`, `orderBy()`, `partitionBy()`, `primaryKey()`, `sampleBy()`, `ttl()`, `settings()`, `comment()`
- [x] Engines: `MergeTree`, `ReplacingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, `CollapsingMergeTree`, `Memory`, `Log`, `NullEngine`
- [x] `ALTER TABLE` — add, drop, rename columns
- [x] Schema introspection — `hasTable()`, `hasColumn()`, `getColumns()`, `getTables()`
- [x] Materialized views — `createMaterializedView()`, `dropView()`, `dropViewIfExists()`, `hasView()`
- [x] Formats: `JsonEachRow`, `CSVWithNames`, `TabSeparatedWithNames`
- [x] Parallel queries via `curl_multi` — `parallel(array $queries)`
- [x] File streaming inserts — `insertFile(string $table, string $path)`
- [x] Async fire-and-forget — `executeAsync()`, `isRunning()`, `kill()`
- [x] Retry logic — `retries` + `retryDelay` on `Config`
- [x] Gzip compression — `compression` on `Config`
- [x] Named placeholder bindings — `:name` in raw SQL
- [x] `FINAL` modifier on query builder — `->final()` for `ReplacingMergeTree` / `CollapsingMergeTree` deduplication at read time
- [x] `SAMPLE` clause on query builder — `->sample(0.1)` for fractional row sampling on MergeTree tables
- [x] Immutable `Config` mutators — `withDatabase()`, `withCredentials()`, `withHttps()`, `withRetries()`, `withCompression()`, etc.
- [x] PHPStan level 8
- [x] GitHub Actions CI (PHP 8.2 + 8.3) with release gate
