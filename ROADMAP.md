# Roadmap

This file tracks planned improvements to the client. Items are checked off as they ship. 🚢

Want to work on something? Open an issue first so we can align on the approach before you invest time in a PR. See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development guide.

---

## Query builder

- [ ] `JOIN` — `join()`, `leftJoin()`, `innerJoin()`, `crossJoin()` with support for ClickHouse join strictness (`ANY`, `ALL`, `ASOF`)
- [ ] `FINAL` modifier — `->final()` appended to the FROM clause, used with `ReplacingMergeTree` / `CollapsingMergeTree` to force deduplication at read time
- [ ] `SAMPLE` clause — `->sample(0.1)` for random fractional row sampling on MergeTree tables
- [ ] `ARRAY JOIN` — ClickHouse-specific clause that flattens array columns into rows
- [ ] `WITH` / CTEs — `->with('cte_name', $subquery)` for common table expressions
- [ ] `UNION ALL` / `UNION DISTINCT` — combine two `QueryBuilder` instances
- [ ] Subqueries — pass a `QueryBuilder` instance as the value in `whereIn()` and similar clauses

---

## HTTP interface features

- [ ] **Settings passthrough** — pass arbitrary ClickHouse query settings per-request or globally via `Config`
    ```php
    $client->query('SELECT ...', settings: ['max_result_rows' => 1000, 'max_threads' => 4]);
    new Config(..., settings: ['max_threads' => 4]);
    ```
- [ ] **Sessions** — `session_id` / `session_timeout` support for temporary tables and stateful queries
    ```php
    $session = $client->session('my-session');
    $session->execute('CREATE TEMPORARY TABLE ...');
    ```
- [ ] **Roles** — `->withRole('analyst')` fluent method on `Client` that sets the `role` URL parameter
- [ ] **Profile** — `->withProfile('readonly')` sets the `profile` URL parameter
- [ ] **Quota key** — `quota_key` URL parameter for per-tenant rate limiting
- [ ] **Server-side parameterized queries** — native `{name:Type}` placeholder syntax, distinct from our client-side `:name` binding
- [ ] **Progress tracking** — `send_progress_in_http_headers=1` with a callback to receive `X-ClickHouse-Progress` updates during long queries
- [ ] **External data** — send a temporary in-memory table alongside a query as a multipart POST body

---

## Formats

- [ ] **`JSONCompactEachRow`** — like `JsonEachRow` but rows are arrays instead of objects; smaller payloads, faster parsing
- [ ] **`JSONCompactEachRowWithNamesAndTypes`** — compact rows with column names and ClickHouse types in the first two rows; useful when type metadata is needed
- [ ] **`Native`** — ClickHouse's own binary columnar format; zero parsing overhead, highest throughput; requires a binary codec (significant effort)
- [ ] **`Parquet`** — Apache Parquet support for `insertFile()` and `query()`; requires an optional PHP Parquet library
- [ ] **`Arrow` / `ArrowStream`** — Apache Arrow columnar format; great for analytics pipelines

---

## Schema / DDL

- [ ] `CREATE VIEW` (non-materialized) — simple `SELECT`-based views
- [ ] `ATTACH` / `DETACH` table support
- [ ] `FREEZE` partition support
- [ ] `MOVE` partition support
- [ ] Dictionary DDL — `CREATE DICTIONARY` / `DROP DICTIONARY`

---

## Client

- [ ] Connection pooling — reuse connections across requests in long-running processes (e.g. FPM workers)
- [ ] Read-replica routing — automatically direct `SELECT` queries to a replica and writes to the primary
- [ ] `insertStream()` — accept a PHP `resource` or `Generator` as the data source instead of a file path (generalises `insertFile`)

---

## Developer experience

- [ ] Update README badge once Packagist listing is live: `[![Latest Version](https://img.shields.io/packagist/v/beeterty/clickhouse-php-client)](https://packagist.org/packages/beeterty/clickhouse-php-client)`
- [ ] PHPStan upgrade to level 10 (v2.x)
- [ ] Benchmark suite — track query throughput and memory across releases

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
- [x] Immutable `Config` mutators — `withDatabase()`, `withCredentials()`, `withHttps()`, `withRetries()`, `withCompression()`, etc.
- [x] PHPStan level 8
- [x] GitHub Actions CI (PHP 8.2 + 8.3) with release gate
