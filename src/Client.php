<?php

namespace Beeterty\ClickHouse;

use Beeterty\ClickHouse\Exception\{ConnectionException, QueryException};
use Beeterty\ClickHouse\ExternalTable;
use Beeterty\ClickHouse\Format\Contracts\Format;
use Beeterty\ClickHouse\Format\{Csv, JsonEachRow};
use Beeterty\ClickHouse\Query\{Builder, Statement};
use Beeterty\ClickHouse\Schema\Schema;
use Beeterty\ClickHouse\Session;
use CurlHandle;

class Client
{
    /**
     * Pool of reusable cURL handles.
     *
     * @var list<CurlHandle>
     */
    private array $curlPool = [];

    /**
     * Maximum number of handles to keep in the pool.
     *
     * @var int
     */
    private int $poolSize;

    /**
     * Read-replica Config instances for SELECT routing.
     *
     * @var list<Config>
     */
    private readonly array $replicas;

    /**
     * Round-robin cursor for replica selection.
     *
     * @var int
     */
    private int $replicaIndex = 0;

    /**
     * Create a new ClickHouse client instance.
     *
     * @param Config       $config   Primary connection configuration.
     * @param int          $poolSize Number of cURL handles to keep in the pool (default: 1).
     * @param list<Config> $replicas Optional read-replica configs for SELECT routing (round-robin).
     */
    public function __construct(
        private readonly Config $config,
        int $poolSize = 1,
        array $replicas = [],
    ) {
        $this->poolSize = max(1, $poolSize);
        $this->replicas = $replicas;

        // Pre-fill the pool.
        for ($i = 0; $i < $this->poolSize; $i++) {
            $this->curlPool[] = curl_init();
        }
    }

    /**
     * Acquire a cURL handle from the pool (or create a fresh one if the pool is empty).
     *
     * @return CurlHandle
     */
    private function acquireHandle(): CurlHandle
    {
        if (!empty($this->curlPool)) {
            /** @var CurlHandle */
            return array_pop($this->curlPool);
        }

        return curl_init();
    }

    /**
     * Return a cURL handle to the pool. If the pool is already at capacity, the handle is closed.
     *
     * @param CurlHandle $handle
     * @return void
     */
    private function releaseHandle(CurlHandle $handle): void
    {
        if (\count($this->curlPool) < $this->poolSize) {
            $this->curlPool[] = $handle;
        } else {
            curl_close($handle);
        }
    }

    /**
     * Select the next replica config for read routing (round-robin).
     * Falls back to the primary config when no replicas are configured.
     *
     * @return Config
     */
    private function selectReplica(): Config
    {
        if (empty($this->replicas)) {
            return $this->config;
        }

        $config = $this->replicas[$this->replicaIndex % \count($this->replicas)];
        $this->replicaIndex++;

        return $config;
    }

    /**
     * Return a Session bound to the given session ID.
     *
     * All requests made through the session carry the session_id and
     * session_timeout URL parameters, allowing temporary tables and other
     * session-scoped state to persist across calls.
     *
     * Example:
     *   $session = $client->session('my-session', timeout: 300);
     *   $session->execute('CREATE TEMPORARY TABLE tmp (id UInt64) ENGINE = Memory');
     *   $session->query('SELECT * FROM tmp')->rows();
     *
     * @see https://clickhouse.com/docs/en/interfaces/http#http-sessions
     *
     * @param string $id      Session identifier — any non-empty string.
     * @param int    $timeout Inactivity timeout in seconds (default: 60).
     * @return Session
     */
    public function session(string $id, int $timeout = 60): Session
    {
        return new Session($this, $id, $timeout);
    }

    /**
     * Return a Schema instance for DDL operations on this connection.
     *
     * @return Schema
     */
    public function schema(): Schema
    {
        return new Schema($this);
    }

    /**
     * Return a fluent query Builder scoped to the given table.
     *
     * @param string $table
     * @return Builder
     */
    public function table(string $table): Builder
    {
        return (new Builder($this))->table($table);
    }

    /**
     * Ping the ClickHouse server to check if it's reachable and responsive.
     *
     * @return bool
     */
    public function ping(): bool
    {
        try {
            $result = $this->send($this->config, 'SELECT 1');

            return trim($result['body']) === '1';
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * Execute a SELECT query and return a Statement wrapping the result rows.
     *
     * The SQL is sent via POST with wait_end_of_query=1 so that HTTP 200 genuinely
     * means success. Named placeholders in the form :name are substituted client-side
     * before the query is dispatched.
     *
     * Per-request settings are merged on top of any global settings defined in Config,
     * with per-request values taking precedence on conflict.
     *
     * Example:
     *   $stmt = $client->query('SELECT * FROM events WHERE status = :status', ['status' => 'active']);
     *   $stmt = $client->query('SELECT 1', settings: ['max_threads' => 4]);
     *
     * @see https://clickhouse.com/docs/en/interfaces/http
     * @see https://clickhouse.com/docs/en/operations/settings/settings
     *
     * @param string      $sql      SQL query string, optionally with :name client-side or {name:Type} server-side placeholders.
     * @param array<string, mixed> $bindings Client-side named placeholder values keyed by placeholder name.
     * @param Format|null $format   Response format — defaults to JsonEachRow.
     * @param array<string, int|float|string|bool> $settings Per-request ClickHouse settings, merged with global Config settings.
     * @param array<string, int|float|string|bool> $params   Server-side query parameters for {name:Type} placeholders — passed as param_name=value URL parameters.
     * @param (callable(array<string, string>): void)|null $onProgress Optional callback invoked for each X-ClickHouse-Progress header received during execution. Automatically enables send_progress_in_http_headers=1.
     * @return Statement
     * @throws ConnectionException
     * @throws QueryException
     */
    public function query(string $sql, array $bindings = [], ?Format $format = null, array $settings = [], array $params = [], ?callable $onProgress = null): Statement
    {
        if ($onProgress !== null) {
            $settings = array_merge(['send_progress_in_http_headers' => 1], $settings);
        }

        $format ??= new JsonEachRow();

        $sql = $this->bindParams($sql, $bindings) . ' FORMAT ' . $format->name();
        $result = $this->send($this->selectReplica(), $sql, '', array_merge($settings, $this->prepareServerParams($params)), $onProgress);

        return new Statement($result['body'], $format, $result['headers']);
    }

    /**
     * Insert an array of rows into a ClickHouse table.
     *
     * Rows are encoded using the given format (default: JsonEachRow) and sent as
     * the POST body. Each row must be an associative array whose keys match the
     * target table's column names.
     *
     * Example:
     *   $client->insert('events', [
     *       ['user_id' => 1, 'event' => 'click', 'ts' => '2024-01-01 00:00:00'],
     *       ['user_id' => 2, 'event' => 'view',  'ts' => '2024-01-01 00:00:01'],
     *   ]);
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/insert-into
     *
     * @param string      $table    Target table name.
     * @param array<int, array<string, mixed>> $rows Array of associative arrays — one per row.
     * @param Format|null $format   Encoding format — defaults to JsonEachRow.
     * @param array<string, int|float|string|bool> $settings Per-request ClickHouse settings.
     * @return bool
     * @throws ConnectionException
     * @throws QueryException
     */
    public function insert(string $table, array $rows, ?Format $format = null, array $settings = []): bool
    {
        $format ??= new JsonEachRow();

        $sql = "INSERT INTO {$table} FORMAT " . $format->name();

        $this->send($this->config, $sql, $format->encode($rows), $settings);

        return true;
    }

    /**
     * Execute a DDL or DML statement (CREATE, ALTER, DROP, OPTIMIZE, etc.).
     *
     * Unlike query(), no FORMAT clause is appended and no response body is parsed.
     * Returns true on success; throws on any server or connection error.
     *
     * Example:
     *   $client->execute('OPTIMIZE TABLE events FINAL');
     *   $client->execute('ALTER TABLE events DELETE WHERE user_id = :id', ['id' => 42]);
     *   $client->execute('INSERT INTO t SELECT * FROM s', settings: ['max_threads' => 8]);
     *
     * @see https://clickhouse.com/docs/en/interfaces/http
     *
     * @param string $sql      DDL or DML statement, optionally with :name client-side or {name:Type} server-side placeholders.
     * @param array<string, mixed>  $bindings Client-side named placeholder values.
     * @param array<string, int|float|string|bool> $settings Per-request ClickHouse settings.
     * @param array<string, int|float|string|bool> $params   Server-side query parameters for {name:Type} placeholders.
     * @param (callable(array<string, string>): void)|null $onProgress Optional callback invoked for each X-ClickHouse-Progress header. Automatically enables send_progress_in_http_headers=1.
     * @return bool
     * @throws ConnectionException
     * @throws QueryException
     */
    public function execute(string $sql, array $bindings = [], array $settings = [], array $params = [], ?callable $onProgress = null): bool
    {
        if ($onProgress !== null) {
            $settings = array_merge(['send_progress_in_http_headers' => 1], $settings);
        }

        $this->send($this->config, $this->bindParams($sql, $bindings), '', array_merge($settings, $this->prepareServerParams($params)), $onProgress);

        return true;
    }

    /**
     * Fire a DDL/DML query without waiting for it to complete.
     *
     * Uses wait_end_of_query=0 and a short read timeout so that the method
     * returns as soon as the query has been submitted. The query continues
     * running on the server (ClickHouse does not cancel write/DDL operations
     * when the HTTP client disconnects).
     *
     * Returns a query_id that can be passed to isRunning() or kill().
     *
     * Note: best suited for DDL and long-running write operations (OPTIMIZE,
     * ALTER, INSERT SELECT). SELECT queries may be cancelled server-side on
     * disconnect depending on ClickHouse's cancel_http_readonly_queries_on_client_close setting.
     *
     * @see https://clickhouse.com/docs/en/interfaces/http#query_id
     *
     * @param string $sql      DDL or DML statement, optionally with :name placeholders.
     * @param array<string, mixed> $bindings Named placeholder values.
     * @return string The generated query_id — pass to isRunning() or kill() to track the query.
     * @throws ConnectionException
     */
    public function executeAsync(string $sql, array $bindings = []): string
    {
        $queryId = uniqid('async_', true);
        $sql = $this->bindParams($sql, $bindings);

        $url = $this->buildUrl($this->config, $sql, ['query_id' => $queryId, 'wait_end_of_query' => 0]);

        $ch = curl_init();

        curl_setopt_array($ch, [
            CURLOPT_URL => $url,
            CURLOPT_POST => true,
            CURLOPT_POSTFIELDS => '',
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_CONNECTTIMEOUT => $this->config->connectTimeout,
            CURLOPT_TIMEOUT_MS => 2000,
            CURLOPT_HTTPHEADER => $this->authHeaders($this->config),
        ]);

        curl_exec($ch);

        return $queryId;
    }

    /**
     * Return true if a query with the given query_id is still executing.
     *
     * Queries system.processes — the live table of currently running queries.
     * Returns false as soon as the query finishes or is killed.
     *
     * @see https://clickhouse.com/docs/en/operations/system-tables/processes
     *
     * @param string $queryId The query_id returned by executeAsync().
     * @return bool
     */
    public function isRunning(string $queryId): bool
    {
        return !$this->query(
            'SELECT query_id FROM system.processes WHERE query_id = :id',
            ['id' => $queryId],
        )->isEmpty();
    }

    /**
     * Kill a running query by its query_id.
     *
     * Sends a KILL QUERY statement targeting the given query_id. Returns true
     * whether or not a matching query was found — check isRunning() first if you
     * need to confirm the query was actually alive.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/kill#kill-query
     *
     * @param string $queryId The query_id to kill.
     * @return bool
     */
    public function kill(string $queryId): bool
    {
        $escaped = str_replace(["\\", "'"], ["\\\\", "\\'"], $queryId);

        $this->execute("KILL QUERY WHERE query_id = '{$escaped}'");

        return true;
    }

    /**
     * Execute multiple SELECT queries in parallel using curl_multi.
     *
     * Each query runs simultaneously over its own cURL handle. Pass an
     * associative array to preserve meaningful keys in the result:
     *
     *   $results = $client->parallel([
     *       'daily'   => $client->table('events')->where('period', 'day')->toSql(),
     *       'weekly'  => $client->table('events')->where('period', 'week')->toSql(),
     *   ]);
     *
     *   $results['daily']->rows();   // Statement for the first query
     *   $results['weekly']->rows();  // Statement for the second query
     *
     * @see https://clickhouse.com/docs/en/interfaces/http
     *
     * @param array<string|int, string|Builder> $queries Keyed map of SQL strings or Builder instances.
     * @param Format|null $format Response format — defaults to JsonEachRow.
     * @param array<string, int|float|string|bool> $settings Per-request ClickHouse settings applied to all parallel queries.
     * @return array<string|int, Statement> Results keyed by the same keys as the input array.
     * @throws ConnectionException
     * @throws QueryException
     */
    public function parallel(array $queries, ?Format $format = null, array $settings = []): array
    {
        $format ??= new JsonEachRow();

        $replicaConfig = $this->selectReplica();

        $handles = [];

        $responseHeaders = [];

        $multi = curl_multi_init();

        foreach ($queries as $key => $query) {
            $sql = $query instanceof Builder ? $query->toSql() : $query;
            $sql .= ' FORMAT ' . $format->name();

            $url = $this->buildUrl($replicaConfig, $sql, ['wait_end_of_query' => 1], $settings);

            $responseHeaders[$key] = [];

            $ch = curl_init();

            curl_setopt_array($ch, [
                CURLOPT_URL => $url,
                CURLOPT_POST => true,
                CURLOPT_POSTFIELDS => '',
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_TIMEOUT => $replicaConfig->timeout,
                CURLOPT_CONNECTTIMEOUT => $replicaConfig->connectTimeout,
                CURLOPT_HTTPHEADER => $this->authHeaders($replicaConfig),
                CURLOPT_ENCODING => '',
                CURLOPT_HEADERFUNCTION => function ($ch, string $header) use ($key, &$responseHeaders): int {
                    $parts = explode(':', $header, 2);

                    if (\count($parts) === 2) {
                        $responseHeaders[$key][trim($parts[0])] = trim($parts[1]);
                    }

                    return \strlen($header);
                },
            ]);

            $handles[$key] = $ch;

            curl_multi_add_handle($multi, $ch);
        }

        do {
            $status = curl_multi_exec($multi, $still_running);

            if ($still_running) {
                curl_multi_select($multi);
            }
        } while ($still_running && $status === CURLM_OK);

        $results = [];

        foreach ($handles as $key => $ch) {
            $body     = curl_multi_getcontent($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            $error    = curl_error($ch);

            curl_multi_remove_handle($multi, $ch);

            if ($body === null || !empty($error)) {
                curl_multi_close($multi);

                throw new ConnectionException("ClickHouse parallel query [{$key}] failed: {$error}");
            }

            if ($httpCode !== 200) {
                curl_multi_close($multi);

                $query = $queries[$key];
                $sql   = $query instanceof Builder ? $query->toSql() : $query;

                throw new QueryException(
                    "ClickHouse parallel query [{$key}] failed [{$httpCode}]: {$body}",
                    $sql
                );
            }

            $results[$key] = new Statement($body, $format, $responseHeaders[$key]);
        }

        curl_multi_close($multi);

        return $results;
    }

    /**
     * Stream a local file directly into ClickHouse without loading it into memory.
     *
     * The file is read in 64 kB chunks via CURLOPT_READFUNCTION and sent as a
     * chunked-transfer POST, so even multi-gigabyte files stay memory-efficient.
     *
     * Format defaults to CSV (CSVWithNames). Any Format whose encode/decode
     * contract matches the file's on-disk structure may be passed instead.
     *
     *   $client->insertFile('events', '/data/events.csv');
     *   $client->insertFile('logs',   '/data/logs.tsv', new TabSeparated());
     *
     * @see https://clickhouse.com/docs/en/interfaces/http#inserting-data
     *
     * @param string      $table  Target table name.
     * @param string      $path   Absolute or relative path to the file to stream.
     * @param Format|null $format File format — defaults to Csv (CSVWithNames).
     * @return bool
     * @throws \RuntimeException If the file does not exist or cannot be read.
     * @throws ConnectionException
     * @throws QueryException
     */
    public function insertFile(string $table, string $path, ?Format $format = null): bool
    {
        $format ??= new Csv();

        if (!is_file($path) || !is_readable($path)) {
            throw new \RuntimeException("Cannot open file for reading: {$path}");
        }

        $fh = fopen($path, 'rb');

        if ($fh === false) {
            throw new \RuntimeException("Cannot open file for reading: {$path}");
        }

        $sql = "INSERT INTO `{$table}` FORMAT " . $format->name();

        $url = $this->buildUrl($this->config, $sql, ['wait_end_of_query' => 1]);

        $responseHeaders = [];

        $headers   = $this->authHeaders($this->config);
        $headers[] = 'Transfer-Encoding: chunked';
        $headers[] = 'Expect:';

        $ch = curl_init();

        curl_setopt_array($ch, [
            CURLOPT_URL            => $url,
            CURLOPT_POST           => true,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT        => $this->config->timeout,
            CURLOPT_CONNECTTIMEOUT => $this->config->connectTimeout,
            CURLOPT_HTTPHEADER     => $headers,
            CURLOPT_ENCODING       => '',
            CURLOPT_READFUNCTION   => function ($ch, $infile, int $length) use ($fh): string {
                if ($length < 1) {
                    return '';
                }

                $chunk = fread($fh, $length);

                return $chunk === false ? '' : $chunk;
            },
            CURLOPT_HEADERFUNCTION => function ($ch, string $header) use (&$responseHeaders): int {
                $parts = explode(':', $header, 2);

                if (\count($parts) === 2) {
                    $responseHeaders[trim($parts[0])] = trim($parts[1]);
                }
                return \strlen($header);
            },
        ]);

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error    = curl_error($ch);

        fclose($fh);

        if ($response === false || !empty($error)) {
            throw new ConnectionException("ClickHouse file insert failed: {$error}");
        }

        if (!\is_string($response)) {
            throw new ConnectionException('ClickHouse file insert failed: unexpected response type');
        }

        if ($httpCode !== 200) {
            throw new QueryException(
                "ClickHouse file insert failed [{$httpCode}]: {$response}",
                $sql
            );
        }

        return true;
    }

    /**
     * Convert server-side query parameters to their param_name URL parameter form.
     *
     * ClickHouse server-side parameterized queries use {name:Type} placeholders
     * in SQL and expect corresponding param_name=value URL parameters. This
     * method prepends "param_" to each key so they are appended to the URL
     * correctly by buildUrl().
     *
     * @param array<string, int|float|string|bool> $params
     * @return array<string, int|float|string|bool>
     */
    private function prepareServerParams(array $params): array
    {
        $result = [];

        foreach ($params as $key => $value) {
            $result['param_' . $key] = $value;
        }

        return $result;
    }

    /**
     * Execute a SELECT query with one or more temporary in-memory tables sent as external data.
     *
     * External tables are transmitted as a multipart POST body alongside the query. Each
     * table is available by name within the SQL for the duration of the request — useful
     * for JOINs against a small in-memory lookup set without creating a permanent table.
     *
     * Example — filter by a dynamic list of IDs:
     *   $result = $client->queryWithExternalData(
     *       'SELECT * FROM events WHERE user_id IN (SELECT id FROM allowed_users)',
     *       externalTables: [
     *           ExternalTable::fromRows('allowed_users', 'id UInt64', [
     *               ['id' => 1],
     *               ['id' => 2],
     *           ]),
     *       ],
     *   );
     *
     * Example — JOIN against an in-memory lookup:
     *   $client->queryWithExternalData(
     *       'SELECT e.ts, l.label FROM events e JOIN labels l ON e.type = l.id',
     *       externalTables: [
     *           new ExternalTable('labels', 'id UInt8, label String', "1\tclick\n2\tview"),
     *       ],
     *   );
     *
     * @see https://clickhouse.com/docs/en/engines/table-engines/special/external-data
     *
     * @param string $sql           SQL query referencing one or more external table names.
     * @param array<int, ExternalTable> $externalTables External table definitions and data.
     * @param array<string, mixed>  $bindings Client-side :name placeholder values.
     * @param Format|null           $format   Response format — defaults to JsonEachRow.
     * @param array<string, int|float|string|bool> $settings Per-request ClickHouse settings.
     * @return Statement
     * @throws ConnectionException
     * @throws QueryException
     */
    public function queryWithExternalData(
        string $sql,
        array $externalTables,
        array $bindings = [],
        ?Format $format = null,
        array $settings = [],
    ): Statement {
        $format ??= new JsonEachRow();

        $sql = $this->bindParams($sql, $bindings) . ' FORMAT ' . $format->name();

        $result = $this->sendExternalData($this->selectReplica(), $sql, $externalTables, $settings);

        return new Statement($result['body'], $format, $result['headers']);
    }

    /**
     * Stream rows from a resource or Generator into a ClickHouse table.
     *
     * Generalises insertFile() by accepting any PHP resource (open file handle)
     * or a Generator that yields associative row arrays. Data is streamed via
     * chunked-transfer POST so even large sources stay memory-efficient.
     *
     * Resource form — stream a pre-formatted file handle directly:
     *   $fh = fopen('/data/events.ndjson', 'rb');
     *   $client->insertStream('events', $fh, new JsonEachRow());
     *
     * Generator form — encode rows lazily as the generator yields them:
     *   $client->insertStream('events', (function () {
     *       foreach ($rows as $row) { yield $row; }
     *   })(), new JsonEachRow());
     *
     * Note: the Generator form encodes one row at a time using
     * Format::encode([$row]). Formats that emit header rows on every
     * encode() call (e.g. CSVWithNames) are not suitable for the Generator
     * form — use the resource form with a pre-formatted file instead.
     *
     * @see https://clickhouse.com/docs/en/interfaces/http#inserting-data
     *
     * @param string        $table    Target table name.
     * @param resource|\Generator $source A readable resource or a Generator yielding associative row arrays.
     * @param Format|null   $format   Format for encoding (Generator form) or describing the resource data. Defaults to JsonEachRow.
     * @param array<string, int|float|string|bool> $settings Per-request ClickHouse settings.
     * @return bool
     * @throws \InvalidArgumentException If $source is neither a resource nor a Generator.
     * @throws ConnectionException
     * @throws QueryException
     */
    public function insertStream(string $table, mixed $source, ?Format $format = null, array $settings = []): bool
    {
        $format ??= new JsonEachRow();

        $sql = "INSERT INTO `{$table}` FORMAT " . $format->name();
        $url = $this->buildUrl($this->config, $sql, ['wait_end_of_query' => 1], $settings);

        $headers   = $this->authHeaders($this->config);
        $headers[] = 'Transfer-Encoding: chunked';
        $headers[] = 'Expect:';

        if (\is_resource($source)) {
            $readFunction = function ($ch, $fd, int $length) use ($source): string {
                if ($length < 1) {
                    return '';
                }

                $chunk = fread($source, $length);

                return $chunk === false ? '' : $chunk;
            };
        } elseif ($source instanceof \Generator) {
            $buffer = '';

            $readFunction = function ($ch, $fd, int $length) use ($format, $source, &$buffer): string {
                while (\strlen($buffer) < $length && $source->valid()) {
                    $yielded = $source->current();
                    $source->next();
                    if (\is_array($yielded)) {
                        $row = [];
                        foreach ($yielded as $k => $v) {
                            if (\is_string($k)) {
                                $row[$k] = $v;
                            }
                        }
                        $buffer .= $format->encode([$row]) . "\n";
                    }
                }

                if ($buffer === '') {
                    return '';
                }

                $chunk  = substr($buffer, 0, $length);
                $buffer = substr($buffer, $length);

                return $chunk;
            };
        } else {
            throw new \InvalidArgumentException(
                'insertStream() expects a resource or \\Generator, got ' . \get_debug_type($source)
            );
        }

        $responseHeaders = [];

        $ch = curl_init();

        curl_setopt_array($ch, [
            CURLOPT_URL            => $url,
            CURLOPT_POST           => true,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT        => $this->config->timeout,
            CURLOPT_CONNECTTIMEOUT => $this->config->connectTimeout,
            CURLOPT_HTTPHEADER     => $headers,
            CURLOPT_ENCODING       => '',
            CURLOPT_READFUNCTION   => $readFunction,
            CURLOPT_HEADERFUNCTION => function ($ch, string $header) use (&$responseHeaders): int {
                $parts = explode(':', $header, 2);

                if (\count($parts) === 2) {
                    $responseHeaders[trim($parts[0])] = trim($parts[1]);
                }

                return \strlen($header);
            },
        ]);

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error    = curl_error($ch);

        curl_close($ch);

        if ($response === false || !empty($error)) {
            throw new ConnectionException("ClickHouse stream insert failed: {$error}");
        }

        if (!\is_string($response)) {
            throw new ConnectionException('ClickHouse stream insert failed: unexpected response type');
        }

        if ($httpCode !== 200) {
            throw new QueryException(
                "ClickHouse stream insert failed [{$httpCode}]: {$response}",
                $sql
            );
        }

        return true;
    }

    /**
     * Substitute named placeholders (:name) in a SQL string.
     *
     * @param string $sql
     * @param array<string, mixed>  $bindings
     * @return string
     */
    private function bindParams(string $sql, array $bindings): string
    {
        if (empty($bindings)) {
            return $sql;
        }

        foreach ($bindings as $key => $value) {
            $placeholder = ':' . ltrim((string) $key, ':');
            $sql = str_replace($placeholder, $this->escape($value), $sql);
        }

        return $sql;
    }

    /**
     * Execute a query with external data tables sent as a multipart POST.
     *
     * Each ExternalTable is attached as a named multipart part (the part name
     * becomes the ClickHouse temporary table name). Structure and format are
     * passed as URL parameters following the <name>_structure / <name>_format
     * convention. A fresh curl handle is used because multipart requires
     * CURLOPT_POSTFIELDS to be an array, not a string.
     *
     * @param Config $config
     * @param string $sql
     * @param array<int, ExternalTable> $externalTables
     * @param array<string, int|float|string|bool> $settings
     * @return array{body: string, headers: array<string, string>}
     * @throws ConnectionException
     * @throws QueryException
     */
    private function sendExternalData(Config $config, string $sql, array $externalTables, array $settings = []): array
    {
        $extraParams = ['wait_end_of_query' => 1];

        foreach ($externalTables as $table) {
            $extraParams[$table->name . '_structure'] = $table->structure;
            $extraParams[$table->name . '_format']    = $table->format;
        }

        $url = $this->buildUrl($config, $sql, $extraParams, $settings);

        $postFields = [];

        foreach ($externalTables as $table) {
            $postFields[$table->name] = new \CURLStringFile($table->data, $table->name, 'text/plain');
        }

        $responseHeaders = [];

        $ch = curl_init();

        curl_setopt_array($ch, [
            CURLOPT_URL            => $url,
            CURLOPT_POST           => true,
            CURLOPT_POSTFIELDS     => $postFields,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT        => $config->timeout,
            CURLOPT_CONNECTTIMEOUT => $config->connectTimeout,
            CURLOPT_HTTPHEADER     => $this->authHeaders($config),
            CURLOPT_ENCODING       => '',
            CURLOPT_HEADERFUNCTION => function ($ch, string $header) use (&$responseHeaders): int {
                $parts = explode(':', $header, 2);

                if (\count($parts) === 2) {
                    $responseHeaders[trim($parts[0])] = trim($parts[1]);
                }

                return \strlen($header);
            },
        ]);

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error    = curl_error($ch);

        curl_close($ch);

        if ($response === false || !empty($error)) {
            throw new ConnectionException("ClickHouse external data query failed: {$error}");
        }

        if (!\is_string($response)) {
            throw new ConnectionException('ClickHouse external data query failed: unexpected response type');
        }

        if ($httpCode !== 200) {
            throw new QueryException(
                "ClickHouse external data query failed [{$httpCode}]: {$response}",
                $sql
            );
        }

        return ['body' => $response, 'headers' => $responseHeaders];
    }

    /**
     * Build the full request URL with all query parameters.
     *
     * Merges base params (database, query, any extras like wait_end_of_query)
     * with global Config settings and per-request settings. Roles are appended
     * individually as separate `role` parameters since http_build_query does
     * not produce the bare `role=x&role=y` form that ClickHouse expects.
     *
     * @param Config $config
     * @param string $sql
     * @param array<string, mixed> $extraParams Additional fixed URL params (e.g. wait_end_of_query, query_id).
     * @param array<string, int|float|string|bool> $settings Per-request settings merged on top of Config::$settings.
     * @return non-empty-string
     */
    private function buildUrl(Config $config, string $sql, array $extraParams = [], array $settings = []): string
    {
        $params = array_merge(
            ['database' => $config->database, 'query' => $sql],
            $extraParams,
            $config->settings,
            $settings,
        );

        if ($config->profile !== null) {
            $params['profile'] = $config->profile;
        }

        if ($config->quotaKey !== null) {
            $params['quota_key'] = $config->quotaKey;
        }

        $url = $config->dataSource() . '/?' . http_build_query($params);

        foreach ($config->roles as $role) {
            $url .= '&role=' . urlencode($role);
        }

        return $url;
    }

    /**
     * Return the standard ClickHouse authentication headers used by every request.
     *
     * @param Config $config
     * @return list<string>
     */
    private function authHeaders(Config $config): array
    {
        return [
            'X-ClickHouse-User: ' . $config->username,
            'X-ClickHouse-Key: '  . $config->password,
            'Content-Type: text/plain',
        ];
    }

    /**
     * Escape a PHP value for safe inclusion in a ClickHouse SQL string.
     *
     * @param mixed $value
     * @return string
     */
    private function escape(mixed $value): string
    {
        if ($value === null) {
            return 'NULL';
        }
        if (\is_bool($value)) {
            return $value ? '1' : '0';
        }
        if (\is_int($value)) {
            return (string) $value;
        }
        if (\is_float($value)) {
            return (string) $value;
        }
        if (\is_array($value)) {
            return '[' . implode(', ', array_map($this->escape(...), $value)) . ']';
        }
        if (\is_string($value)) {
            return "'" . str_replace(["\\", "'"], ["\\\\", "\\'"], $value) . "'";
        }
        // Object or resource: delegate to string representation via Stringable or fallback.
        $str = $value instanceof \Stringable ? (string) $value : get_debug_type($value);
        return "'" . str_replace(["\\", "'"], ["\\\\", "\\'"], $str) . "'";
    }

    /**
     * Execute a raw HTTP request against the ClickHouse HTTP interface.
     *
     * The SQL is always sent as the `query` URL parameter. `wait_end_of_query=1`
     * ensures the server buffers the full response before sending headers, so
     * HTTP 200 genuinely means the query succeeded. Every request uses POST —
     * ClickHouse treats GET as readonly (Code 164) and rejects DDL/DML over it.
     * For INSERT statements $body carries the row data; it is empty otherwise.
     *
     * When $config->retries > 0, connection failures are retried up to that many
     * extra times with $config->retryDelay milliseconds between attempts.
     *
     * When $config->compression is true, non-empty bodies are gzip-compressed
     * before sending. Responses are always decompressed by cURL automatically.
     *
     * @param Config $config
     * @param string $sql
     * @param string $body
     * @param array<string, int|float|string|bool> $settings Per-request settings merged with global Config settings.
     * @param (callable(array<string, string>): void)|null $onProgress Optional progress callback.
     * @return array{body: string, headers: array<string, string>}
     * @throws ConnectionException
     * @throws QueryException
     */
    private function send(Config $config, string $sql, string $body = '', array $settings = [], ?callable $onProgress = null): array
    {
        $lastException = new ConnectionException('ClickHouse connection failed');

        for ($attempt = 0; $attempt <= $config->retries; $attempt++) {
            if ($attempt > 0) {
                usleep($config->retryDelay * 1000);
            }

            try {
                return $this->attempt($config, $sql, $body, $settings, $onProgress);
            } catch (ConnectionException $e) {
                $lastException = $e;
            }
        }

        throw $lastException;
    }

    /**
     * Perform a single HTTP attempt.
     *
     * @param Config $config
     * @param string $sql
     * @param string $body
     * @param array<string, int|float|string|bool> $settings Per-request settings.
     * @param (callable(array<string, string>): void)|null $onProgress Optional progress callback.
     * @return array{body: string, headers: array<string, string>}
     * @throws ConnectionException
     * @throws QueryException
     */
    private function attempt(Config $config, string $sql, string $body, array $settings = [], ?callable $onProgress = null): array
    {
        $url = $this->buildUrl($config, $sql, ['wait_end_of_query' => 1], $settings);

        $headers = $this->authHeaders($config);

        if ($config->compression && $body !== '') {
            $compressed = gzencode($body, 1);

            if ($compressed !== false) {
                $body = $compressed;
                $headers[] = 'Content-Encoding: gzip';
            }
        }

        $responseHeaders = [];

        $curl = $this->acquireHandle();

        try {
            curl_setopt_array($curl, [
                CURLOPT_URL => $url,
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_TIMEOUT => $config->timeout,
                CURLOPT_CONNECTTIMEOUT => $config->connectTimeout,
                CURLOPT_HTTPHEADER => $headers,
                CURLOPT_ENCODING => '',
                CURLOPT_HEADERFUNCTION => function ($curl, string $header) use (&$responseHeaders, $onProgress): int {
                    $parts = explode(':', $header, 2);

                    if (\count($parts) === 2) {
                        $name  = trim($parts[0]);
                        $value = trim($parts[1]);
                        $responseHeaders[$name] = $value;

                        if ($onProgress !== null && $name === 'X-ClickHouse-Progress') {
                            $progress = json_decode($value, true);
                            if (\is_array($progress)) {
                                /** @var array<string, string> $progress */
                                $onProgress($progress);
                            }
                        }
                    }
                    return \strlen($header);
                },
            ]);

            curl_setopt($curl, CURLOPT_POST, true);
            curl_setopt($curl, CURLOPT_POSTFIELDS, $body);

            $response = curl_exec($curl);
            $httpCode = curl_getinfo($curl, CURLINFO_HTTP_CODE);
            $error    = curl_error($curl);
        } finally {
            $this->releaseHandle($curl);
        }

        if ($response === false || !empty($error)) {
            throw new ConnectionException(
                "ClickHouse connection failed: {$error}"
            );
        }

        if (!\is_string($response)) {
            throw new ConnectionException('ClickHouse connection failed: unexpected response type');
        }

        if ($httpCode !== 200) {
            throw new QueryException(
                "ClickHouse query failed [{$httpCode}]: {$response}",
                $sql
            );
        }

        return ['body' => $response, 'headers' => $responseHeaders];
    }
}
