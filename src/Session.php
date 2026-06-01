<?php

namespace Beeterty\ClickHouse;

use Beeterty\ClickHouse\Format\Contracts\Format;
use Beeterty\ClickHouse\Query\Statement;

/**
 * A ClickHouse HTTP session.
 *
 * A session ties multiple requests together with a shared `session_id` so that
 * temporary tables, session-scoped settings, and other stateful constructs
 * persist across calls within the same session lifetime.
 *
 * Obtain a Session via Client::session() rather than constructing it directly:
 *
 *   $session = $client->session('my-session', timeout: 300);
 *   $session->execute('CREATE TEMPORARY TABLE tmp (id UInt64) ENGINE = Memory');
 *   $session->execute('INSERT INTO tmp VALUES (1), (2), (3)');
 *   $rows = $session->query('SELECT * FROM tmp')->rows();
 *
 * Sessions are identified by a string ID and expire after a configurable
 * inactivity timeout (default: 60 seconds). ClickHouse allows only one
 * concurrent request per session_id; parallel requests on the same session
 * are serialised on the server side.
 *
 * Note: the fluent query builder (table() / where() / get()) is not available
 * on Session because Builder dispatches directly through Client. To combine
 * query building with a session, compile the SQL first and pass it to query():
 *
 *   $sql = $client->table('tmp')->where('id', '>', 1)->toSql();
 *   $session->query($sql)->rows();
 *
 * @see https://clickhouse.com/docs/en/interfaces/http#http-sessions
 */
class Session
{
    /**
     * Create a new Session instance.
     *
     * @param Client $client  The underlying ClickHouse client.
     * @param string $id      Session identifier — any non-empty string.
     * @param int    $timeout Inactivity timeout in seconds before the session expires (default: 60).
     */
    public function __construct(
        private readonly Client $client,
        private readonly string $id,
        private readonly int $timeout = 60,
    ) {
        //
    }

    /**
     * Return the session identifier.
     *
     * @return string
     */
    public function id(): string
    {
        return $this->id;
    }

    /**
     * Execute a SELECT query within this session and return a Statement.
     *
     * Temporary tables created earlier in the same session are accessible.
     *
     * Example:
     *   $rows = $session->query('SELECT * FROM tmp')->rows();
     *
     * @see https://clickhouse.com/docs/en/interfaces/http#http-sessions
     *
     * @param string $sql      SQL query, optionally with :name client-side or {name:Type} server-side placeholders.
     * @param array<string, mixed> $bindings Client-side named placeholder values.
     * @param Format|null $format  Response format — defaults to JsonEachRow.
     * @param array<string, int|float|string|bool> $settings Per-request ClickHouse settings.
     * @param array<string, int|float|string|bool> $params   Server-side query parameters for {name:Type} placeholders.
     * @param (callable(array<string, string>): void)|null $onProgress Optional progress callback.
     * @return Statement
     */
    public function query(
        string $sql,
        array $bindings = [],
        ?Format $format = null,
        array $settings = [],
        array $params = [],
        ?callable $onProgress = null,
    ): Statement {
        return $this->client->query($sql, $bindings, $format, $this->mergeSettings($settings), $params, $onProgress);
    }

    /**
     * Execute a DDL or DML statement within this session.
     *
     * Temporary tables created in this call are available to subsequent
     * requests on the same session.
     *
     * Example:
     *   $session->execute('CREATE TEMPORARY TABLE tmp (id UInt64) ENGINE = Memory');
     *   $session->execute('INSERT INTO tmp VALUES (1), (2)');
     *
     * @see https://clickhouse.com/docs/en/interfaces/http#http-sessions
     *
     * @param string $sql      DDL or DML statement, optionally with :name placeholders.
     * @param array<string, mixed> $bindings Named placeholder values.
     * @param array<string, int|float|string|bool> $settings Per-request ClickHouse settings.
     * @param (callable(array<string, string>): void)|null $onProgress Optional progress callback.
     * @return bool
     */
    public function execute(string $sql, array $bindings = [], array $settings = [], ?callable $onProgress = null): bool
    {
        return $this->client->execute($sql, $bindings, $this->mergeSettings($settings), [], $onProgress);
    }

    /**
     * Insert rows into a table within this session.
     *
     * @see https://clickhouse.com/docs/en/interfaces/http#http-sessions
     *
     * @param string      $table    Target table name.
     * @param array<int, array<string, mixed>> $rows Array of associative row arrays.
     * @param Format|null $format   Encoding format — defaults to JsonEachRow.
     * @param array<string, int|float|string|bool> $settings Per-request ClickHouse settings.
     * @return bool
     */
    public function insert(string $table, array $rows, ?Format $format = null, array $settings = []): bool
    {
        return $this->client->insert($table, $rows, $format, $this->mergeSettings($settings));
    }

    /**
     * Merge session URL parameters on top of any caller-supplied settings.
     *
     * session_id and session_timeout are sent as URL parameters, which
     * is exactly what the $settings path in Client::buildUrl() does.
     *
     * @param array<string, int|float|string|bool> $settings
     * @return array<string, int|float|string|bool>
     */
    private function mergeSettings(array $settings): array
    {
        return array_merge(
            ['session_id' => $this->id, 'session_timeout' => $this->timeout],
            $settings,
        );
    }
}
