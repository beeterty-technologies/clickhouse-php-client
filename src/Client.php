<?php

namespace Beeterty\ClickHouse;

use Beeterty\ClickHouse\Exception\{ConnectionException, QueryException};
use Beeterty\ClickHouse\Format\Contracts\Format;
use Beeterty\ClickHouse\Format\JsonEachRow;
use CurlHandle;

class Client
{
    /**
     * The cURL handle for the ClickHouse client.
     *
     * @var CurlHandle
     */
    private CurlHandle $curl;

    /**
     * Create a new ClickHouse client instance.
     *
     * @param Config $config
     */
    public function __construct(
        private readonly Config $config
    ) {
        $this->curl = curl_init();
    }

    /**
     * Ping the ClickHouse server to check if it's reachable and responsive.
     *
     * @return bool
     */
    public function ping(): bool
    {
        try {
            $response = $this->send('SELECT 1');

            return trim($response) === '1';
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * Execute a SELECT query and return a Statement wrapping the result rows.
     *
     * @param string  $sql
     * @param array   $bindings  Named placeholders: [':name' => 'Alice', ':age' => 30]
     * @param Format|null $format  Defaults to JsonEachRow
     * @return Statement
     * @throws ConnectionException
     * @throws QueryException
     */
    public function query(string $sql, array $bindings = [], ?Format $format = null): Statement
    {
        $format = $format ?? new JsonEachRow();
        $sql    = $this->bindParams($sql, $bindings) . ' FORMAT ' . $format->name();

        return new Statement($this->send($sql), $format);
    }

    /**
     * Insert rows into a table.
     *
     * @param string  $table
     * @param array   $rows   Array of associative arrays
     * @param Format|null $format  Defaults to JsonEachRow
     * @return bool
     * @throws ConnectionException
     * @throws QueryException
     */
    public function insert(string $table, array $rows, ?Format $format = null): bool
    {
        $format = $format ?? new JsonEachRow();
        $sql = "INSERT INTO {$table} FORMAT " . $format->name();

        $this->send($sql, $format->encode($rows));

        return true;
    }

    /**
     * Execute a DDL or DML statement (CREATE, ALTER, DROP, etc.).
     *
     * @param string $sql
     * @param array  $bindings
     * @return bool
     * @throws ConnectionException
     * @throws QueryException
     */
    public function execute(string $sql, array $bindings = []): bool
    {
        $this->send($this->bindParams($sql, $bindings));

        return true;
    }

    /**
     * Substitute named placeholders (:name) in a SQL string.
     *
     * @param string $sql
     * @param array  $bindings
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
     * Escape a PHP value for safe inclusion in a ClickHouse SQL string.
     *
     * @param mixed $value
     * @return string
     */
    private function escape(mixed $value): string
    {
        return match (true) {
            $value === null => 'NULL',
            \is_bool($value) => $value ? '1' : '0',
            \is_int($value) => (string) $value,
            \is_float($value) => (string) $value,
            \is_array($value) => '[' . implode(', ', array_map($this->escape(...), $value)) . ']',
            default => "'" . str_replace(["\\", "'"], ["\\\\", "\\'"], (string) $value) . "'",
        };
    }

    /**
     * Execute a raw HTTP request against the ClickHouse HTTP interface.
     *
     * The SQL is always sent as the `query` URL parameter. When $body is
     * non-empty (e.g. INSERT data) a POST request is made; otherwise GET.
     *
     * @param string $sql
     * @param string $body
     * @return string
     * @throws ConnectionException
     * @throws QueryException
     */
    private function send(string $sql, string $body = ''): string
    {
        $url = $this->config->dataSource() . '/?' . http_build_query([
            'database' => $this->config->database,
            'query' => $sql,
        ]);

        curl_setopt_array($this->curl, [
            CURLOPT_URL => $url,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT => $this->config->timeout,
            CURLOPT_CONNECTTIMEOUT => $this->config->connectTimeout,
            CURLOPT_HTTPHEADER => [
                'X-ClickHouse-User: ' . $this->config->username,
                'X-ClickHouse-Key: '  . $this->config->password,
                'Content-Type: text/plain',
            ],
        ]);

        if (!empty($body)) {
            curl_setopt($this->curl, CURLOPT_POST, true);
            curl_setopt($this->curl, CURLOPT_POSTFIELDS, $body);
        } else {
            curl_setopt($this->curl, CURLOPT_HTTPGET, true);
        }

        $response = curl_exec($this->curl);
        $httpCode = curl_getinfo($this->curl, CURLINFO_HTTP_CODE);
        $error = curl_error($this->curl);

        if ($response === false || !empty($error)) {
            throw new ConnectionException(
                "ClickHouse connection failed: {$error}"
            );
        }

        if ($httpCode !== 200) {
            throw new QueryException(
                "ClickHouse query failed [{$httpCode}]: {$response}",
                $sql
            );
        }

        return $response;
    }
}
