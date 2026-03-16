<?php

namespace Beeterty\ClickHouse\Tests\Integration;

use Beeterty\ClickHouse\Client;
use Beeterty\ClickHouse\Config;
use PHPUnit\Framework\TestCase;

/**
 * Base class for all integration tests.
 *
 * Reads connection settings from environment variables so the same
 * tests work both locally and inside the GitHub Actions ClickHouse service:
 *
 *   CLICKHOUSE_HOST   (default: 127.0.0.1)
 *   CLICKHOUSE_PORT   (default: 8123)
 *   CLICKHOUSE_DB     (default: default)
 *   CLICKHOUSE_USER   (default: default)
 *   CLICKHOUSE_PASS   (default: '')
 */
abstract class IntegrationTestCase extends TestCase
{
    protected Client $client;

    protected function setUp(): void
    {
        $this->client = new Client(new Config(
            host: (string)  (getenv('CLICKHOUSE_HOST') ?: '127.0.0.1'),
            port: (int)     (getenv('CLICKHOUSE_PORT') ?: 8123),
            database: (string)  (getenv('CLICKHOUSE_DB')   ?: 'default'),
            username: (string)  (getenv('CLICKHOUSE_USERNAME') ?: 'default'),
            password: (string)  (getenv('CLICKHOUSE_PASSWORD') ?: ''),
        ));
    }

    /**
     * Drop a table silently — used in tearDown() so cleanup always runs
     * even when a test assertion fails mid-way.
     */
    protected function dropTableSilently(string $table): void
    {
        try {
            $this->client->schema()->dropIfExists($table);
        } catch (\Throwable) {
            // Intentionally swallowed — best-effort cleanup.
        }
    }
}
