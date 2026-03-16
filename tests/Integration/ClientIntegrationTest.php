<?php

namespace Beeterty\ClickHouse\Tests\Integration;

use Beeterty\ClickHouse\Exception\QueryException;
use Beeterty\ClickHouse\Schema\Blueprint;
use Beeterty\ClickHouse\Schema\Engine\MergeTree;

class ClientIntegrationTest extends IntegrationTestCase
{
    private string $table = 'integration_client_test';

    protected function setUp(): void
    {
        parent::setUp();

        $this->client->schema()->createIfNotExists($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->string('name');
            $t->uint32('score')->default(0);
            $t->engine(new MergeTree())->orderBy(['id']);
        });
    }

    protected function tearDown(): void
    {
        $this->dropTableSilently($this->table);
    }

    // ─── ping() ───────────────────────────────────────────────────────────────

    public function test_ping_returns_true_when_server_is_reachable(): void
    {
        $this->assertTrue($this->client->ping());
    }

    // ─── query() ──────────────────────────────────────────────────────────────

    public function test_query_select_one(): void
    {
        $result = $this->client->query('SELECT 1 AS n')->value();

        $this->assertSame(1, $result);
    }

    public function test_query_returns_statement_with_rows(): void
    {
        $this->client->insert($this->table, [
            ['id' => 1, 'name' => 'Alice', 'score' => 10],
            ['id' => 2, 'name' => 'Bob',   'score' => 20],
        ]);

        $stmt = $this->client->query("SELECT * FROM {$this->table} ORDER BY id");

        $this->assertCount(2, $stmt);
        $this->assertSame('Alice', $stmt->first()['name']);
    }

    public function test_query_with_bindings(): void
    {
        $this->client->insert($this->table, [
            ['id' => 10, 'name' => 'Charlie', 'score' => 99],
        ]);

        $stmt = $this->client->query(
            "SELECT name FROM {$this->table} WHERE id = :id",
            ['id' => 10],
        );

        $this->assertSame('Charlie', $stmt->value());
    }

    public function test_query_with_string_binding(): void
    {
        $this->client->insert($this->table, [
            ['id' => 11, 'name' => "O'Brien", 'score' => 5],
        ]);

        $stmt = $this->client->query(
            "SELECT id FROM {$this->table} WHERE name = :name",
            ['name' => "O'Brien"],
        );

        $this->assertSame(11, $stmt->value());
    }

    // ─── value() ──────────────────────────────────────────────────────────────

    public function test_value_returns_scalar_from_aggregate(): void
    {
        $this->client->insert($this->table, [
            ['id' => 20, 'name' => 'X', 'score' => 1],
            ['id' => 21, 'name' => 'Y', 'score' => 2],
            ['id' => 22, 'name' => 'Z', 'score' => 3],
        ]);

        $count = $this->client
            ->query("SELECT count() FROM {$this->table}")
            ->value();

        $this->assertGreaterThanOrEqual(3, $count);
    }

    // ─── pluck() ──────────────────────────────────────────────────────────────

    public function test_pluck_returns_flat_column_array(): void
    {
        $this->client->insert($this->table, [
            ['id' => 30, 'name' => 'Foo', 'score' => 0],
            ['id' => 31, 'name' => 'Bar', 'score' => 0],
        ]);

        $names = $this->client
            ->query("SELECT name FROM {$this->table} WHERE id IN (30, 31) ORDER BY id")
            ->pluck('name');

        $this->assertSame(['Foo', 'Bar'], $names);
    }

    // ─── insert() ─────────────────────────────────────────────────────────────

    public function test_insert_returns_true(): void
    {
        $result = $this->client->insert($this->table, [
            ['id' => 40, 'name' => 'Insert Test', 'score' => 0],
        ]);

        $this->assertTrue($result);
    }

    public function test_inserted_rows_are_queryable(): void
    {
        $this->client->insert($this->table, [
            ['id' => 50, 'name' => 'Persisted', 'score' => 77],
        ]);

        // ClickHouse writes are eventually consistent — force a sync
        $this->client->execute("OPTIMIZE TABLE {$this->table} FINAL");

        $row = $this->client
            ->query("SELECT * FROM {$this->table} WHERE id = 50")
            ->first();

        $this->assertNotNull($row);
        $this->assertSame('Persisted', $row['name']);
        $this->assertSame(77, $row['score']);
    }

    // ─── execute() ────────────────────────────────────────────────────────────

    public function test_execute_returns_true(): void
    {
        $result = $this->client->execute("OPTIMIZE TABLE {$this->table} FINAL");

        $this->assertTrue($result);
    }

    // ─── Error handling ───────────────────────────────────────────────────────

    public function test_query_throws_query_exception_on_bad_sql(): void
    {
        $this->expectException(QueryException::class);

        $this->client->query('SELECT * FROM table_that_does_not_exist_xyz');
    }

    public function test_query_exception_contains_sql(): void
    {
        $sql = 'SELECT * FROM no_such_table_abc';

        try {
            $this->client->query($sql);
            $this->fail('QueryException was not thrown');
        } catch (QueryException $e) {
            $this->assertSame($sql . ' FORMAT JSONEachRow', $e->sql);
        }
    }

    // ─── Statement metadata ───────────────────────────────────────────────────

    public function test_query_id_is_populated(): void
    {
        $stmt = $this->client->query('SELECT 1');

        $this->assertNotNull($stmt->queryId());
        $this->assertNotEmpty($stmt->queryId());
    }

    public function test_summary_is_populated(): void
    {
        $stmt = $this->client->query('SELECT 1');

        $this->assertIsArray($stmt->summary());
    }
}
