<?php

namespace Beeterty\ClickHouse\Tests\Unit;

use Beeterty\ClickHouse\Client;
use Beeterty\ClickHouse\Config;
use Beeterty\ClickHouse\ExternalTable;
use Beeterty\ClickHouse\Format\JsonEachRow;
use Beeterty\ClickHouse\Format\TabSeparated;
use PHPUnit\Framework\TestCase;

class ExternalTableTest extends TestCase
{
    // ─── Constructor form ─────────────────────────────────────────────────────

    public function test_constructor_sets_all_fields(): void
    {
        $table = new ExternalTable('users', 'id UInt64, name String', "1\tAlice\n2\tBob");

        $this->assertSame('users', $table->name);
        $this->assertSame('id UInt64, name String', $table->structure);
        $this->assertSame("1\tAlice\n2\tBob", $table->data);
        $this->assertSame('TabSeparated', $table->format);
    }

    public function test_constructor_custom_format(): void
    {
        $table = new ExternalTable('t', 'id UInt64', '{"id":1}', 'JSONEachRow');

        $this->assertSame('JSONEachRow', $table->format);
    }

    // ─── fromRows() ───────────────────────────────────────────────────────────

    public function test_from_rows_defaults_to_json_each_row(): void
    {
        $table = ExternalTable::fromRows(
            'lookup',
            'id UInt64, label String',
            [['id' => 1, 'label' => 'foo']],
        );

        $this->assertSame('lookup', $table->name);
        $this->assertSame('id UInt64, label String', $table->structure);
        $this->assertSame('JSONEachRow', $table->format);
        $this->assertStringContainsString('"id"', $table->data);
        $this->assertStringContainsString('"label"', $table->data);
    }

    public function test_from_rows_encodes_multiple_rows(): void
    {
        $table = ExternalTable::fromRows(
            'users',
            'id UInt64, name String',
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
            ],
        );

        $lines = array_filter(explode("\n", trim($table->data)));
        $this->assertCount(2, $lines);
    }

    public function test_from_rows_uses_provided_format(): void
    {
        $format = new JsonEachRow();
        $table  = ExternalTable::fromRows(
            'events',
            'id UInt64',
            [['id' => 42]],
            $format,
        );

        $this->assertSame('JSONEachRow', $table->format);
        $decoded = json_decode($table->data, true);
        $this->assertSame(42, $decoded['id']);
    }

    public function test_from_rows_empty_rows_produces_empty_data(): void
    {
        $table = ExternalTable::fromRows('t', 'id UInt64', []);

        $this->assertSame('', $table->data);
    }

    // ─── Client integration ───────────────────────────────────────────────────

    public function test_query_with_external_data_accepts_external_tables(): void
    {
        $client = new Client(new Config(host: '127.0.0.1', port: 1));

        try {
            $client->queryWithExternalData(
                'SELECT * FROM lookup',
                externalTables: [
                    ExternalTable::fromRows('lookup', 'id UInt64', [['id' => 1]]),
                ],
            );
        } catch (\InvalidArgumentException $e) {
            $this->fail('queryWithExternalData() should not throw InvalidArgumentException');
        } catch (\Throwable) {
            // Connection failure expected — not an argument error.
        }

        $this->assertTrue(true);
    }

    public function test_query_with_external_data_accepts_empty_tables_array(): void
    {
        $client = new Client(new Config(host: '127.0.0.1', port: 1));

        try {
            $client->queryWithExternalData('SELECT 1', externalTables: []);
        } catch (\InvalidArgumentException $e) {
            $this->fail('queryWithExternalData() should not throw InvalidArgumentException');
        } catch (\Throwable) {
            // Connection failure expected.
        }

        $this->assertTrue(true);
    }
}
