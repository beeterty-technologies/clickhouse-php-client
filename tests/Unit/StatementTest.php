<?php

namespace Beeterty\ClickHouse\Tests\Unit;

use Beeterty\ClickHouse\Format\JsonEachRow;
use Beeterty\ClickHouse\Statement;
use PHPUnit\Framework\TestCase;

class StatementTest extends TestCase
{
    // ─── Helpers ──────────────────────────────────────────────────────────────

    private function make(string $raw, array $headers = []): Statement
    {
        return new Statement($raw, new JsonEachRow(), $headers);
    }

    // ─── rows() ───────────────────────────────────────────────────────────────

    public function test_rows_returns_decoded_rows(): void
    {
        $stmt = $this->make("{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}");

        $this->assertCount(2, $stmt->rows());
        $this->assertSame(['id' => 1, 'name' => 'Alice'], $stmt->rows()[0]);
        $this->assertSame(['id' => 2, 'name' => 'Bob'], $stmt->rows()[1]);
    }

    public function test_rows_returns_empty_array_for_empty_response(): void
    {
        $stmt = $this->make('');

        $this->assertSame([], $stmt->rows());
    }

    public function test_rows_are_decoded_once_and_cached(): void
    {
        $stmt = $this->make('{"id":1}');

        $this->assertSame($stmt->rows(), $stmt->rows());
    }

    // ─── first() ──────────────────────────────────────────────────────────────

    public function test_first_returns_first_row(): void
    {
        $stmt = $this->make("{\"id\":1}\n{\"id\":2}");

        $this->assertSame(['id' => 1], $stmt->first());
    }

    public function test_first_returns_null_when_empty(): void
    {
        $stmt = $this->make('');

        $this->assertNull($stmt->first());
    }

    // ─── value() ──────────────────────────────────────────────────────────────

    public function test_value_returns_scalar_from_first_column(): void
    {
        $stmt = $this->make('{"count()":42}');

        $this->assertSame(42, $stmt->value());
    }

    public function test_value_returns_first_column_when_multiple_columns(): void
    {
        $stmt = $this->make('{"total":100,"other":200}');

        $this->assertSame(100, $stmt->value());
    }

    public function test_value_returns_null_when_empty(): void
    {
        $stmt = $this->make('');

        $this->assertNull($stmt->value());
    }

    // ─── pluck() ──────────────────────────────────────────────────────────────

    public function test_pluck_returns_flat_array_of_column(): void
    {
        $stmt = $this->make("{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}");

        $this->assertSame([1, 2], $stmt->pluck('id'));
        $this->assertSame(['Alice', 'Bob'], $stmt->pluck('name'));
    }

    public function test_pluck_returns_empty_array_when_no_rows(): void
    {
        $stmt = $this->make('');

        $this->assertSame([], $stmt->pluck('id'));
    }

    // ─── count() / isEmpty() ──────────────────────────────────────────────────

    public function test_count_returns_number_of_rows(): void
    {
        $stmt = $this->make("{\"id\":1}\n{\"id\":2}\n{\"id\":3}");

        $this->assertSame(3, $stmt->count());
    }

    public function test_count_returns_zero_for_empty_response(): void
    {
        $stmt = $this->make('');

        $this->assertSame(0, $stmt->count());
    }

    public function test_is_empty_returns_true_for_empty_result(): void
    {
        $this->assertTrue($this->make('')->isEmpty());
    }

    public function test_is_empty_returns_false_when_rows_exist(): void
    {
        $this->assertFalse($this->make('{"id":1}')->isEmpty());
    }

    // ─── raw() ────────────────────────────────────────────────────────────────

    public function test_raw_returns_original_response_body(): void
    {
        $raw  = '{"id":1}';
        $stmt = $this->make($raw);

        $this->assertSame($raw, $stmt->raw());
    }

    // ─── queryId() ────────────────────────────────────────────────────────────

    public function test_query_id_returns_value_from_headers(): void
    {
        $stmt = $this->make('{}', ['X-ClickHouse-Query-Id' => 'abc-123']);

        $this->assertSame('abc-123', $stmt->queryId());
    }

    public function test_query_id_returns_null_when_header_missing(): void
    {
        $this->assertNull($this->make('{}')->queryId());
    }

    // ─── summary() ────────────────────────────────────────────────────────────

    public function test_summary_decodes_json_header(): void
    {
        $summary = ['read_rows' => '10', 'read_bytes' => '1024'];
        $stmt    = $this->make('{}', ['X-ClickHouse-Summary' => json_encode($summary)]);

        $this->assertSame($summary, $stmt->summary());
    }

    public function test_summary_returns_empty_array_when_header_missing(): void
    {
        $this->assertSame([], $this->make('{}')->summary());
    }

    // ─── IteratorAggregate ────────────────────────────────────────────────────

    public function test_is_iterable_with_foreach(): void
    {
        $stmt = $this->make("{\"id\":1}\n{\"id\":2}");
        $rows = [];

        foreach ($stmt as $row) {
            $rows[] = $row;
        }

        $this->assertCount(2, $rows);
        $this->assertSame(['id' => 1], $rows[0]);
    }

    // ─── Countable ────────────────────────────────────────────────────────────

    public function test_count_works_with_count_function(): void
    {
        $stmt = $this->make("{\"id\":1}\n{\"id\":2}");

        $this->assertSame(2, count($stmt));
    }
}
