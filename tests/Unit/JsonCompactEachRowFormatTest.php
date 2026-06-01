<?php

namespace Beeterty\ClickHouse\Tests\Unit;

use Beeterty\ClickHouse\Format\JsonCompactEachRow;
use Beeterty\ClickHouse\Format\JsonCompactEachRowWithNamesAndTypes;
use PHPUnit\Framework\TestCase;

class JsonCompactEachRowFormatTest extends TestCase
{
    // ─── JsonCompactEachRow ───────────────────────────────────────────────────

    private JsonCompactEachRow $compact;

    protected function setUp(): void
    {
        $this->compact = new JsonCompactEachRow();
    }

    public function test_compact_name(): void
    {
        $this->assertSame('JSONCompactEachRow', $this->compact->name());
    }

    public function test_compact_encode_empty_returns_empty_string(): void
    {
        $this->assertSame('', $this->compact->encode([]));
    }

    public function test_compact_encode_single_row_as_json_array(): void
    {
        $output = $this->compact->encode([['id' => 1, 'name' => 'Alice']]);
        $this->assertSame('[1,"Alice"]', $output);
    }

    public function test_compact_encode_multiple_rows_newline_delimited(): void
    {
        $output = $this->compact->encode([
            ['id' => 1, 'name' => 'Alice'],
            ['id' => 2, 'name' => 'Bob'],
        ]);
        $this->assertSame("[1,\"Alice\"]\n[2,\"Bob\"]", $output);
    }

    public function test_compact_encode_discards_array_keys(): void
    {
        $output = $this->compact->encode([['z_col' => 'first', 'a_col' => 'second']]);
        $this->assertSame('["first","second"]', $output);
    }

    public function test_compact_encode_null_value(): void
    {
        $output = $this->compact->encode([['id' => 1, 'note' => null]]);
        $this->assertSame('[1,null]', $output);
    }

    public function test_compact_decode_empty_returns_empty_array(): void
    {
        $this->assertSame([], $this->compact->decode(''));
        $this->assertSame([], $this->compact->decode('   '));
    }

    public function test_compact_decode_single_row(): void
    {
        $rows = $this->compact->decode('[1,"Alice"]');
        $this->assertCount(1, $rows);
        $this->assertSame([1, 'Alice'], $rows[0]);
    }

    public function test_compact_decode_multiple_rows(): void
    {
        $rows = $this->compact->decode("[1,\"Alice\"]\n[2,\"Bob\"]");
        $this->assertCount(2, $rows);
        $this->assertSame([1, 'Alice'], $rows[0]);
        $this->assertSame([2, 'Bob'], $rows[1]);
    }

    public function test_compact_decode_returns_integer_indexed_arrays(): void
    {
        $rows = $this->compact->decode('["a","b","c"]');
        $this->assertArrayHasKey(0, $rows[0]);
        $this->assertArrayHasKey(1, $rows[0]);
        $this->assertArrayHasKey(2, $rows[0]);
    }

    public function test_compact_decode_skips_blank_lines(): void
    {
        $rows = $this->compact->decode("[1]\n\n[2]\n");
        $this->assertCount(2, $rows);
    }

    // ─── JsonCompactEachRowWithNamesAndTypes ──────────────────────────────────

    private JsonCompactEachRowWithNamesAndTypes $withNames;

    protected function setUpWithNames(): void
    {
        $this->withNames = new JsonCompactEachRowWithNamesAndTypes();
    }

    private function withNames(): JsonCompactEachRowWithNamesAndTypes
    {
        return new JsonCompactEachRowWithNamesAndTypes();
    }

    public function test_with_names_format_name(): void
    {
        $this->assertSame('JSONCompactEachRowWithNamesAndTypes', $this->withNames()->name());
    }

    public function test_with_names_encode_empty_returns_empty_string(): void
    {
        $this->assertSame('', $this->withNames()->encode([]));
    }

    public function test_with_names_encode_emits_names_row_first(): void
    {
        $output = $this->withNames()->encode([['id' => 1, 'name' => 'Alice']]);
        $lines  = explode("\n", $output);
        $this->assertSame('["id","name"]', $lines[0]);
    }

    public function test_with_names_encode_emits_types_row_second(): void
    {
        $output = $this->withNames()->encode([['id' => 1, 'score' => 3.14, 'label' => 'ok']]);
        $lines  = explode("\n", $output);
        $types  = json_decode($lines[1], true);
        $this->assertSame('Int64', $types[0]);
        $this->assertSame('Float64', $types[1]);
        $this->assertSame('String', $types[2]);
    }

    public function test_with_names_encode_infers_bool_as_uint8(): void
    {
        $output = $this->withNames()->encode([['active' => true]]);
        $lines  = explode("\n", $output);
        $types  = json_decode($lines[1], true);
        $this->assertSame('UInt8', $types[0]);
    }

    public function test_with_names_encode_infers_null_as_nullable_string(): void
    {
        $output = $this->withNames()->encode([['note' => null]]);
        $lines  = explode("\n", $output);
        $types  = json_decode($lines[1], true);
        $this->assertSame('Nullable(String)', $types[0]);
    }

    public function test_with_names_encode_data_rows_start_at_line_3(): void
    {
        $output = $this->withNames()->encode([
            ['id' => 1, 'name' => 'Alice'],
            ['id' => 2, 'name' => 'Bob'],
        ]);
        $lines = explode("\n", $output);
        $this->assertSame('[1,"Alice"]', $lines[2]);
        $this->assertSame('[2,"Bob"]', $lines[3]);
    }

    public function test_with_names_decode_empty_returns_empty_array(): void
    {
        $this->assertSame([], $this->withNames()->decode(''));
        $this->assertSame([], $this->withNames()->decode('   '));
    }

    public function test_with_names_decode_only_headers_returns_empty_array(): void
    {
        $raw  = '["id","name"]' . "\n" . '["Int64","String"]';
        $this->assertSame([], $this->withNames()->decode($raw));
    }

    public function test_with_names_decode_produces_associative_rows(): void
    {
        $raw = implode("\n", [
            '["id","name"]',
            '["Int64","String"]',
            '[1,"Alice"]',
            '[2,"Bob"]',
        ]);

        $rows = $this->withNames()->decode($raw);

        $this->assertCount(2, $rows);
        $this->assertSame(['id' => 1, 'name' => 'Alice'], $rows[0]);
        $this->assertSame(['id' => 2, 'name' => 'Bob'], $rows[1]);
    }

    public function test_with_names_decode_preserves_null_values(): void
    {
        $raw = implode("\n", [
            '["id","note"]',
            '["Int64","Nullable(String)"]',
            '[1,null]',
        ]);

        $rows = $this->withNames()->decode($raw);
        $this->assertNull($rows[0]['note']);
    }

    public function test_with_names_roundtrip(): void
    {
        $original = [
            ['id' => 1, 'score' => 9.5, 'label' => 'A'],
            ['id' => 2, 'score' => 7.0, 'label' => 'B'],
        ];

        $format  = $this->withNames();
        $encoded = $format->encode($original);
        $decoded = $format->decode($encoded);

        $this->assertCount(2, $decoded);
        $this->assertSame(1, $decoded[0]['id']);
        $this->assertSame('A', $decoded[0]['label']);
        $this->assertSame(2, $decoded[1]['id']);
        $this->assertSame('B', $decoded[1]['label']);
    }
}
