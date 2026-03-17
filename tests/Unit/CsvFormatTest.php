<?php

namespace Beeterty\ClickHouse\Tests\Unit;

use Beeterty\ClickHouse\Format\Csv;
use PHPUnit\Framework\TestCase;

class CsvFormatTest extends TestCase
{
    private Csv $format;

    protected function setUp(): void
    {
        $this->format = new Csv();
    }

    public function test_name_returns_csv_with_names(): void
    {
        $this->assertSame('CSVWithNames', $this->format->name());
    }

    public function test_encode_empty_rows_returns_empty_string(): void
    {
        $this->assertSame('', $this->format->encode([]));
    }

    public function test_encode_produces_header_row(): void
    {
        $output = $this->format->encode([['id' => 1, 'name' => 'Alice']]);
        $lines  = explode("\n", $output);
        $this->assertSame('id,name', $lines[0]);
    }

    public function test_encode_produces_data_rows(): void
    {
        $output = $this->format->encode([
            ['id' => 1, 'name' => 'Alice'],
            ['id' => 2, 'name' => 'Bob'],
        ]);
        $lines = explode("\n", $output);
        $this->assertSame('id,name', $lines[0]);
        $this->assertSame('1,Alice', $lines[1]);
        $this->assertSame('2,Bob', $lines[2]);
    }

    public function test_encode_quotes_values_with_commas(): void
    {
        $output = $this->format->encode([['name' => 'Smith, John']]);
        $this->assertStringContainsString('"Smith, John"', $output);
    }

    public function test_encode_quotes_values_with_double_quotes(): void
    {
        $output = $this->format->encode([['name' => 'Say "hello"']]);
        $this->assertStringContainsString('"Say ""hello"""', $output);
    }

    public function test_encode_quotes_values_with_newlines(): void
    {
        $output = $this->format->encode([['note' => "line1\nline2"]]);
        $this->assertStringContainsString('"line1' . "\n" . 'line2"', $output);
    }

    public function test_encode_null_becomes_empty_string(): void
    {
        $output = $this->format->encode([['id' => 1, 'note' => null]]);
        $lines  = explode("\n", $output);
        $this->assertSame('1,', $lines[1]);
    }

    public function test_decode_empty_string_returns_empty_array(): void
    {
        $this->assertSame([], $this->format->decode(''));
        $this->assertSame([], $this->format->decode('   '));
    }

    public function test_decode_parses_header_and_rows(): void
    {
        $raw  = "id,name\n1,Alice\n2,Bob";
        $rows = $this->format->decode($raw);
        $this->assertCount(2, $rows);
        $this->assertSame(['id' => '1', 'name' => 'Alice'], $rows[0]);
        $this->assertSame(['id' => '2', 'name' => 'Bob'], $rows[1]);
    }

    public function test_decode_handles_quoted_values(): void
    {
        $raw  = "id,name\n1,\"Smith, John\"";
        $rows = $this->format->decode($raw);
        $this->assertSame('Smith, John', $rows[0]['name']);
    }

    public function test_decode_handles_escaped_quotes(): void
    {
        $raw  = "id,name\n1,\"Say \"\"hello\"\"\"";
        $rows = $this->format->decode($raw);
        $this->assertSame('Say "hello"', $rows[0]['name']);
    }

    public function test_decode_skips_empty_lines(): void
    {
        $raw  = "id,name\n1,Alice\n\n2,Bob\n";
        $rows = $this->format->decode($raw);
        $this->assertCount(2, $rows);
    }
}
