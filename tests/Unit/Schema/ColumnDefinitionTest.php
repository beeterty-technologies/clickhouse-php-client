<?php

namespace Beeterty\ClickHouse\Tests\Unit\Schema;

use Beeterty\ClickHouse\Schema\ColumnDefinition;
use PHPUnit\Framework\TestCase;

class ColumnDefinitionTest extends TestCase
{
    // [Basic toSql()]

    public function test_basic_column_sql(): void
    {
        $column = new ColumnDefinition('id', 'UInt64');

        $this->assertSame('`id` UInt64', $column->toSql());
    }

    public function test_get_name(): void
    {
        $column = new ColumnDefinition('created_at', 'DateTime');

        $this->assertSame('created_at', $column->getName());
    }

    // [nullable()]

    public function test_nullable_wraps_type(): void
    {
        $column = new ColumnDefinition('name', 'String');
        $column->nullable();

        $this->assertSame('`name` Nullable(String)', $column->toSql());
    }

    public function test_nullable_returns_static_for_chaining(): void
    {
        $column = new ColumnDefinition('name', 'String');

        $this->assertSame($column, $column->nullable());
    }

    // [lowCardinality()]

    public function test_low_cardinality_wraps_type(): void
    {
        $column = new ColumnDefinition('status', 'String');
        $column->lowCardinality();

        $this->assertSame('`status` LowCardinality(String)', $column->toSql());
    }

    public function test_nullable_then_low_cardinality(): void
    {
        $column = new ColumnDefinition('status', 'String');
        $column->nullable()->lowCardinality();

        $this->assertSame('`status` LowCardinality(Nullable(String))', $column->toSql());
    }

    // [default()]

    public function test_default_string_value(): void
    {
        $column = new ColumnDefinition('status', 'String');
        $column->default('active');

        $this->assertSame("`status` String DEFAULT 'active'", $column->toSql());
    }

    public function test_default_integer_value(): void
    {
        $column = new ColumnDefinition('count', 'UInt32');
        $column->default(0);

        $this->assertSame('`count` UInt32 DEFAULT 0', $column->toSql());
    }

    public function test_default_float_value(): void
    {
        $column = new ColumnDefinition('ratio', 'Float64');
        $column->default(0.5);

        $this->assertSame('`ratio` Float64 DEFAULT 0.5', $column->toSql());
    }

    public function test_default_bool_true(): void
    {
        $column = new ColumnDefinition('active', 'Bool');
        $column->default(true);

        $this->assertSame('`active` Bool DEFAULT 1', $column->toSql());
    }

    public function test_default_bool_false(): void
    {
        $column = new ColumnDefinition('active', 'Bool');
        $column->default(false);

        $this->assertSame('`active` Bool DEFAULT 0', $column->toSql());
    }

    public function test_default_null_value(): void
    {
        $column = new ColumnDefinition('name', 'String');
        $column->nullable()->default(null);

        $this->assertSame('`name` Nullable(String) DEFAULT NULL', $column->toSql());
    }

    // [comment()]

    public function test_comment_appended_to_sql(): void
    {
        $column = new ColumnDefinition('id', 'UInt64');
        $column->comment('Primary key');

        $this->assertSame("`id` UInt64 COMMENT 'Primary key'", $column->toSql());
    }

    public function test_comment_escapes_single_quotes(): void
    {
        $column = new ColumnDefinition('note', 'String');
        $column->comment("User's note");

        $this->assertStringContainsString("COMMENT 'User\\'s note'", $column->toSql());
    }

    // [codec()]

    public function test_codec_appended_to_sql(): void
    {
        $column = new ColumnDefinition('ts', 'DateTime');
        $column->codec('Delta, LZ4');

        $this->assertSame('`ts` DateTime CODEC(Delta, LZ4)', $column->toSql());
    }

    // [ttl()]

    public function test_ttl_appended_to_sql(): void
    {
        $column = new ColumnDefinition('ts', 'DateTime');
        $column->ttl('ts + INTERVAL 1 YEAR');

        $this->assertSame('`ts` DateTime TTL ts + INTERVAL 1 YEAR', $column->toSql());
    }

    // [after()]

    public function test_get_after_is_null_by_default(): void
    {
        $column = new ColumnDefinition('email', 'String');

        $this->assertNull($column->getAfter());
    }

    public function test_after_stores_column_name(): void
    {
        $column = new ColumnDefinition('email', 'String');
        $column->after('name');

        $this->assertSame('name', $column->getAfter());
    }

    public function test_after_does_not_affect_to_sql(): void
    {
        $column = new ColumnDefinition('email', 'String');
        $column->after('name');

        $this->assertSame('`email` String', $column->toSql());
    }

    // [change()]

    public function test_is_change_is_false_by_default(): void
    {
        $column = new ColumnDefinition('id', 'UInt64');

        $this->assertFalse($column->isChange());
    }

    public function test_change_marks_column_as_modification(): void
    {
        $column = new ColumnDefinition('id', 'UInt64');
        $column->change();

        $this->assertTrue($column->isChange());
    }

    // [Modifier ordering in toSql()]

    public function test_all_modifiers_appear_in_correct_order(): void
    {
        $column = new ColumnDefinition('status', 'String');
        $column->nullable()
            ->lowCardinality()
            ->default('active')
            ->comment('Row status')
            ->codec('ZSTD(1)')
            ->ttl('created_at + INTERVAL 30 DAY');

        $sql = $column->toSql();

        $defaultPos = strpos($sql, 'DEFAULT');
        $commentPos = strpos($sql, 'COMMENT');
        $codecPos = strpos($sql, 'CODEC');
        $ttlPos = strpos($sql, 'TTL');

        $this->assertLessThan($commentPos, $defaultPos);
        $this->assertLessThan($codecPos, $commentPos);
        $this->assertLessThan($ttlPos, $codecPos);
    }
}
