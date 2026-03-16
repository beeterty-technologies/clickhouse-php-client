<?php

namespace Beeterty\ClickHouse\Tests\Unit\Schema;

use Beeterty\ClickHouse\Schema\ColumnDefinition;
use PHPUnit\Framework\TestCase;

class ColumnDefinitionTest extends TestCase
{
    // ─── Basic toSql() ────────────────────────────────────────────────────────

    public function test_basic_column_sql(): void
    {
        $col = new ColumnDefinition('id', 'UInt64');

        $this->assertSame('`id` UInt64', $col->toSql());
    }

    public function test_get_name(): void
    {
        $col = new ColumnDefinition('created_at', 'DateTime');

        $this->assertSame('created_at', $col->getName());
    }

    // ─── nullable() ───────────────────────────────────────────────────────────

    public function test_nullable_wraps_type(): void
    {
        $col = new ColumnDefinition('name', 'String');
        $col->nullable();

        $this->assertSame('`name` Nullable(String)', $col->toSql());
    }

    public function test_nullable_returns_static_for_chaining(): void
    {
        $col = new ColumnDefinition('name', 'String');

        $this->assertSame($col, $col->nullable());
    }

    // ─── lowCardinality() ─────────────────────────────────────────────────────

    public function test_low_cardinality_wraps_type(): void
    {
        $col = new ColumnDefinition('status', 'String');
        $col->lowCardinality();

        $this->assertSame('`status` LowCardinality(String)', $col->toSql());
    }

    public function test_nullable_then_low_cardinality(): void
    {
        $col = new ColumnDefinition('status', 'String');
        $col->nullable()->lowCardinality();

        $this->assertSame('`status` LowCardinality(Nullable(String))', $col->toSql());
    }

    // ─── default() ────────────────────────────────────────────────────────────

    public function test_default_string_value(): void
    {
        $col = new ColumnDefinition('status', 'String');
        $col->default('active');

        $this->assertSame("`status` String DEFAULT 'active'", $col->toSql());
    }

    public function test_default_integer_value(): void
    {
        $col = new ColumnDefinition('count', 'UInt32');
        $col->default(0);

        $this->assertSame('`count` UInt32 DEFAULT 0', $col->toSql());
    }

    public function test_default_float_value(): void
    {
        $col = new ColumnDefinition('ratio', 'Float64');
        $col->default(0.5);

        $this->assertSame('`ratio` Float64 DEFAULT 0.5', $col->toSql());
    }

    public function test_default_bool_true(): void
    {
        $col = new ColumnDefinition('active', 'Bool');
        $col->default(true);

        $this->assertSame('`active` Bool DEFAULT 1', $col->toSql());
    }

    public function test_default_bool_false(): void
    {
        $col = new ColumnDefinition('active', 'Bool');
        $col->default(false);

        $this->assertSame('`active` Bool DEFAULT 0', $col->toSql());
    }

    public function test_default_null_value(): void
    {
        $col = new ColumnDefinition('name', 'String');
        $col->nullable()->default(null);

        $this->assertSame('`name` Nullable(String) DEFAULT NULL', $col->toSql());
    }

    // ─── comment() ────────────────────────────────────────────────────────────

    public function test_comment_appended_to_sql(): void
    {
        $col = new ColumnDefinition('id', 'UInt64');
        $col->comment('Primary key');

        $this->assertSame("`id` UInt64 COMMENT 'Primary key'", $col->toSql());
    }

    public function test_comment_escapes_single_quotes(): void
    {
        $col = new ColumnDefinition('note', 'String');
        $col->comment("User's note");

        $this->assertStringContainsString("COMMENT 'User\\'s note'", $col->toSql());
    }

    // ─── codec() ──────────────────────────────────────────────────────────────

    public function test_codec_appended_to_sql(): void
    {
        $col = new ColumnDefinition('ts', 'DateTime');
        $col->codec('Delta, LZ4');

        $this->assertSame('`ts` DateTime CODEC(Delta, LZ4)', $col->toSql());
    }

    // ─── ttl() ────────────────────────────────────────────────────────────────

    public function test_ttl_appended_to_sql(): void
    {
        $col = new ColumnDefinition('ts', 'DateTime');
        $col->ttl('ts + INTERVAL 1 YEAR');

        $this->assertSame('`ts` DateTime TTL ts + INTERVAL 1 YEAR', $col->toSql());
    }

    // ─── after() ──────────────────────────────────────────────────────────────

    public function test_get_after_is_null_by_default(): void
    {
        $col = new ColumnDefinition('email', 'String');

        $this->assertNull($col->getAfter());
    }

    public function test_after_stores_column_name(): void
    {
        $col = new ColumnDefinition('email', 'String');
        $col->after('name');

        $this->assertSame('name', $col->getAfter());
    }

    public function test_after_does_not_affect_to_sql(): void
    {
        // AFTER is handled by Grammar, not toSql()
        $col = new ColumnDefinition('email', 'String');
        $col->after('name');

        $this->assertSame('`email` String', $col->toSql());
    }

    // ─── change() ─────────────────────────────────────────────────────────────

    public function test_is_change_is_false_by_default(): void
    {
        $col = new ColumnDefinition('id', 'UInt64');

        $this->assertFalse($col->isChange());
    }

    public function test_change_marks_column_as_modification(): void
    {
        $col = new ColumnDefinition('id', 'UInt64');
        $col->change();

        $this->assertTrue($col->isChange());
    }

    // ─── Modifier ordering in toSql() ─────────────────────────────────────────

    public function test_all_modifiers_appear_in_correct_order(): void
    {
        $col = new ColumnDefinition('status', 'String');
        $col->nullable()
            ->lowCardinality()
            ->default('active')
            ->comment('Row status')
            ->codec('ZSTD(1)')
            ->ttl('created_at + INTERVAL 30 DAY');

        $sql = $col->toSql();

        // Verify order: type → DEFAULT → COMMENT → CODEC → TTL
        $defaultPos  = strpos($sql, 'DEFAULT');
        $commentPos  = strpos($sql, 'COMMENT');
        $codecPos    = strpos($sql, 'CODEC');
        $ttlPos      = strpos($sql, 'TTL');

        $this->assertLessThan($commentPos, $defaultPos);
        $this->assertLessThan($codecPos, $commentPos);
        $this->assertLessThan($ttlPos, $codecPos);
    }
}
