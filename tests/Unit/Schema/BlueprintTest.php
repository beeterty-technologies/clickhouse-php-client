<?php

namespace Beeterty\ClickHouse\Tests\Unit\Schema;

use Beeterty\ClickHouse\Schema\Blueprint;
use Beeterty\ClickHouse\Schema\ColumnDefinition;
use Beeterty\ClickHouse\Schema\Engine\AggregatingMergeTree;
use Beeterty\ClickHouse\Schema\Engine\CollapsingMergeTree;
use Beeterty\ClickHouse\Schema\Engine\MergeTree;
use Beeterty\ClickHouse\Schema\Engine\ReplacingMergeTree;
use PHPUnit\Framework\TestCase;

class BlueprintTest extends TestCase
{
    private function bp(): Blueprint
    {
        return new Blueprint();
    }

    // ─── Integer columns ──────────────────────────────────────────────────────

    public function test_uint8(): void  { $this->assertSame('`x` UInt8',   $this->bp()->uint8('x')->toSql());  }
    public function test_uint16(): void { $this->assertSame('`x` UInt16',  $this->bp()->uint16('x')->toSql()); }
    public function test_uint32(): void { $this->assertSame('`x` UInt32',  $this->bp()->uint32('x')->toSql()); }
    public function test_uint64(): void { $this->assertSame('`x` UInt64',  $this->bp()->uint64('x')->toSql()); }
    public function test_int8(): void   { $this->assertSame('`x` Int8',    $this->bp()->int8('x')->toSql());   }
    public function test_int16(): void  { $this->assertSame('`x` Int16',   $this->bp()->int16('x')->toSql());  }
    public function test_int32(): void  { $this->assertSame('`x` Int32',   $this->bp()->int32('x')->toSql());  }
    public function test_int64(): void  { $this->assertSame('`x` Int64',   $this->bp()->int64('x')->toSql());  }

    // ─── Float columns ────────────────────────────────────────────────────────

    public function test_float32(): void { $this->assertSame('`x` Float32', $this->bp()->float32('x')->toSql()); }
    public function test_float64(): void { $this->assertSame('`x` Float64', $this->bp()->float64('x')->toSql()); }

    // ─── Decimal ──────────────────────────────────────────────────────────────

    public function test_decimal(): void
    {
        $this->assertSame('`price` Decimal(10, 2)', $this->bp()->decimal('price', 10, 2)->toSql());
    }

    // ─── String types ─────────────────────────────────────────────────────────

    public function test_string(): void
    {
        $this->assertSame('`name` String', $this->bp()->string('name')->toSql());
    }

    public function test_fixed_string(): void
    {
        $this->assertSame('`code` FixedString(10)', $this->bp()->fixedString('code', 10)->toSql());
    }

    // ─── Date / Time ──────────────────────────────────────────────────────────

    public function test_date(): void
    {
        $this->assertSame('`d` Date', $this->bp()->date('d')->toSql());
    }

    public function test_date_time_without_timezone(): void
    {
        $this->assertSame('`ts` DateTime', $this->bp()->dateTime('ts')->toSql());
    }

    public function test_date_time_with_timezone(): void
    {
        $this->assertSame("`ts` DateTime('UTC')", $this->bp()->dateTime('ts', 'UTC')->toSql());
    }

    public function test_date_time64_default_precision(): void
    {
        $this->assertSame('`ts` DateTime64(3)', $this->bp()->dateTime64('ts')->toSql());
    }

    public function test_date_time64_with_precision_and_timezone(): void
    {
        $this->assertSame("`ts` DateTime64(6, 'UTC')", $this->bp()->dateTime64('ts', 6, 'UTC')->toSql());
    }

    // ─── Boolean / UUID ───────────────────────────────────────────────────────

    public function test_boolean(): void { $this->assertSame('`active` Bool', $this->bp()->boolean('active')->toSql()); }
    public function test_uuid(): void    { $this->assertSame('`id` UUID',     $this->bp()->uuid('id')->toSql());         }

    // ─── Enum ─────────────────────────────────────────────────────────────────

    public function test_enum8_with_explicit_values(): void
    {
        $col = $this->bp()->enum8('status', ['active' => 1, 'inactive' => 2]);

        $this->assertSame("`status` Enum8('active' = 1, 'inactive' = 2)", $col->toSql());
    }

    public function test_enum8_with_auto_indexed_values(): void
    {
        $col = $this->bp()->enum8('status', ['active', 'inactive']);

        $this->assertSame("`status` Enum8('active' = 1, 'inactive' = 2)", $col->toSql());
    }

    public function test_enum16(): void
    {
        $col = $this->bp()->enum16('priority', ['low' => 1, 'high' => 2]);

        $this->assertStringContainsString('Enum16', $col->toSql());
    }

    // ─── IP types ─────────────────────────────────────────────────────────────

    public function test_ipv4(): void { $this->assertSame('`ip` IPv4', $this->bp()->ipv4('ip')->toSql()); }
    public function test_ipv6(): void { $this->assertSame('`ip` IPv6', $this->bp()->ipv6('ip')->toSql()); }

    // ─── JSON ─────────────────────────────────────────────────────────────────

    public function test_json(): void { $this->assertSame('`data` JSON', $this->bp()->json('data')->toSql()); }

    // ─── Complex types ────────────────────────────────────────────────────────

    public function test_array(): void
    {
        $this->assertSame('`tags` Array(String)', $this->bp()->array('tags', 'String')->toSql());
    }

    public function test_map(): void
    {
        $this->assertSame('`meta` Map(String, String)', $this->bp()->map('meta', 'String', 'String')->toSql());
    }

    public function test_tuple(): void
    {
        $this->assertSame('`coords` Tuple(Float64, Float64)', $this->bp()->tuple('coords', 'Float64', 'Float64')->toSql());
    }

    // ─── Convenience shorthands ───────────────────────────────────────────────

    public function test_id_creates_uint64_named_id(): void
    {
        $col = $this->bp()->id();

        $this->assertSame('`id` UInt64', $col->toSql());
    }

    public function test_id_accepts_custom_name(): void
    {
        $col = $this->bp()->id('user_id');

        $this->assertSame('`user_id` UInt64', $col->toSql());
    }

    public function test_timestamps_adds_created_at_and_updated_at(): void
    {
        $bp = $this->bp();
        $bp->timestamps();

        $columns = $bp->getColumns();

        $this->assertCount(2, $columns);
        $this->assertSame('created_at', $columns[0]->getName());
        $this->assertSame('updated_at', $columns[1]->getName());
    }

    public function test_timestamps_columns_are_nullable(): void
    {
        $bp = $this->bp();
        $bp->timestamps();

        $columns = $bp->getColumns();

        $this->assertStringContainsString('Nullable', $columns[0]->toSql());
        $this->assertStringContainsString('Nullable', $columns[1]->toSql());
    }

    public function test_soft_deletes_adds_deleted_at(): void
    {
        $bp = $this->bp();
        $bp->softDeletes();

        $columns = $bp->getColumns();

        $this->assertCount(1, $columns);
        $this->assertSame('deleted_at', $columns[0]->getName());
        $this->assertStringContainsString('Nullable', $columns[0]->toSql());
    }

    public function test_soft_deletes_accepts_custom_column_name(): void
    {
        $bp = $this->bp();
        $bp->softDeletes('removed_at');

        $this->assertSame('removed_at', $bp->getColumns()[0]->getName());
    }

    public function test_raw_column(): void
    {
        $col = $this->bp()->rawColumn('data', 'Tuple(UInt32, String)');

        $this->assertSame('`data` Tuple(UInt32, String)', $col->toSql());
    }

    // ─── ALTER helpers ────────────────────────────────────────────────────────

    public function test_drop_column_adds_to_drops(): void
    {
        $bp = $this->bp();
        $bp->dropColumn('legacy');

        $this->assertContains('legacy', $bp->getDrops());
    }

    public function test_drop_timestamps_adds_both_columns(): void
    {
        $bp = $this->bp();
        $bp->dropTimestamps();

        $this->assertContains('created_at', $bp->getDrops());
        $this->assertContains('updated_at', $bp->getDrops());
    }

    public function test_drop_soft_deletes_adds_deleted_at(): void
    {
        $bp = $this->bp();
        $bp->dropSoftDeletes();

        $this->assertContains('deleted_at', $bp->getDrops());
    }

    public function test_drop_soft_deletes_accepts_custom_name(): void
    {
        $bp = $this->bp();
        $bp->dropSoftDeletes('removed_at');

        $this->assertContains('removed_at', $bp->getDrops());
    }

    public function test_rename_column(): void
    {
        $bp = $this->bp();
        $bp->renameColumn('old_name', 'new_name');

        $renames = $bp->getRenames();

        $this->assertCount(1, $renames);
        $this->assertSame('old_name', $renames[0]['from']);
        $this->assertSame('new_name', $renames[0]['to']);
    }

    // ─── Engine & table options ───────────────────────────────────────────────

    public function test_engine_is_stored(): void
    {
        $bp = $this->bp();
        $bp->engine(new MergeTree());

        $this->assertInstanceOf(MergeTree::class, $bp->getEngine());
    }

    public function test_order_by_string_is_cast_to_array(): void
    {
        $bp = $this->bp();
        $bp->orderBy('created_at');

        $this->assertSame(['created_at'], $bp->getOrderBy());
    }

    public function test_order_by_array(): void
    {
        $bp = $this->bp();
        $bp->orderBy(['id', 'created_at']);

        $this->assertSame(['id', 'created_at'], $bp->getOrderBy());
    }

    public function test_partition_by(): void
    {
        $bp = $this->bp();
        $bp->partitionBy('toYYYYMM(created_at)');

        $this->assertSame('toYYYYMM(created_at)', $bp->getPartitionBy());
    }

    public function test_primary_key(): void
    {
        $bp = $this->bp();
        $bp->primaryKey('id');

        $this->assertSame('id', $bp->getPrimaryKey());
    }

    public function test_sample_by(): void
    {
        $bp = $this->bp();
        $bp->sampleBy('intHash32(id)');

        $this->assertSame('intHash32(id)', $bp->getSampleBy());
    }

    public function test_settings_are_merged(): void
    {
        $bp = $this->bp();
        $bp->settings(['index_granularity' => 8192]);
        $bp->settings(['merge_with_ttl_timeout' => 86400]);

        $settings = $bp->getSettings();

        $this->assertSame(8192, $settings['index_granularity']);
        $this->assertSame(86400, $settings['merge_with_ttl_timeout']);
    }

    public function test_ttl(): void
    {
        $bp = $this->bp();
        $bp->ttl('created_at + INTERVAL 1 YEAR');

        $this->assertSame('created_at + INTERVAL 1 YEAR', $bp->getTtl());
    }

    public function test_comment(): void
    {
        $bp = $this->bp();
        $bp->comment('User events table');

        $this->assertSame('User events table', $bp->getComment());
    }

    // ─── Column registration ──────────────────────────────────────────────────

    public function test_get_columns_returns_all_defined_columns(): void
    {
        $bp = $this->bp();
        $bp->uint64('id');
        $bp->string('name');
        $bp->dateTime('ts');

        $this->assertCount(3, $bp->getColumns());
        $this->assertContainsOnlyInstancesOf(ColumnDefinition::class, $bp->getColumns());
    }

    public function test_column_methods_return_column_definition(): void
    {
        $col = $this->bp()->string('name');

        $this->assertInstanceOf(ColumnDefinition::class, $col);
    }
}
