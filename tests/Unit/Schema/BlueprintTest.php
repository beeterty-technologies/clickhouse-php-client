<?php

namespace Beeterty\ClickHouse\Tests\Unit\Schema;

use Beeterty\ClickHouse\Schema\{Blueprint, ColumnDefinition};
use Beeterty\ClickHouse\Schema\Engine\MergeTree;
use PHPUnit\Framework\TestCase;

class BlueprintTest extends TestCase
{
    private function blueprint(): Blueprint
    {
        return new Blueprint();
    }

    // [Integer columns]

    public function test_uint8(): void
    {
        $this->assertSame('`x` UInt8',   $this->blueprint()->uint8('x')->toSql());
    }
    public function test_uint16(): void
    {
        $this->assertSame('`x` UInt16',  $this->blueprint()->uint16('x')->toSql());
    }
    public function test_uint32(): void
    {
        $this->assertSame('`x` UInt32',  $this->blueprint()->uint32('x')->toSql());
    }
    public function test_uint64(): void
    {
        $this->assertSame('`x` UInt64',  $this->blueprint()->uint64('x')->toSql());
    }
    public function test_int8(): void
    {
        $this->assertSame('`x` Int8',    $this->blueprint()->int8('x')->toSql());
    }
    public function test_int16(): void
    {
        $this->assertSame('`x` Int16',   $this->blueprint()->int16('x')->toSql());
    }
    public function test_int32(): void
    {
        $this->assertSame('`x` Int32',   $this->blueprint()->int32('x')->toSql());
    }
    public function test_int64(): void
    {
        $this->assertSame('`x` Int64',   $this->blueprint()->int64('x')->toSql());
    }

    // [Float columns]

    public function test_float32(): void
    {
        $this->assertSame('`x` Float32', $this->blueprint()->float32('x')->toSql());
    }
    public function test_float64(): void
    {
        $this->assertSame('`x` Float64', $this->blueprint()->float64('x')->toSql());
    }

    // [Decimal]

    public function test_decimal(): void
    {
        $this->assertSame('`price` Decimal(10, 2)', $this->blueprint()->decimal('price', 10, 2)->toSql());
    }

    // [String types]

    public function test_string(): void
    {
        $this->assertSame('`name` String', $this->blueprint()->string('name')->toSql());
    }

    public function test_fixed_string(): void
    {
        $this->assertSame('`code` FixedString(10)', $this->blueprint()->fixedString('code', 10)->toSql());
    }

    // [Date / Time]

    public function test_date(): void
    {
        $this->assertSame('`d` Date', $this->blueprint()->date('d')->toSql());
    }

    public function test_date_time_without_timezone(): void
    {
        $this->assertSame('`ts` DateTime', $this->blueprint()->dateTime('ts')->toSql());
    }

    public function test_date_time_with_timezone(): void
    {
        $this->assertSame("`ts` DateTime('UTC')", $this->blueprint()->dateTime('ts', 'UTC')->toSql());
    }

    public function test_date_time64_default_precision(): void
    {
        $this->assertSame('`ts` DateTime64(3)', $this->blueprint()->dateTime64('ts')->toSql());
    }

    public function test_date_time64_with_precision_and_timezone(): void
    {
        $this->assertSame("`ts` DateTime64(6, 'UTC')", $this->blueprint()->dateTime64('ts', 6, 'UTC')->toSql());
    }

    // [Boolean / UUID]

    public function test_boolean(): void
    {
        $this->assertSame('`active` Bool', $this->blueprint()->boolean('active')->toSql());
    }
    public function test_uuid(): void
    {
        $this->assertSame('`id` UUID',     $this->blueprint()->uuid('id')->toSql());
    }

    // [Enum]

    public function test_enum8_with_explicit_values(): void
    {
        $col = $this->blueprint()->enum8('status', ['active' => 1, 'inactive' => 2]);

        $this->assertSame("`status` Enum8('active' = 1, 'inactive' = 2)", $col->toSql());
    }

    public function test_enum8_with_auto_indexed_values(): void
    {
        $col = $this->blueprint()->enum8('status', ['active', 'inactive']);

        $this->assertSame("`status` Enum8('active' = 1, 'inactive' = 2)", $col->toSql());
    }

    public function test_enum16(): void
    {
        $col = $this->blueprint()->enum16('priority', ['low' => 1, 'high' => 2]);

        $this->assertStringContainsString('Enum16', $col->toSql());
    }

    // [IP types]

    public function test_ipv4(): void
    {
        $this->assertSame('`ip` IPv4', $this->blueprint()->ipv4('ip')->toSql());
    }
    public function test_ipv6(): void
    {
        $this->assertSame('`ip` IPv6', $this->blueprint()->ipv6('ip')->toSql());
    }

    // [JSON]

    public function test_json(): void
    {
        $this->assertSame('`data` JSON', $this->blueprint()->json('data')->toSql());
    }

    // [Complex types]

    public function test_array(): void
    {
        $this->assertSame('`tags` Array(String)', $this->blueprint()->array('tags', 'String')->toSql());
    }

    public function test_map(): void
    {
        $this->assertSame('`meta` Map(String, String)', $this->blueprint()->map('meta', 'String', 'String')->toSql());
    }

    public function test_tuple(): void
    {
        $this->assertSame('`coords` Tuple(Float64, Float64)', $this->blueprint()->tuple('coords', 'Float64', 'Float64')->toSql());
    }

    // [Convenience shorthands]

    public function test_id_creates_uint64_named_id(): void
    {
        $col = $this->blueprint()->id();

        $this->assertSame('`id` UInt64', $col->toSql());
    }

    public function test_id_accepts_custom_name(): void
    {
        $col = $this->blueprint()->id('user_id');

        $this->assertSame('`user_id` UInt64', $col->toSql());
    }

    public function test_timestamps_adds_created_at_and_updated_at(): void
    {
        $table = $this->blueprint();
        $table->timestamps();

        $columns = $table->getColumns();

        $this->assertCount(2, $columns);
        $this->assertSame('created_at', $columns[0]->getName());
        $this->assertSame('updated_at', $columns[1]->getName());
    }

    public function test_timestamps_columns_are_nullable(): void
    {
        $table = $this->blueprint();
        $table->timestamps();

        $columns = $table->getColumns();

        $this->assertStringContainsString('Nullable', $columns[0]->toSql());
        $this->assertStringContainsString('Nullable', $columns[1]->toSql());
    }

    public function test_soft_deletes_adds_deleted_at(): void
    {
        $table = $this->blueprint();
        $table->softDeletes();

        $columns = $table->getColumns();

        $this->assertCount(1, $columns);
        $this->assertSame('deleted_at', $columns[0]->getName());
        $this->assertStringContainsString('Nullable', $columns[0]->toSql());
    }

    public function test_soft_deletes_accepts_custom_column_name(): void
    {
        $table = $this->blueprint();
        $table->softDeletes('removed_at');

        $this->assertSame('removed_at', $table->getColumns()[0]->getName());
    }

    public function test_raw_column(): void
    {
        $col = $this->blueprint()->rawColumn('data', 'Tuple(UInt32, String)');

        $this->assertSame('`data` Tuple(UInt32, String)', $col->toSql());
    }

    // [ALTER helpers]

    public function test_drop_column_adds_to_drops(): void
    {
        $table = $this->blueprint();
        $table->dropColumn('legacy');

        $this->assertContains('legacy', $table->getDrops());
    }

    public function test_drop_timestamps_adds_both_columns(): void
    {
        $table = $this->blueprint();
        $table->dropTimestamps();

        $this->assertContains('created_at', $table->getDrops());
        $this->assertContains('updated_at', $table->getDrops());
    }

    public function test_drop_soft_deletes_adds_deleted_at(): void
    {
        $table = $this->blueprint();
        $table->dropSoftDeletes();

        $this->assertContains('deleted_at', $table->getDrops());
    }

    public function test_drop_soft_deletes_accepts_custom_name(): void
    {
        $table = $this->blueprint();
        $table->dropSoftDeletes('removed_at');

        $this->assertContains('removed_at', $table->getDrops());
    }

    public function test_rename_column(): void
    {
        $table = $this->blueprint();
        $table->renameColumn('old_name', 'new_name');

        $renames = $table->getRenames();

        $this->assertCount(1, $renames);
        $this->assertSame('old_name', $renames[0]['from']);
        $this->assertSame('new_name', $renames[0]['to']);
    }

    // [Engine & table options]

    public function test_engine_is_stored(): void
    {
        $table = $this->blueprint();
        $table->engine(new MergeTree());

        $this->assertInstanceOf(MergeTree::class, $table->getEngine());
    }

    public function test_order_by_string_is_cast_to_array(): void
    {
        $table = $this->blueprint();
        $table->orderBy('created_at');

        $this->assertSame(['created_at'], $table->getOrderBy());
    }

    public function test_order_by_array(): void
    {
        $table = $this->blueprint();
        $table->orderBy(['id', 'created_at']);

        $this->assertSame(['id', 'created_at'], $table->getOrderBy());
    }

    public function test_partition_by(): void
    {
        $table = $this->blueprint();
        $table->partitionBy('toYYYYMM(created_at)');

        $this->assertSame('toYYYYMM(created_at)', $table->getPartitionBy());
    }

    public function test_primary_key(): void
    {
        $table = $this->blueprint();
        $table->primaryKey('id');

        $this->assertSame('id', $table->getPrimaryKey());
    }

    public function test_sample_by(): void
    {
        $table = $this->blueprint();
        $table->sampleBy('intHash32(id)');

        $this->assertSame('intHash32(id)', $table->getSampleBy());
    }

    public function test_settings_are_merged(): void
    {
        $table = $this->blueprint();
        $table->settings(['index_granularity' => 8192]);
        $table->settings(['merge_with_ttl_timeout' => 86400]);

        $settings = $table->getSettings();

        $this->assertSame(8192, $settings['index_granularity']);
        $this->assertSame(86400, $settings['merge_with_ttl_timeout']);
    }

    public function test_ttl(): void
    {
        $table = $this->blueprint();
        $table->ttl('created_at + INTERVAL 1 YEAR');

        $this->assertSame('created_at + INTERVAL 1 YEAR', $table->getTtl());
    }

    public function test_comment(): void
    {
        $table = $this->blueprint();
        $table->comment('User events table');

        $this->assertSame('User events table', $table->getComment());
    }

    // [Column registration]

    public function test_get_columns_returns_all_defined_columns(): void
    {
        $table = $this->blueprint();
        $table->uint64('id');
        $table->string('name');
        $table->dateTime('ts');

        $this->assertCount(3, $table->getColumns());
        $this->assertContainsOnlyInstancesOf(ColumnDefinition::class, $table->getColumns());
    }

    public function test_column_methods_return_column_definition(): void
    {
        $col = $this->blueprint()->string('name');

        $this->assertInstanceOf(ColumnDefinition::class, $col);
    }
}
