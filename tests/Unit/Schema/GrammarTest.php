<?php

namespace Beeterty\ClickHouse\Tests\Unit\Schema;

use Beeterty\ClickHouse\Schema\Blueprint;
use Beeterty\ClickHouse\Schema\Grammar;
use Beeterty\ClickHouse\Schema\Engine\MergeTree;
use Beeterty\ClickHouse\Schema\Engine\ReplacingMergeTree;
use PHPUnit\Framework\TestCase;

class GrammarTest extends TestCase
{
    private Grammar $grammar;

    protected function setUp(): void
    {
        $this->grammar = new Grammar();
    }

    private function blueprint(): Blueprint
    {
        return new Blueprint();
    }

    // [compileCreate()]

    public function test_compile_create_contains_table_name(): void
    {
        $table = $this->blueprint();

        $table->uint64('id');
        $table->engine(new MergeTree())->orderBy('id');

        $sql = $this->grammar->compileCreate('events', $table);

        $this->assertStringContainsString('CREATE TABLE `events`', $sql);
    }

    public function test_compile_create_contains_column_definitions(): void
    {
        $table = $this->blueprint();;
        $table->uint64('id');
        $table->string('name');

        $sql = $this->grammar->compileCreate('events', $table);

        $this->assertStringContainsString('`id` UInt64', $sql);
        $this->assertStringContainsString('`name` String', $sql);
    }

    public function test_compile_create_contains_engine(): void
    {
        $table = $this->blueprint();;
        $table->uint64('id');
        $table->engine(new MergeTree())->orderBy('id');

        $sql = $this->grammar->compileCreate('events', $table);

        $this->assertStringContainsString('ENGINE = MergeTree()', $sql);
    }

    public function test_compile_create_contains_order_by(): void
    {
        $table = $this->blueprint();;
        $table->uint64('id');
        $table->engine(new MergeTree())->orderBy(['id', 'ts']);

        $sql = $this->grammar->compileCreate('events', $table);

        $this->assertStringContainsString('ORDER BY (id, ts)', $sql);
    }

    public function test_compile_create_contains_partition_by(): void
    {
        $table = $this->blueprint();;
        $table->uint64('id');
        $table->engine(new MergeTree())->orderBy('id')->partitionBy('toYYYYMM(ts)');

        $sql = $this->grammar->compileCreate('events', $table);

        $this->assertStringContainsString('PARTITION BY toYYYYMM(ts)', $sql);
    }

    public function test_compile_create_contains_primary_key(): void
    {
        $table = $this->blueprint();;
        $table->uint64('id');
        $table->engine(new MergeTree())->orderBy('id')->primaryKey('id');

        $sql = $this->grammar->compileCreate('events', $table);

        $this->assertStringContainsString('PRIMARY KEY id', $sql);
    }

    public function test_compile_create_contains_sample_by(): void
    {
        $table = $this->blueprint();;
        $table->uint64('id');
        $table->engine(new MergeTree())->orderBy('id')->sampleBy('intHash32(id)');

        $sql = $this->grammar->compileCreate('events', $table);

        $this->assertStringContainsString('SAMPLE BY intHash32(id)', $sql);
    }

    public function test_compile_create_contains_settings(): void
    {
        $table = $this->blueprint();;
        $table->uint64('id');
        $table->engine(new MergeTree())->orderBy('id')->settings(['index_granularity' => 8192]);

        $sql = $this->grammar->compileCreate('events', $table);

        $this->assertStringContainsString('SETTINGS index_granularity = 8192', $sql);
    }

    public function test_compile_create_contains_table_ttl(): void
    {
        $table = $this->blueprint();;
        $table->uint64('id');
        $table->engine(new MergeTree())->orderBy('id')->ttl('ts + INTERVAL 1 YEAR');

        $sql = $this->grammar->compileCreate('events', $table);

        $this->assertStringContainsString('TTL ts + INTERVAL 1 YEAR', $sql);
    }

    public function test_compile_create_contains_table_comment(): void
    {
        $table = $this->blueprint();;
        $table->uint64('id');
        $table->engine(new MergeTree())->orderBy('id')->comment('User events');

        $sql = $this->grammar->compileCreate('events', $table);

        $this->assertStringContainsString("COMMENT 'User events'", $sql);
    }

    public function test_compile_create_with_replacing_merge_tree_engine(): void
    {
        $table = $this->blueprint();;
        $table->uint64('id');
        $table->engine(new ReplacingMergeTree('version'))->orderBy('id');

        $sql = $this->grammar->compileCreate('events', $table);

        $this->assertStringContainsString('ENGINE = ReplacingMergeTree(version)', $sql);
    }

    // [compileCreateIfNotExists()]

    public function test_compile_create_if_not_exists(): void
    {
        $table = $this->blueprint();;
        $table->uint64('id');
        $table->engine(new MergeTree())->orderBy('id');

        $sql = $this->grammar->compileCreateIfNotExists('events', $table);

        $this->assertStringContainsString('CREATE TABLE IF NOT EXISTS `events`', $sql);
    }

    // [compileDrop()]

    public function test_compile_drop(): void
    {
        $this->assertSame('DROP TABLE `events`', $this->grammar->compileDrop('events'));
    }

    // [compileDropIfExists()]

    public function test_compile_drop_if_exists(): void
    {
        $this->assertSame('DROP TABLE IF EXISTS `events`', $this->grammar->compileDropIfExists('events'));
    }

    // [compileRename()]

    public function test_compile_rename(): void
    {
        $this->assertSame(
            'RENAME TABLE `old_table` TO `new_table`',
            $this->grammar->compileRename('old_table', 'new_table')
        );
    }

    // [compileAlter()]

    public function test_compile_alter_returns_empty_array_for_empty_blueprint(): void
    {
        $this->assertSame([], $this->grammar->compileAlter('users', $this->blueprint()));
    }

    public function test_compile_alter_add_column(): void
    {
        $table = $this->blueprint();;
        $table->string('email');

        $statements = $this->grammar->compileAlter('users', $table);

        $this->assertCount(1, $statements);
        $this->assertStringContainsString('ALTER TABLE `users`', $statements[0]);
        $this->assertStringContainsString('ADD COLUMN `email` String', $statements[0]);
    }

    public function test_compile_alter_modify_column(): void
    {
        $table = $this->blueprint();;
        $table->uint64('id')->change();

        $statements = $this->grammar->compileAlter('users', $table);

        $this->assertStringContainsString('MODIFY COLUMN `id` UInt64', $statements[0]);
    }

    public function test_compile_alter_add_column_after(): void
    {
        $table = $this->blueprint();;
        $table->string('email')->after('name');

        $statements = $this->grammar->compileAlter('users', $table);

        $this->assertStringContainsString('ADD COLUMN `email` String AFTER `name`', $statements[0]);
    }

    public function test_compile_alter_modify_column_after(): void
    {
        $table = $this->blueprint();;
        $table->string('email')->change()->after('name');

        $statements = $this->grammar->compileAlter('users', $table);

        $this->assertStringContainsString('MODIFY COLUMN `email` String AFTER `name`', $statements[0]);
    }

    public function test_compile_alter_drop_column(): void
    {
        $table = $this->blueprint();;
        $table->dropColumn('legacy_field');

        $statements = $this->grammar->compileAlter('users', $table);

        $this->assertStringContainsString('DROP COLUMN `legacy_field`', $statements[0]);
    }

    public function test_compile_alter_rename_column(): void
    {
        $table = $this->blueprint();;
        $table->renameColumn('old_name', 'new_name');

        $statements = $this->grammar->compileAlter('users', $table);

        $this->assertStringContainsString('RENAME COLUMN `old_name` TO `new_name`', $statements[0]);
    }

    public function test_compile_alter_batches_multiple_actions_into_one_statement(): void
    {
        $table = $this->blueprint();;
        $table->string('email');
        $table->dropColumn('phone');
        $table->renameColumn('ts', 'created_at');

        $statements = $this->grammar->compileAlter('users', $table);

        $this->assertCount(1, $statements);
        $this->assertStringContainsString('ADD COLUMN `email` String', $statements[0]);
        $this->assertStringContainsString('DROP COLUMN `phone`', $statements[0]);
        $this->assertStringContainsString('RENAME COLUMN `ts` TO `created_at`', $statements[0]);
    }

    // [compileMaterializedView()]

    public function test_compile_materialized_view(): void
    {
        $sql = $this->grammar->compileMaterializedView(
            'mv_daily',
            'daily_stats',
            'SELECT toDate(ts) AS day, count() AS n FROM events GROUP BY day',
        );

        $this->assertStringStartsWith('CREATE MATERIALIZED VIEW `mv_daily`', $sql);
        $this->assertStringContainsString('TO `daily_stats`', $sql);
        $this->assertStringContainsString('AS SELECT toDate(ts)', $sql);
        $this->assertStringNotContainsString('IF NOT EXISTS', $sql);
        $this->assertStringNotContainsString('POPULATE', $sql);
    }

    public function test_compile_materialized_view_if_not_exists(): void
    {
        $sql = $this->grammar->compileMaterializedView(
            'mv_daily',
            'daily_stats',
            'SELECT 1',
            ifNotExists: true,
        );

        $this->assertStringContainsString('IF NOT EXISTS', $sql);
    }

    public function test_compile_materialized_view_with_populate(): void
    {
        $sql = $this->grammar->compileMaterializedView(
            'mv_daily',
            'daily_stats',
            'SELECT 1',
            populate: true,
        );

        $this->assertStringContainsString('POPULATE', $sql);
        $this->assertStringContainsString('AS SELECT 1', $sql);
    }

    public function test_compile_materialized_view_if_not_exists_and_populate(): void
    {
        $sql = $this->grammar->compileMaterializedView(
            'mv',
            'target',
            'SELECT 1',
            ifNotExists: true,
            populate: true,
        );

        $this->assertSame('CREATE MATERIALIZED VIEW IF NOT EXISTS `mv` TO `target` POPULATE AS SELECT 1', $sql);
    }

    // [compileDropView()]

    public function test_compile_drop_view(): void
    {
        $this->assertSame('DROP VIEW `mv_daily`', $this->grammar->compileDropView('mv_daily'));
    }

    public function test_compile_drop_view_if_exists(): void
    {
        $this->assertSame(
            'DROP VIEW IF EXISTS `mv_daily`',
            $this->grammar->compileDropViewIfExists('mv_daily')
        );
    }
}
