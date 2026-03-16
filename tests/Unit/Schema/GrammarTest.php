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

    private function bp(): Blueprint
    {
        return new Blueprint();
    }

    // ─── compileCreate() ──────────────────────────────────────────────────────

    public function test_compile_create_contains_table_name(): void
    {
        $bp = $this->bp();
        $bp->uint64('id');
        $bp->engine(new MergeTree())->orderBy('id');

        $sql = $this->grammar->compileCreate('events', $bp);

        $this->assertStringContainsString('CREATE TABLE `events`', $sql);
    }

    public function test_compile_create_contains_column_definitions(): void
    {
        $bp = $this->bp();
        $bp->uint64('id');
        $bp->string('name');

        $sql = $this->grammar->compileCreate('events', $bp);

        $this->assertStringContainsString('`id` UInt64', $sql);
        $this->assertStringContainsString('`name` String', $sql);
    }

    public function test_compile_create_contains_engine(): void
    {
        $bp = $this->bp();
        $bp->uint64('id');
        $bp->engine(new MergeTree())->orderBy('id');

        $sql = $this->grammar->compileCreate('events', $bp);

        $this->assertStringContainsString('ENGINE = MergeTree()', $sql);
    }

    public function test_compile_create_contains_order_by(): void
    {
        $bp = $this->bp();
        $bp->uint64('id');
        $bp->engine(new MergeTree())->orderBy(['id', 'ts']);

        $sql = $this->grammar->compileCreate('events', $bp);

        $this->assertStringContainsString('ORDER BY (id, ts)', $sql);
    }

    public function test_compile_create_contains_partition_by(): void
    {
        $bp = $this->bp();
        $bp->uint64('id');
        $bp->engine(new MergeTree())->orderBy('id')->partitionBy('toYYYYMM(ts)');

        $sql = $this->grammar->compileCreate('events', $bp);

        $this->assertStringContainsString('PARTITION BY toYYYYMM(ts)', $sql);
    }

    public function test_compile_create_contains_primary_key(): void
    {
        $bp = $this->bp();
        $bp->uint64('id');
        $bp->engine(new MergeTree())->orderBy('id')->primaryKey('id');

        $sql = $this->grammar->compileCreate('events', $bp);

        $this->assertStringContainsString('PRIMARY KEY id', $sql);
    }

    public function test_compile_create_contains_sample_by(): void
    {
        $bp = $this->bp();
        $bp->uint64('id');
        $bp->engine(new MergeTree())->orderBy('id')->sampleBy('intHash32(id)');

        $sql = $this->grammar->compileCreate('events', $bp);

        $this->assertStringContainsString('SAMPLE BY intHash32(id)', $sql);
    }

    public function test_compile_create_contains_settings(): void
    {
        $bp = $this->bp();
        $bp->uint64('id');
        $bp->engine(new MergeTree())->orderBy('id')->settings(['index_granularity' => 8192]);

        $sql = $this->grammar->compileCreate('events', $bp);

        $this->assertStringContainsString('SETTINGS index_granularity = 8192', $sql);
    }

    public function test_compile_create_contains_table_ttl(): void
    {
        $bp = $this->bp();
        $bp->uint64('id');
        $bp->engine(new MergeTree())->orderBy('id')->ttl('ts + INTERVAL 1 YEAR');

        $sql = $this->grammar->compileCreate('events', $bp);

        $this->assertStringContainsString('TTL ts + INTERVAL 1 YEAR', $sql);
    }

    public function test_compile_create_contains_table_comment(): void
    {
        $bp = $this->bp();
        $bp->uint64('id');
        $bp->engine(new MergeTree())->orderBy('id')->comment('User events');

        $sql = $this->grammar->compileCreate('events', $bp);

        $this->assertStringContainsString("COMMENT 'User events'", $sql);
    }

    public function test_compile_create_with_replacing_merge_tree_engine(): void
    {
        $bp = $this->bp();
        $bp->uint64('id');
        $bp->engine(new ReplacingMergeTree('version'))->orderBy('id');

        $sql = $this->grammar->compileCreate('events', $bp);

        $this->assertStringContainsString('ENGINE = ReplacingMergeTree(version)', $sql);
    }

    // ─── compileCreateIfNotExists() ───────────────────────────────────────────

    public function test_compile_create_if_not_exists(): void
    {
        $bp = $this->bp();
        $bp->uint64('id');
        $bp->engine(new MergeTree())->orderBy('id');

        $sql = $this->grammar->compileCreateIfNotExists('events', $bp);

        $this->assertStringContainsString('CREATE TABLE IF NOT EXISTS `events`', $sql);
    }

    // ─── compileDrop() ────────────────────────────────────────────────────────

    public function test_compile_drop(): void
    {
        $this->assertSame('DROP TABLE `events`', $this->grammar->compileDrop('events'));
    }

    // ─── compileDropIfExists() ────────────────────────────────────────────────

    public function test_compile_drop_if_exists(): void
    {
        $this->assertSame('DROP TABLE IF EXISTS `events`', $this->grammar->compileDropIfExists('events'));
    }

    // ─── compileRename() ──────────────────────────────────────────────────────

    public function test_compile_rename(): void
    {
        $this->assertSame(
            'RENAME TABLE `old_table` TO `new_table`',
            $this->grammar->compileRename('old_table', 'new_table')
        );
    }

    // ─── compileAlter() ───────────────────────────────────────────────────────

    public function test_compile_alter_returns_empty_array_for_empty_blueprint(): void
    {
        $this->assertSame([], $this->grammar->compileAlter('users', $this->bp()));
    }

    public function test_compile_alter_add_column(): void
    {
        $bp = $this->bp();
        $bp->string('email');

        $statements = $this->grammar->compileAlter('users', $bp);

        $this->assertCount(1, $statements);
        $this->assertStringContainsString('ALTER TABLE `users`', $statements[0]);
        $this->assertStringContainsString('ADD COLUMN `email` String', $statements[0]);
    }

    public function test_compile_alter_modify_column(): void
    {
        $bp = $this->bp();
        $bp->uint64('id')->change();

        $statements = $this->grammar->compileAlter('users', $bp);

        $this->assertStringContainsString('MODIFY COLUMN `id` UInt64', $statements[0]);
    }

    public function test_compile_alter_add_column_after(): void
    {
        $bp = $this->bp();
        $bp->string('email')->after('name');

        $statements = $this->grammar->compileAlter('users', $bp);

        $this->assertStringContainsString('ADD COLUMN `email` String AFTER `name`', $statements[0]);
    }

    public function test_compile_alter_modify_column_after(): void
    {
        $bp = $this->bp();
        $bp->string('email')->change()->after('name');

        $statements = $this->grammar->compileAlter('users', $bp);

        $this->assertStringContainsString('MODIFY COLUMN `email` String AFTER `name`', $statements[0]);
    }

    public function test_compile_alter_drop_column(): void
    {
        $bp = $this->bp();
        $bp->dropColumn('legacy_field');

        $statements = $this->grammar->compileAlter('users', $bp);

        $this->assertStringContainsString('DROP COLUMN `legacy_field`', $statements[0]);
    }

    public function test_compile_alter_rename_column(): void
    {
        $bp = $this->bp();
        $bp->renameColumn('old_name', 'new_name');

        $statements = $this->grammar->compileAlter('users', $bp);

        $this->assertStringContainsString('RENAME COLUMN `old_name` TO `new_name`', $statements[0]);
    }

    public function test_compile_alter_batches_multiple_actions_into_one_statement(): void
    {
        $bp = $this->bp();
        $bp->string('email');
        $bp->dropColumn('phone');
        $bp->renameColumn('ts', 'created_at');

        $statements = $this->grammar->compileAlter('users', $bp);

        // All actions in a single ALTER TABLE statement
        $this->assertCount(1, $statements);
        $this->assertStringContainsString('ADD COLUMN `email` String', $statements[0]);
        $this->assertStringContainsString('DROP COLUMN `phone`', $statements[0]);
        $this->assertStringContainsString('RENAME COLUMN `ts` TO `created_at`', $statements[0]);
    }
}
