<?php

namespace Beeterty\ClickHouse\Tests\Integration;

use Beeterty\ClickHouse\Schema\Blueprint;
use Beeterty\ClickHouse\Schema\Engine\MergeTree;
use Beeterty\ClickHouse\Schema\Engine\ReplacingMergeTree;

class SchemaIntegrationTest extends IntegrationTestCase
{
    // Each test gets a unique table name to avoid cross-test pollution.
    private string $table;

    protected function setUp(): void
    {
        parent::setUp();
        $this->table = 'schema_test_' . substr(md5(uniqid('', true)), 0, 8);
    }

    protected function tearDown(): void
    {
        $this->dropTableSilently($this->table);
        $this->dropTableSilently($this->table . '_renamed');
    }

    // ─── create() ─────────────────────────────────────────────────────────────

    public function test_create_makes_table_available(): void
    {
        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->string('name');
            $t->engine(new MergeTree())->orderBy(['id']);
        });

        $this->assertTrue($this->client->schema()->hasTable($this->table));
    }

    public function test_create_with_full_blueprint(): void
    {
        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->string('status')->lowCardinality()->default('active');
            $t->dateTime('created_at', 'UTC')->nullable();
            $t->uint32('score')->default(0)->comment('User score');
            $t->engine(new MergeTree())
              ->orderBy(['id', 'created_at'])
              ->partitionBy('toYYYYMM(created_at)')
              ->settings(['index_granularity' => 8192]);
        });

        $this->assertTrue($this->client->schema()->hasTable($this->table));

        $columns = $this->client->schema()->getColumns($this->table);
        $names   = array_column($columns, 'name');

        $this->assertContains('id', $names);
        $this->assertContains('status', $names);
        $this->assertContains('created_at', $names);
        $this->assertContains('score', $names);
    }

    public function test_create_with_replacing_merge_tree(): void
    {
        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->uint64('version');
            $t->string('data');
            $t->engine(new ReplacingMergeTree('version'))->orderBy(['id']);
        });

        $this->assertTrue($this->client->schema()->hasTable($this->table));
    }

    // ─── createIfNotExists() ──────────────────────────────────────────────────

    public function test_create_if_not_exists_does_not_throw_when_table_exists(): void
    {
        $callback = function (Blueprint $t) {
            $t->uint64('id');
            $t->engine(new MergeTree())->orderBy(['id']);
        };

        $this->client->schema()->create($this->table, $callback);
        // Second call must not throw
        $this->client->schema()->createIfNotExists($this->table, $callback);

        $this->assertTrue($this->client->schema()->hasTable($this->table));
    }

    // ─── hasTable() ───────────────────────────────────────────────────────────

    public function test_has_table_returns_false_for_nonexistent_table(): void
    {
        $this->assertFalse($this->client->schema()->hasTable('table_that_does_not_exist_xyz'));
    }

    public function test_has_table_returns_true_after_creation(): void
    {
        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->engine(new MergeTree())->orderBy(['id']);
        });

        $this->assertTrue($this->client->schema()->hasTable($this->table));
    }

    // ─── hasColumn() ──────────────────────────────────────────────────────────

    public function test_has_column_returns_true_for_existing_column(): void
    {
        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->string('email');
            $t->engine(new MergeTree())->orderBy(['id']);
        });

        $this->assertTrue($this->client->schema()->hasColumn($this->table, 'email'));
    }

    public function test_has_column_returns_false_for_missing_column(): void
    {
        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->engine(new MergeTree())->orderBy(['id']);
        });

        $this->assertFalse($this->client->schema()->hasColumn($this->table, 'no_such_column'));
    }

    // ─── getColumns() ─────────────────────────────────────────────────────────

    public function test_get_columns_returns_all_columns(): void
    {
        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->string('name');
            $t->uint32('age');
            $t->engine(new MergeTree())->orderBy(['id']);
        });

        $columns = $this->client->schema()->getColumns($this->table);
        $names   = array_column($columns, 'name');

        $this->assertContains('id', $names);
        $this->assertContains('name', $names);
        $this->assertContains('age', $names);
    }

    public function test_get_columns_includes_type_info(): void
    {
        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->engine(new MergeTree())->orderBy(['id']);
        });

        $columns = $this->client->schema()->getColumns($this->table);
        $id      = current(array_filter($columns, fn($c) => $c['name'] === 'id'));

        $this->assertArrayHasKey('type', $id);
        $this->assertSame('UInt64', $id['type']);
    }

    // ─── getTables() ──────────────────────────────────────────────────────────

    public function test_get_tables_includes_created_table(): void
    {
        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->engine(new MergeTree())->orderBy(['id']);
        });

        $tables = $this->client->schema()->getTables();
        $names  = array_column($tables, 'name');

        $this->assertContains($this->table, $names);
    }

    // ─── table() — ALTER ──────────────────────────────────────────────────────

    public function test_alter_add_column(): void
    {
        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->engine(new MergeTree())->orderBy(['id']);
        });

        $this->client->schema()->table($this->table, function (Blueprint $t) {
            $t->string('email');
        });

        $this->assertTrue($this->client->schema()->hasColumn($this->table, 'email'));
    }

    public function test_alter_drop_column(): void
    {
        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->string('temp_col');
            $t->engine(new MergeTree())->orderBy(['id']);
        });

        $this->client->schema()->table($this->table, function (Blueprint $t) {
            $t->dropColumn('temp_col');
        });

        $this->assertFalse($this->client->schema()->hasColumn($this->table, 'temp_col'));
    }

    public function test_alter_rename_column(): void
    {
        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->string('old_name');
            $t->engine(new MergeTree())->orderBy(['id']);
        });

        $this->client->schema()->table($this->table, function (Blueprint $t) {
            $t->renameColumn('old_name', 'new_name');
        });

        $this->assertFalse($this->client->schema()->hasColumn($this->table, 'old_name'));
        $this->assertTrue($this->client->schema()->hasColumn($this->table, 'new_name'));
    }

    public function test_alter_modify_column(): void
    {
        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint32('id');
            $t->string('note');
            $t->engine(new MergeTree())->orderBy(['id']);
        });

        // Widen uint32 → uint64
        $this->client->schema()->table($this->table, function (Blueprint $t) {
            $t->uint64('id')->change();
        });

        $columns = $this->client->schema()->getColumns($this->table);
        $id      = current(array_filter($columns, fn($c) => $c['name'] === 'id'));

        $this->assertSame('UInt64', $id['type']);
    }

    // ─── drop() ───────────────────────────────────────────────────────────────

    public function test_drop_removes_table(): void
    {
        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->engine(new MergeTree())->orderBy(['id']);
        });

        $this->client->schema()->drop($this->table);

        $this->assertFalse($this->client->schema()->hasTable($this->table));
    }

    // ─── dropIfExists() ───────────────────────────────────────────────────────

    public function test_drop_if_exists_does_not_throw_for_missing_table(): void
    {
        // Should not throw even though the table does not exist
        $this->client->schema()->dropIfExists('table_that_was_never_created_xyz');

        $this->assertTrue(true); // reached here without exception
    }

    // ─── rename() ─────────────────────────────────────────────────────────────

    public function test_rename_moves_table_to_new_name(): void
    {
        $renamed = $this->table . '_renamed';

        $this->client->schema()->create($this->table, function (Blueprint $t) {
            $t->uint64('id');
            $t->engine(new MergeTree())->orderBy(['id']);
        });

        $this->client->schema()->rename($this->table, $renamed);

        $this->assertFalse($this->client->schema()->hasTable($this->table));
        $this->assertTrue($this->client->schema()->hasTable($renamed));
    }
}
