<?php

namespace Beeterty\ClickHouse\Tests\Integration;

use Beeterty\ClickHouse\Schema\Blueprint;
use Beeterty\ClickHouse\Schema\Engine\MergeTree;
use Beeterty\ClickHouse\Schema\Engine\SummingMergeTree;

class MaterializedViewIntegrationTest extends IntegrationTestCase
{
    private string $sourceTable;
    private string $targetTable;
    private string $viewName;

    protected function setUp(): void
    {
        parent::setUp();

        $suffix = substr(md5((string) microtime(true)), 0, 8);

        $this->sourceTable = "mv_src_$suffix";
        $this->targetTable = "mv_tgt_$suffix";
        $this->viewName = "mv_view_$suffix";

        $this->client->schema()->create($this->sourceTable, function (Blueprint $table): void {
            $table->uint32('user_id');
            $table->uint64('amount');
            $table->engine(new MergeTree())->orderBy('user_id');
        });

        $this->client->schema()->create($this->targetTable, function (Blueprint $table): void {
            $table->uint32('user_id');
            $table->uint64('total');
            $table->engine(new SummingMergeTree())->orderBy('user_id');
        });
    }

    protected function tearDown(): void
    {
        $this->client->schema()->dropViewIfExists($this->viewName);
        $this->dropTableSilently($this->targetTable);
        $this->dropTableSilently($this->sourceTable);
    }

    public function test_create_materialized_view_makes_it_available(): void
    {
        $this->client->schema()->createMaterializedView(
            name: $this->viewName,
            to: $this->targetTable,
            selectSql: "SELECT user_id, sum(amount) AS total FROM `{$this->sourceTable}` GROUP BY user_id",
        );

        $this->assertTrue($this->client->schema()->hasView($this->viewName));
    }

    public function test_create_materialized_view_if_not_exists_does_not_throw(): void
    {
        $selectSql = "SELECT user_id, sum(amount) AS total FROM `{$this->sourceTable}` GROUP BY user_id";

        $this->client->schema()->createMaterializedView(
            name: $this->viewName,
            to: $this->targetTable,
            selectSql: $selectSql,
            ifNotExists: true,
        );

        $this->client->schema()->createMaterializedView(
            name: $this->viewName,
            to: $this->targetTable,
            selectSql: $selectSql,
            ifNotExists: true,
        );

        $this->assertTrue($this->client->schema()->hasView($this->viewName));
    }

    public function test_drop_view_removes_it(): void
    {
        $this->client->schema()->createMaterializedView(
            name: $this->viewName,
            to: $this->targetTable,
            selectSql: "SELECT user_id, sum(amount) AS total FROM `{$this->sourceTable}` GROUP BY user_id",
        );

        $this->client->schema()->dropView($this->viewName);

        $this->assertFalse($this->client->schema()->hasView($this->viewName));
    }

    public function test_drop_view_if_exists_does_not_throw_when_missing(): void
    {
        $this->client->schema()->dropViewIfExists($this->viewName);

        $this->assertFalse($this->client->schema()->hasView($this->viewName));
    }

    public function test_has_view_returns_false_for_nonexistent_view(): void
    {
        $this->assertFalse($this->client->schema()->hasView($this->viewName));
    }
}
