<?php

namespace Beeterty\ClickHouse\Tests\Integration;

use Beeterty\ClickHouse\Query\Statement;
use Beeterty\ClickHouse\Schema\Blueprint;
use Beeterty\ClickHouse\Schema\Engine\MergeTree;

class ParallelIntegrationTest extends IntegrationTestCase
{
    private string $table;

    protected function setUp(): void
    {
        parent::setUp();

        $this->table = 'parallel_test_' . substr(md5((string) microtime(true)), 0, 8);

        $this->client->schema()->create($this->table, function (Blueprint $table): void {
            $table->uint32('id');
            $table->string('category');
            $table->int32('value');
            $table->engine(new MergeTree())->orderBy('id');
        });

        $this->client->insert($this->table, [
            ['id' => 1, 'category' => 'a', 'value' => 10],
            ['id' => 2, 'category' => 'b', 'value' => 20],
            ['id' => 3, 'category' => 'a', 'value' => 30],
            ['id' => 4, 'category' => 'b', 'value' => 40],
            ['id' => 5, 'category' => 'a', 'value' => 50],
        ]);
    }

    protected function tearDown(): void
    {
        $this->dropTableSilently($this->table);
    }

    public function test_parallel_returns_one_statement_per_query(): void
    {
        $results = $this->client->parallel([
            'all'   => "SELECT * FROM `{$this->table}`",
            'cat_a' => "SELECT * FROM `{$this->table}` WHERE category = 'a'",
        ]);

        $this->assertArrayHasKey('all',   $results);
        $this->assertArrayHasKey('cat_a', $results);
        $this->assertInstanceOf(Statement::class, $results['all']);
        $this->assertInstanceOf(Statement::class, $results['cat_a']);
    }

    public function test_parallel_results_are_correct(): void
    {
        $results = $this->client->parallel([
            'all'   => "SELECT * FROM `{$this->table}`",
            'cat_a' => "SELECT * FROM `{$this->table}` WHERE category = 'a'",
            'cat_b' => "SELECT * FROM `{$this->table}` WHERE category = 'b'",
        ]);

        $this->assertCount(5, $results['all']->rows());
        $this->assertCount(3, $results['cat_a']->rows());
        $this->assertCount(2, $results['cat_b']->rows());
    }

    public function test_parallel_preserves_numeric_keys(): void
    {
        $results = $this->client->parallel([
            0 => "SELECT * FROM `{$this->table}` LIMIT 1",
            1 => "SELECT * FROM `{$this->table}` LIMIT 2",
        ]);

        $this->assertArrayHasKey(0, $results);
        $this->assertArrayHasKey(1, $results);
        $this->assertCount(1, $results[0]->rows());
        $this->assertCount(2, $results[1]->rows());
    }

    public function test_parallel_accepts_query_builders(): void
    {
        $results = $this->client->parallel([
            'low'  => $this->client->table($this->table)->where('value', '<', 30),
            'high' => $this->client->table($this->table)->where('value', '>=', 30),
        ]);

        $this->assertCount(2, $results['low']->rows());
        $this->assertCount(3, $results['high']->rows());
    }

    public function test_parallel_with_single_query(): void
    {
        $results = $this->client->parallel([
            'only' => "SELECT count() AS n FROM `{$this->table}`",
        ]);

        $this->assertCount(1, $results);
        $this->assertSame('5', (string) $results['only']->value());
    }

    public function test_parallel_with_empty_array_returns_empty(): void
    {
        $results = $this->client->parallel([]);

        $this->assertSame([], $results);
    }
}
