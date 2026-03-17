<?php

namespace Beeterty\ClickHouse\Tests\Integration;

use Beeterty\ClickHouse\Schema\Blueprint;
use Beeterty\ClickHouse\Schema\Engine\MergeTree;

class QueryBuilderIntegrationTest extends IntegrationTestCase
{
    private string $table;

    protected function setUp(): void
    {
        parent::setUp();

        $this->table = 'qb_test_' . substr(md5((string) microtime(true)), 0, 8);

        $this->client->schema()->create($this->table, function (Blueprint $table): void {
            $table->uint32('id');
            $table->string('status');
            $table->int32('score');
            $table->engine(new MergeTree())->orderBy('id');
        });

        $this->client->insert($this->table, [
            ['id' => 1, 'status' => 'active',   'score' => 90],
            ['id' => 2, 'status' => 'active',   'score' => 70],
            ['id' => 3, 'status' => 'inactive', 'score' => 50],
            ['id' => 4, 'status' => 'active',   'score' => 80],
            ['id' => 5, 'status' => 'inactive', 'score' => 60],
        ]);
    }

    protected function tearDown(): void
    {
        $this->dropTableSilently($this->table);
    }

    public function test_table_select_all_returns_all_rows(): void
    {
        $rows = $this->client->table($this->table)->get()->rows();

        $this->assertCount(5, $rows);
    }

    public function test_where_filters_rows(): void
    {
        $rows = $this->client->table($this->table)
            ->where('status', 'active')
            ->get()->rows();

        $this->assertCount(3, $rows);
    }

    public function test_where_with_operator(): void
    {
        $rows = $this->client->table($this->table)
            ->where('score', '>=', 80)
            ->get()->rows();

        $this->assertCount(2, $rows);
    }

    public function test_where_in(): void
    {
        $rows = $this->client->table($this->table)
            ->whereIn('id', [1, 3])
            ->get()->rows();

        $this->assertCount(2, $rows);
    }

    public function test_where_between(): void
    {
        $rows = $this->client->table($this->table)
            ->whereBetween('score', 60, 80)
            ->get()->rows();

        $this->assertCount(3, $rows);
    }

    public function test_order_by_desc(): void
    {
        $scores = array_map(
            'intval',
            $this->client->table($this->table)->orderByDesc('score')->pluck('score')
        );

        $this->assertSame([90, 80, 70, 60, 50], $scores);
    }

    public function test_limit_restricts_rows(): void
    {
        $rows = $this->client->table($this->table)->limit(2)->get()->rows();
        $this->assertCount(2, $rows);
    }

    public function test_limit_and_offset(): void
    {
        $all    = $this->client->table($this->table)->orderBy('id')->get()->rows();
        $paged  = $this->client->table($this->table)->orderBy('id')->limit(2)->offset(2)->get()->rows();

        $this->assertCount(2, $paged);
        $this->assertSame($all[2]['id'], $paged[0]['id']);
    }

    public function test_select_specific_columns(): void
    {
        $rows = $this->client->table($this->table)->select('id', 'status')->limit(1)->get()->rows();

        $this->assertArrayHasKey('id', $rows[0]);
        $this->assertArrayHasKey('status', $rows[0]);
        $this->assertArrayNotHasKey('score', $rows[0]);
    }

    public function test_select_raw(): void
    {
        $total = $this->client->table($this->table)->selectRaw('count() AS n')->value();

        $this->assertSame('5', (string) $total);
    }

    public function test_count_terminal(): void
    {
        $count = $this->client->table($this->table)->where('status', 'active')->count();

        $this->assertSame(3, $count);
    }

    public function test_first_terminal(): void
    {
        $row = $this->client->table($this->table)->orderBy('id')->first();

        $this->assertNotNull($row);
        $this->assertSame('1', (string) $row['id']);
    }

    public function test_pluck_terminal(): void
    {
        $statuses = $this->client->table($this->table)->orderBy('id')->pluck('status');

        $this->assertCount(5, $statuses);
        $this->assertSame('active', $statuses[0]);
    }

    public function test_chunk_paginates_all_rows(): void
    {
        $collected = [];

        $this->client->table($this->table)
            ->orderBy('id')
            ->chunk(2, function (array $rows) use (&$collected): void {
                foreach ($rows as $row) {
                    $collected[] = $row['id'];
                }
            });

        $this->assertCount(5, $collected);
    }

    public function test_chunk_stops_on_false(): void
    {
        $batches = 0;

        $this->client->table($this->table)
            ->orderBy('id')
            ->chunk(2, function () use (&$batches): bool {
                $batches++;
                return false;
            });

        $this->assertSame(1, $batches);
    }

    public function test_group_by_and_select_raw(): void
    {
        $rows = $this->client->table($this->table)
            ->select('status')
            ->addSelectRaw('count() AS n')
            ->groupBy('status')
            ->orderBy('status')
            ->get()->rows();

        $this->assertCount(2, $rows);
        $this->assertSame('active', $rows[0]['status']);
        $this->assertSame('3', (string) $rows[0]['n']);
    }
}
