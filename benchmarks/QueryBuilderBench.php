<?php

namespace Beeterty\ClickHouse\Benchmarks;

use Beeterty\ClickHouse\Client;
use Beeterty\ClickHouse\Config;
use Beeterty\ClickHouse\Query\Builder;
use Beeterty\ClickHouse\Query\JoinClause;
use PhpBench\Attributes\BeforeMethods;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;
use PhpBench\Attributes\Warmup;

/**
 * Benchmarks for Query\Builder::toSql() compilation speed.
 *
 * These run entirely in-process with no ClickHouse server required.
 * They measure how fast the builder assembles SQL strings for different
 * query shapes — useful for detecting regressions in the compilation path.
 */
#[Warmup(2)]
#[Iterations(5)]
class QueryBuilderBench
{
    private Builder $builder;

    public function setUp(): void
    {
        $this->builder = new Builder($this->createMockClient());
    }

    /**
     * Simple SELECT * FROM table — the baseline.
     */
    #[Revs(10000)]
    #[BeforeMethods('setUp')]
    public function benchSimpleSelect(): void
    {
        (clone $this->builder)
            ->table('events')
            ->toSql();
    }

    /**
     * SELECT with a WHERE clause and column list.
     */
    #[Revs(5000)]
    #[BeforeMethods('setUp')]
    public function benchSelectWhere(): void
    {
        (clone $this->builder)
            ->table('events')
            ->select('id', 'user_id', 'event', 'ts')
            ->where('status', 'active')
            ->where('ts', '>=', '2024-01-01')
            ->toSql();
    }

    /**
     * Aggregation query — GROUP BY + HAVING + ORDER BY + LIMIT.
     */
    #[Revs(5000)]
    #[BeforeMethods('setUp')]
    public function benchAggregation(): void
    {
        (clone $this->builder)
            ->table('events')
            ->select('user_id')
            ->addSelectRaw('count() AS n, avg(score) AS avg_score')
            ->where('status', 'active')
            ->groupBy('user_id')
            ->having('n > 10')
            ->orderByDesc('n')
            ->limit(100)
            ->toSql();
    }

    /**
     * JOIN with multiple conditions — exercises the JoinClause path.
     */
    #[Revs(5000)]
    #[BeforeMethods('setUp')]
    public function benchJoin(): void
    {
        (clone $this->builder)
            ->table('events')
            ->select('events.id', 'users.name')
            ->join('users', function (JoinClause $join): void {
                $join->on('events.user_id', '=', 'users.id')
                     ->on('events.tenant_id', '=', 'users.tenant_id');
            })
            ->where('events.status', 'active')
            ->limit(50)
            ->toSql();
    }

    /**
     * CTE + UNION — exercises the WITH and UNION compilation paths.
     */
    #[Revs(2000)]
    #[BeforeMethods('setUp')]
    public function benchCteUnion(): void
    {
        $sub = (clone $this->builder)->table('events_archive')->select('id', 'ts');

        (clone $this->builder)
            ->with('recent', (clone $this->builder)->table('events')->where('ts', '>=', '2024-01-01'))
            ->table('recent')
            ->select('id', 'ts')
            ->unionAll($sub)
            ->toSql();
    }

    /**
     * Full query with every major clause — worst-case compilation path.
     */
    #[Revs(2000)]
    #[BeforeMethods('setUp')]
    public function benchFullQuery(): void
    {
        (clone $this->builder)
            ->table('events')
            ->select('user_id', 'region')
            ->addSelectRaw('count() AS n, sum(revenue) AS total')
            ->prewhere('event_date', '>=', '2024-01-01')
            ->where('status', 'active')
            ->whereIn('region', ['EU', 'US', 'APAC'])
            ->groupBy('user_id', 'region')
            ->having('n > 5')
            ->orderByDesc('total')
            ->orderBy('user_id')
            ->limit(100)
            ->offset(200)
            ->toSql();
    }

    private function createMockClient(): Client
    {
        // Client::__construct only stores config and pre-fills curl pool — safe to use offline.
        return new Client(new Config());
    }
}
