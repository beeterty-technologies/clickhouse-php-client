<?php

namespace Beeterty\ClickHouse\Benchmarks;

use Beeterty\ClickHouse\Format\JsonEachRow;
use Beeterty\ClickHouse\Query\Statement;
use PhpBench\Attributes\BeforeMethods;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;
use PhpBench\Attributes\Warmup;

/**
 * Benchmarks for Statement construction and iteration.
 *
 * Measures how fast the client can parse and iterate over result payloads
 * of different sizes — useful for tracking decode overhead regressions.
 */
#[Warmup(2)]
#[Iterations(5)]
class StatementBench
{
    private string $payload100;
    private string $payload1000;
    private string $payload10000;
    private JsonEachRow $format;

    public function setUp(): void
    {
        $this->format       = new JsonEachRow();
        $this->payload100   = $this->buildPayload(100);
        $this->payload1000  = $this->buildPayload(1000);
        $this->payload10000 = $this->buildPayload(10000);
    }

    /**
     * Parse and iterate 100 rows.
     */
    #[Revs(1000)]
    #[BeforeMethods('setUp')]
    public function benchRows100(): void
    {
        $stmt = new Statement($this->payload100, $this->format, []);
        foreach ($stmt as $row) {
            // iterate
        }
    }

    /**
     * Parse and iterate 1 000 rows.
     */
    #[Revs(200)]
    #[BeforeMethods('setUp')]
    public function benchRows1000(): void
    {
        $stmt = new Statement($this->payload1000, $this->format, []);
        foreach ($stmt as $row) {
            // iterate
        }
    }

    /**
     * Parse and iterate 10 000 rows.
     */
    #[Revs(20)]
    #[BeforeMethods('setUp')]
    public function benchRows10000(): void
    {
        $stmt = new Statement($this->payload10000, $this->format, []);
        foreach ($stmt as $row) {
            // iterate
        }
    }

    /**
     * `pluck()` on 1 000 rows — tests the hot path used by many callers.
     */
    #[Revs(200)]
    #[BeforeMethods('setUp')]
    public function benchPluck1000(): void
    {
        $stmt = new Statement($this->payload1000, $this->format, []);
        $stmt->pluck('id');
    }

    /**
     * `chunk()` over 1 000 rows with chunk size 100.
     */
    #[Revs(200)]
    #[BeforeMethods('setUp')]
    public function benchChunk1000(): void
    {
        $stmt = new Statement($this->payload1000, $this->format, []);
        $stmt->chunk(100, static function (array $rows): void {
            // process
        });
    }

    private function buildPayload(int $count): string
    {
        $lines = [];
        for ($i = 0; $i < $count; $i++) {
            $lines[] = json_encode([
                'id'      => $i + 1,
                'user_id' => ($i % 50) + 1,
                'event'   => 'click',
                'score'   => round($i * 0.1, 2),
                'ts'      => '2024-01-01',
            ], JSON_THROW_ON_ERROR);
        }
        return implode("\n", $lines);
    }
}
