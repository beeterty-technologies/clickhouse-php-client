<?php

namespace Beeterty\ClickHouse\Benchmarks;

use Beeterty\ClickHouse\Format\Csv;
use Beeterty\ClickHouse\Format\JsonCompactEachRow;
use Beeterty\ClickHouse\Format\JsonCompactEachRowWithNamesAndTypes;
use Beeterty\ClickHouse\Format\JsonEachRow;
use Beeterty\ClickHouse\Format\TabSeparated;
use PhpBench\Attributes\BeforeMethods;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;
use PhpBench\Attributes\Warmup;

/**
 * Benchmarks for Format encode() and decode() throughput.
 *
 * Covers all five text formats with a realistic 100-row dataset.
 * No ClickHouse server required — pure in-process serialisation.
 */
#[Warmup(2)]
#[Iterations(5)]
class FormatBench
{
    /** @var array<int, array<string, mixed>> */
    private array $rows;

    private string $jsonEachRowEncoded;
    private string $compactEncoded;
    private string $compactWithNamesEncoded;
    private string $csvEncoded;
    private string $tsvEncoded;

    public function setUp(): void
    {
        $this->rows = $this->generateRows(100);

        $this->jsonEachRowEncoded         = (new JsonEachRow())->encode($this->rows);
        $this->compactEncoded             = (new JsonCompactEachRow())->encode($this->rows);
        $this->compactWithNamesEncoded    = (new JsonCompactEachRowWithNamesAndTypes())->encode($this->rows);
        $this->csvEncoded                 = (new Csv())->encode($this->rows);
        $this->tsvEncoded                 = (new TabSeparated())->encode($this->rows);
    }

    // ─── JsonEachRow ──────────────────────────────────────────────────────────

    #[Revs(1000)]
    #[BeforeMethods('setUp')]
    public function benchJsonEachRowEncode(): void
    {
        (new JsonEachRow())->encode($this->rows);
    }

    #[Revs(1000)]
    #[BeforeMethods('setUp')]
    public function benchJsonEachRowDecode(): void
    {
        (new JsonEachRow())->decode($this->jsonEachRowEncoded);
    }

    // ─── JSONCompactEachRow ───────────────────────────────────────────────────

    #[Revs(1000)]
    #[BeforeMethods('setUp')]
    public function benchJsonCompactEncode(): void
    {
        (new JsonCompactEachRow())->encode($this->rows);
    }

    #[Revs(1000)]
    #[BeforeMethods('setUp')]
    public function benchJsonCompactDecode(): void
    {
        (new JsonCompactEachRow())->decode($this->compactEncoded);
    }

    // ─── JSONCompactEachRowWithNamesAndTypes ──────────────────────────────────

    #[Revs(1000)]
    #[BeforeMethods('setUp')]
    public function benchJsonCompactWithNamesEncode(): void
    {
        (new JsonCompactEachRowWithNamesAndTypes())->encode($this->rows);
    }

    #[Revs(1000)]
    #[BeforeMethods('setUp')]
    public function benchJsonCompactWithNamesDecode(): void
    {
        (new JsonCompactEachRowWithNamesAndTypes())->decode($this->compactWithNamesEncoded);
    }

    // ─── CSV ─────────────────────────────────────────────────────────────────

    #[Revs(500)]
    #[BeforeMethods('setUp')]
    public function benchCsvEncode(): void
    {
        (new Csv())->encode($this->rows);
    }

    #[Revs(500)]
    #[BeforeMethods('setUp')]
    public function benchCsvDecode(): void
    {
        (new Csv())->decode($this->csvEncoded);
    }

    // ─── TabSeparated ─────────────────────────────────────────────────────────

    #[Revs(500)]
    #[BeforeMethods('setUp')]
    public function benchTabSeparatedEncode(): void
    {
        (new TabSeparated())->encode($this->rows);
    }

    #[Revs(500)]
    #[BeforeMethods('setUp')]
    public function benchTabSeparatedDecode(): void
    {
        (new TabSeparated())->decode($this->tsvEncoded);
    }

    /**
     * Generate N realistic event rows.
     *
     * @return array<int, array<string, mixed>>
     */
    private function generateRows(int $count): array
    {
        $rows   = [];
        $events = ['click', 'view', 'purchase', 'scroll', 'hover'];

        for ($i = 0; $i < $count; $i++) {
            $rows[] = [
                'id'      => $i + 1,
                'user_id' => ($i % 50) + 1,
                'event'   => $events[$i % 5],
                'score'   => round(($i % 100) * 0.1, 2),
                'ts'      => '2024-01-' . str_pad((string)(($i % 28) + 1), 2, '0', STR_PAD_LEFT),
                'active'  => $i % 3 !== 0,
                'label'   => 'row_' . $i,
            ];
        }

        return $rows;
    }
}
