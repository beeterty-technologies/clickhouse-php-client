<?php

namespace Beeterty\ClickHouse\Format;

use Beeterty\ClickHouse\Format\Contracts\Format;

/**
 * ClickHouse JSONCompactEachRowWithNamesAndTypes format.
 *
 * Like JSONCompactEachRow, but the first two rows are metadata headers:
 *   Row 1 — JSON array of column names.
 *   Row 2 — JSON array of ClickHouse type strings for each column.
 *   Row 3+ — Data rows as JSON arrays.
 *
 * Output example:
 *   ["date","user_id","event"]
 *   ["Date","UInt64","LowCardinality(String)"]
 *   ["2024-01-01",1,"click"]
 *   ["2024-01-02",2,"view"]
 *
 * Decoded rows are associative arrays keyed by the column names from row 1,
 * making this format as convenient to work with as JSONEachRow while still
 * producing more compact payloads than the object-per-row form.
 *
 * Note: when encoding for INSERT, column types are inferred from the PHP values
 * in the first row (e.g. int → Int64, float → Float64, string → String). This
 * is a best-effort inference — if the inferred types do not match the table
 * schema, ClickHouse will attempt to coerce the values.
 *
 * @see https://clickhouse.com/docs/en/interfaces/formats/JSONCompactEachRowWithNamesAndTypes
 */
final class JsonCompactEachRowWithNamesAndTypes implements Format
{
    /**
     * @inheritDoc
     */
    public function name(): string
    {
        return 'JSONCompactEachRowWithNamesAndTypes';
    }

    /**
     * Encode rows as newline-delimited JSON arrays preceded by names and types header rows.
     *
     * The column names are derived from the keys of the first row. Types are
     * inferred from the corresponding values in the first row.
     *
     * @param array<int, array<string, mixed>> $rows
     * @return string
     */
    public function encode(array $rows): string
    {
        if (empty($rows)) {
            return '';
        }

        $columns = array_keys($rows[0]);
        $types   = array_map($this->inferType(...), array_values($rows[0]));

        $lines   = [];
        $lines[] = json_encode($columns, JSON_THROW_ON_ERROR);
        $lines[] = json_encode($types, JSON_THROW_ON_ERROR);

        foreach ($rows as $row) {
            $lines[] = json_encode(array_values($row), JSON_THROW_ON_ERROR);
        }

        return implode("\n", $lines);
    }

    /**
     * Decode the response into associative arrays using the names header row.
     *
     * The first line is parsed as column names. The second line (types) is
     * discarded. Every subsequent line is combined with the column names to
     * produce a string-keyed associative array identical in shape to what
     * JSONEachRow returns.
     *
     * @param string $raw
     * @return array<int, array<string, mixed>>
     */
    public function decode(string $raw): array
    {
        if (trim($raw) === '') {
            return [];
        }

        $lines = array_values(array_filter(explode("\n", trim($raw))));

        if (count($lines) < 2) {
            return [];
        }

        /** @var string[] $columns */
        $columns = json_decode($lines[0], true, flags: JSON_THROW_ON_ERROR);

        $rows = [];

        for ($i = 2; $i < count($lines); $i++) {
            /** @var array<int, mixed> $values */
            $values = json_decode($lines[$i], true, flags: JSON_THROW_ON_ERROR);
            $rows[] = array_combine($columns, $values);
        }

        return $rows;
    }

    /**
     * Infer a ClickHouse type string from a PHP value.
     *
     * This is a best-effort mapping used when encoding rows for INSERT. The
     * inferred type may not match the actual column type exactly; ClickHouse
     * will attempt to coerce mismatched values.
     *
     * @param mixed $value
     * @return string
     */
    private function inferType(mixed $value): string
    {
        return match (true) {
            is_bool($value)  => 'UInt8',
            is_int($value)   => 'Int64',
            is_float($value) => 'Float64',
            is_null($value)  => 'Nullable(String)',
            is_array($value) => 'Array(String)',
            default          => 'String',
        };
    }
}
