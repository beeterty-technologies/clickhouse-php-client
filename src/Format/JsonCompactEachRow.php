<?php

namespace Beeterty\ClickHouse\Format;

use Beeterty\ClickHouse\Format\Contracts\Format;

/**
 * ClickHouse JSONCompactEachRow format.
 *
 * Like JSONEachRow, but each row is a JSON array of values rather than a JSON
 * object. Column names are not included in the output, making payloads smaller
 * and parsing faster at the cost of losing self-describing column information.
 *
 * Output example:
 *   ["2024-01-01", 1, "click"]
 *   ["2024-01-02", 2, "view"]
 *
 * Note: decoded rows are integer-indexed arrays because column names are not
 * present in the wire format. Use JSONCompactEachRowWithNamesAndTypes if you
 * need column names available after decoding.
 *
 * @see https://clickhouse.com/docs/en/interfaces/formats/JSONCompactEachRow
 */
final class JsonCompactEachRow implements Format
{
    /**
     * @inheritDoc
     */
    public function name(): string
    {
        return 'JSONCompactEachRow';
    }

    /**
     * Encode rows as newline-delimited JSON arrays.
     *
     * Each associative row is converted to an ordered value array via
     * array_values(), discarding the PHP-side column keys. The order of
     * values matches the insertion order of the row array's keys.
     *
     * @param array<int, array<string, mixed>> $rows
     * @return string
     */
    public function encode(array $rows): string
    {
        return implode("\n", array_map(
            fn(array $row) => json_encode(array_values($row), JSON_THROW_ON_ERROR),
            $rows
        ));
    }

    /**
     * Decode newline-delimited JSON arrays into integer-indexed row arrays.
     *
     * Each line is decoded as a JSON array. The result is an array of arrays
     * where each inner array is integer-indexed (no column names).
     *
     * @param string $raw
     * @return list<list<mixed>>
     */
    public function decode(string $raw): array
    {
        if (trim($raw) === '') {
            return [];
        }

        $rows = [];

        foreach (array_filter(explode("\n", trim($raw))) as $line) {
            $decoded = json_decode($line, true, flags: JSON_THROW_ON_ERROR);
            $rows[]  = \is_array($decoded) ? array_values($decoded) : [];
        }

        return $rows;
    }
}
