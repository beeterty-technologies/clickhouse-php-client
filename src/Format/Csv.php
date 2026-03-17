<?php

namespace Beeterty\ClickHouse\Format;

use Beeterty\ClickHouse\Format\Contracts\Format;

/**
 * ClickHouse CSVWithNames format.
 *
 * The first row is a header line with column names. Each subsequent row is a
 * comma-separated record. Values containing commas, double-quotes, or newlines
 * are RFC 4180-quoted.
 */
final class Csv implements Format
{
    /**
     * @inheritDoc
     */
    public function name(): string
    {
        return 'CSVWithNames';
    }

    /**
     * @inheritDoc
     */
    public function encode(array $rows): string
    {
        if (empty($rows)) {
            return '';
        }

        $headers = array_keys($rows[0]);
        $lines = [$this->encodeLine($headers)];

        foreach ($rows as $row) {
            $lines[] = $this->encodeLine(array_values($row));
        }

        return implode("\n", $lines);
    }

    /**
     * @inheritDoc
     */
    public function decode(string $raw): array
    {
        if (trim($raw) === '') {
            return [];
        }

        $lines = explode("\n", trim($raw));

        $headers = array_map('strval', str_getcsv((string) array_shift($lines), ',', '"', ''));
        $rows = [];

        foreach ($lines as $line) {
            if (trim($line) === '') {
                continue;
            }

            $values = array_map('strval', str_getcsv($line, ',', '"', ''));
            $rows[] = array_combine($headers, $values);
        }

        return $rows;
    }

    /**
     * Encode a single row of values as a CSV line (no trailing newline).
     *
     * @param array<int, mixed> $values
     * @return string
     */
    private function encodeLine(array $values): string
    {
        $cells = array_map(static function (mixed $value): string {
            if ($value === null) {
                return '';
            }

            $str = (string) $value;

            if (
                str_contains($str, ',')
                || str_contains($str, '"')
                || str_contains($str, "\n")
                || str_contains($str, "\r")
            ) {
                return '"' . str_replace('"', '""', $str) . '"';
            }

            return $str;
        }, $values);

        return implode(',', $cells);
    }
}
