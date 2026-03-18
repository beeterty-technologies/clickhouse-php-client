<?php

namespace Beeterty\ClickHouse\Format;

use Beeterty\ClickHouse\Format\Contracts\Format;

/**
 * ClickHouse TabSeparatedWithNames format.
 *
 * The first row is a header line containing column names. Each subsequent row
 * is a tab-separated record. Column names and values are separated by a single
 * tab character (`\t`); rows are separated by a newline (`\n`).
 *
 * @see https://clickhouse.com/docs/en/interfaces/formats#tabseparatedwithnames
 */
final class TabSeparated implements Format
{
    /** 
     * Column names parsed from the header row during {@see decode()}.
     * 
     * @var string[]
     */
    private array $headers = [];

    /**
     * @inheritDoc
     */
    public function name(): string
    {
        return 'TabSeparatedWithNames';
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
        $lines = [implode("\t", $headers)];

        foreach ($rows as $row) {
            $lines[] = implode("\t", array_values($row));
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

        $this->headers = explode("\t", array_shift($lines));

        $rows = [];

        foreach ($lines as $line) {
            if (trim($line) === '') {
                continue;
            }

            $values = explode("\t", $line);

            $rows[] = array_combine($this->headers, $values);
        }

        return $rows;
    }

    /**
     * Return the column names parsed from the most recent {@see decode()} call.
     *
     * Useful when decoding a raw ClickHouse response and you need to inspect
     * the column order independently of the row data.
     *
     * @return string[]
     */
    public function headers(): array
    {
        return $this->headers;
    }
}
