<?php

namespace Beeterty\ClickHouse\Format;

use Beeterty\ClickHouse\Format\Contracts\Format;

/**
 * ClickHouse JSONEachRow format.
 *
 * Each newline-delimited line is a separate JSON object representing one row.
 * Object keys are column names; values are the corresponding column values.
 * This is the default format used by the client for SELECT queries and inserts
 * because it is self-describing, streamable, and trivially parsed.
 *
 * @see https://clickhouse.com/docs/en/interfaces/formats#jsoneachrow
 */
final class JsonEachRow implements Format
{
    /**
     * @inheritDoc
     */
    public function name(): string
    {
        return 'JSONEachRow';
    }

    /**
     * @inheritDoc
     */
    public function encode(array $rows): string
    {
        $lines = [];
        foreach ($rows as $row) {
            $lines[] = json_encode($row, JSON_THROW_ON_ERROR);
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

        $rows = [];

        foreach (array_filter(explode("\n", trim($raw))) as $line) {
            $decoded = json_decode($line, true, flags: JSON_THROW_ON_ERROR);
            $rows[]  = \is_array($decoded) ? $decoded : [];
        }

        return $rows;
    }
}
