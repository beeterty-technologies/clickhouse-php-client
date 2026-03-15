<?php

namespace Beeterty\ClickHouse\Format;

use Beeterty\ClickHouse\Format\Contracts\Format;

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
        return implode("\n", array_map(
            fn(array $row) => json_encode($row, JSON_THROW_ON_ERROR),
            $rows
        ));
    }

    /**
     * @inheritDoc
     */
    public function decode(string $raw): array
    {
        if (trim($raw) === '') {
            return [];
        }

        return array_map(
            fn(string $line) => json_decode($line, true, flags: JSON_THROW_ON_ERROR),
            array_filter(explode("\n", trim($raw)))
        );
    }
}
