<?php

namespace Beeterty\ClickHouse\Format\Contracts;

interface Format
{
    /**
     * Get the name of the format.
     * 
     * @return string
     */
    public function name(): string;

    /**
     * Encode an array of rows into the format's wire representation.
     *
     * @param array<int, array<string, mixed>> $rows
     * @return string
     */
    public function encode(array $rows): string;

    /**
     * Decode the raw data into an array of rows.
     *
     * Each row is an associative array (string keys for named formats such as
     * JSONEachRow, integer keys for compact formats such as JSONCompactEachRow).
     *
     * @param string $raw
     * @return array<int, array<array-key, mixed>>
     */
    public function decode(string $raw): array;
}
