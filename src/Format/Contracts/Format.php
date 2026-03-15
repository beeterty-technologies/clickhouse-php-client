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
     * @param array $rows
     * @return string
     */
    public function encode(array $rows): string;

    /**
     * Decode the raw data into an array.
     *
     * @param string $raw
     * @return array
     */
    public function decode(string $raw): array;
}
