<?php

namespace Beeterty\ClickHouse\Schema\Engine;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

/**
 * The Null engine discards all written data and returns empty results on reads.
 * Useful for testing or as a target for Materialized Views.
 */
class NullEngine implements Engine
{
    public function toSql(): string
    {
        return 'Null()';
    }
}
