<?php

namespace Beeterty\ClickHouse\Schema\Engine;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

class SummingMergeTree implements Engine
{
    /**
     * SummingMergeTree engine for tables that require summing of numeric columns during merges.
     * 
     * @param string[] $columns Columns to sum. If empty, all numeric columns are summed.
     */
    public function __construct(
        private readonly array $columns = [],
    ) {}

    /**
     * @inheritDoc
     */
    public function toSql(): string
    {
        return empty($this->columns)
            ? 'SummingMergeTree()'
            : 'SummingMergeTree(' . implode(', ', $this->columns) . ')';
    }
}
