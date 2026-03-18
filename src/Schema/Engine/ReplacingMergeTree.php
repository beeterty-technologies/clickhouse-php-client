<?php

namespace Beeterty\ClickHouse\Schema\Engine;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

class ReplacingMergeTree implements Engine
{
    /**
     * ReplacingMergeTree engine for tables that require deduplication of rows based on a version column.
     * 
     * @param string|null $version Optional version column used to pick the latest row.
     */
    public function __construct(
        private readonly ?string $version = null,
    ) {
        // 
    }

    /**
     * @inheritDoc
     */
    public function toSql(): string
    {
        return $this->version
            ? "ReplacingMergeTree({$this->version})"
            : 'ReplacingMergeTree()';
    }
}
