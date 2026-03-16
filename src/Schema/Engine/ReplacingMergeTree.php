<?php

namespace Beeterty\ClickHouse\Schema\Engine;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

class ReplacingMergeTree implements Engine
{
    /**
     * @param string|null $version Optional version column used to pick the latest row.
     */
    public function __construct(
        private readonly ?string $version = null,
    ) {}

    public function toSql(): string
    {
        return $this->version
            ? "ReplacingMergeTree({$this->version})"
            : 'ReplacingMergeTree()';
    }
}
