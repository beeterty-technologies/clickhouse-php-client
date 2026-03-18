<?php

namespace Beeterty\ClickHouse\Schema\Engine;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

class AggregatingMergeTree implements Engine
{
    /**
     * @inheritDoc
     */
    public function toSql(): string
    {
        return 'AggregatingMergeTree()';
    }
}
