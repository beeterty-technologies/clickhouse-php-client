<?php

namespace Beeterty\ClickHouse\Schema\Engine;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

class AggregatingMergeTree implements Engine
{
    public function toSql(): string
    {
        return 'AggregatingMergeTree()';
    }
}
