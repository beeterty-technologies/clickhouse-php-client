<?php

namespace Beeterty\ClickHouse\Schema\Engine;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

class MergeTree implements Engine
{
    /**
     * @inheritDoc
     */
    public function toSql(): string
    {
        return 'MergeTree()';
    }
}
