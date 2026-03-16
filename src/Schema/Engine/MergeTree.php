<?php

namespace Beeterty\ClickHouse\Schema\Engine;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

class MergeTree implements Engine
{
    public function toSql(): string
    {
        return 'MergeTree()';
    }
}
