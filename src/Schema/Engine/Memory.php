<?php

namespace Beeterty\ClickHouse\Schema\Engine;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

class Memory implements Engine
{
    public function toSql(): string
    {
        return 'Memory()';
    }
}
