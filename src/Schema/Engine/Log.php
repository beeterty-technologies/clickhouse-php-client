<?php

namespace Beeterty\ClickHouse\Schema\Engine;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

class Log implements Engine
{
    public function toSql(): string
    {
        return 'Log()';
    }
}
