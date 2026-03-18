<?php

namespace Beeterty\ClickHouse\Schema\Engine;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

class Log implements Engine
{
    /**
     * @inheritDoc
     */
    public function toSql(): string
    {
        return 'Log()';
    }
}
