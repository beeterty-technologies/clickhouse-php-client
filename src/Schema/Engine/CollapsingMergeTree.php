<?php

namespace Beeterty\ClickHouse\Schema\Engine;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

class CollapsingMergeTree implements Engine
{
    /**
     * @param string $sign The sign column (Int8) that marks rows as state (+1) or cancel (-1).
     */
    public function __construct(
        private readonly string $sign,
    ) {}

    public function toSql(): string
    {
        return "CollapsingMergeTree({$this->sign})";
    }
}
