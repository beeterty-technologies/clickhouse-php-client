<?php

namespace Beeterty\ClickHouse\Schema\Contracts;

interface Engine
{
    /**
     * Compile the engine definition to its ClickHouse SQL representation.
     *
     * @return string
     */
    public function toSql(): string;
}
