<?php

namespace Beeterty\ClickHouse\Exception;

class QueryException extends ClickHouseException
{
    /**
     * Construct a new QueryException.
     *
     * @param string $message The error message.
     * @param string $sql The SQL query that caused the exception.
     * @param \Throwable|null $previous The previous exception, if any.
     */
    public function __construct(
        string $message,
        public readonly string $sql,
        ?\Throwable $previous = null
    ) {
        parent::__construct($message, 0, $previous);
    }
}
