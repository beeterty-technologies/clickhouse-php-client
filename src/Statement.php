<?php

namespace Beeterty\ClickHouse;

use Beeterty\ClickHouse\Format\Contracts\Format;
use Countable;
use IteratorAggregate;
use ArrayIterator;
use Traversable;

class Statement implements Countable, IteratorAggregate
{
    /**
     * The decoded rows from the raw response.
     * 
     * @var array|null
     */
    private ?array $decoded = null;

    /**
     * Create a new Statement instance.
     *
     * @param string $raw The raw response string from ClickHouse.
     * @param Format $format The format used to decode the raw response.
     */
    public function __construct(
        private readonly string $raw,
        private readonly Format $format,
    ) {
        //
    }

    /**
     * Return all decoded rows.
     *
     * @return array
     */
    public function rows(): array
    {
        return $this->decoded ??= $this->format->decode($this->raw);
    }

    /**
     * Return the first row, or null if the result is empty.
     *
     * @return array|null
     */
    public function first(): ?array
    {
        return $this->rows()[0] ?? null;
    }

    /**
     * Return the number of rows in the result.
     *
     * @return int
     */
    public function count(): int
    {
        return count($this->rows());
    }

    /**
     * Return true if the result contains no rows.
     *
     * @return bool
     */
    public function isEmpty(): bool
    {
        return $this->count() === 0;
    }

    /**
     * Return the raw response string from ClickHouse.
     *
     * @return string
     */
    public function raw(): string
    {
        return $this->raw;
    }

    /**
     * Get an iterator for the rows in the result.
     * 
     * @return Traversable
     */
    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->rows());
    }
}
