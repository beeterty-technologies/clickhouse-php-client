<?php

namespace Beeterty\ClickHouse;

use Beeterty\ClickHouse\Format\Contracts\Format;
use Countable;
use IteratorAggregate;
use ArrayIterator;
use Traversable;

/** @implements IteratorAggregate<int, array<string, mixed>> */
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
     * @param string $raw     The raw response body from ClickHouse.
     * @param Format $format  The format used to decode the raw response.
     * @param array<string,string> $headers The response headers from ClickHouse.
     */
    public function __construct(
        private readonly string $raw,
        private readonly Format $format,
        private readonly array $headers = [],
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
     * Return a single scalar value from the first row and first column.
     *
     * Ideal for aggregate queries:
     *   $client->query('SELECT count() FROM events')->value() // → 42
     *
     * @return mixed
     */
    public function value(): mixed
    {
        $row = $this->first();

        if ($row === null) {
            return null;
        }

        return reset($row);
    }

    /**
     * Return a flat array of values for a single column across all rows.
     *
     * Ideal for SELECT-one-column queries:
     *   $client->query('SELECT id FROM users')->pluck('id') // → [1, 2, 3]
     *
     * @param string $column
     * @return array
     */
    public function pluck(string $column): array
    {
        return array_column($this->rows(), $column);
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
     * Return the raw response body from ClickHouse.
     *
     * @return string
     */
    public function raw(): string
    {
        return $this->raw;
    }

    /**
     * Return the ClickHouse query ID assigned to this request.
     *
     * @return string|null
     */
    public function queryId(): ?string
    {
        return $this->headers['X-ClickHouse-Query-Id'] ?? null;
    }

    /**
     * Return the execution summary reported by ClickHouse.
     *
     * Contains keys such as read_rows, read_bytes, written_rows, written_bytes,
     * total_rows_to_read, result_rows, result_bytes, elapsed_ns.
     *
     * @return array
     */
    public function summary(): array
    {
        $raw = $this->headers['X-ClickHouse-Summary'] ?? '{}';

        return json_decode($raw, true) ?? [];
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
