<?php

namespace Beeterty\ClickHouse\Query;

use Beeterty\ClickHouse\Format\Contracts\Format;
use Countable;
use IteratorAggregate;
use ArrayIterator;
use Traversable;

/** @implements IteratorAggregate<int, array<string, mixed>> */
class Statement implements Countable, IteratorAggregate
{
    /**
     * The decoded rows from the raw response body, lazily populated on first access.
     *
     * @var array<int, array<string, mixed>>|null
     */
    private ?array $decoded = null;

    /**
     * Create a new Statement instance.
     *
     * Statements are returned by Client::query() and Client::parallel() — you do
     * not normally construct them directly.
     *
     * @param string               $raw     Raw response body from the ClickHouse HTTP interface.
     * @param Format               $format  Format instance used to decode the raw body into rows.
     * @param array<string,string> $headers HTTP response headers from ClickHouse.
     */
    public function __construct(
        private readonly string $raw,
        private readonly Format $format,
        private readonly array $headers = [],
    ) {
        //
    }

    /**
     * Return all decoded rows as an array of associative arrays.
     *
     * The result is decoded lazily and cached — subsequent calls return the same
     * array without re-parsing the raw response body.
     *
     * Example:
     *   $rows = $client->query('SELECT id, name FROM users LIMIT 10')->rows();
     *   foreach ($rows as $row) {
     *       echo $row['name'];
     *   }
     *
     * @return array<int, array<string, mixed>>
     */
    public function rows(): array
    {
        return $this->decoded ??= $this->format->decode($this->raw);
    }

    /**
     * Return the first row, or null if the result is empty.
     *
     * Example:
     *   $user = $client->query('SELECT * FROM users WHERE id = 1')->first();
     *   // → ['id' => 1, 'name' => 'Alice']  or null
     *
     * @return array<string, mixed>|null
     */
    public function first(): ?array
    {
        return $this->rows()[0] ?? null;
    }

    /**
     * Return the first column of the first row as a scalar value.
     *
     * Ideal for single-value aggregate queries where only one cell is expected.
     * Returns null when the result set is empty.
     *
     * Example:
     *   $count = $client->query('SELECT count() FROM events')->value();
     *   // → 42
     *
     *   $max = $client->query('SELECT max(score) FROM users')->value();
     *   // → 99
     *
     * @return mixed The scalar value from the first column of the first row, or null.
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
     * Example:
     *   $ids = $client->query('SELECT id FROM users ORDER BY id')->pluck('id');
     *   // → [1, 2, 3, ...]
     *
     * @param string $column The column name whose values to extract.
     * @return array<int, mixed>
     */
    public function pluck(string $column): array
    {
        return array_column($this->rows(), $column);
    }

    /**
     * Return the number of rows in the result.
     *
     * Also satisfies the Countable interface, so you can pass a Statement directly
     * to PHP's count() function.
     *
     * Example:
     *   $stmt = $client->query('SELECT * FROM events LIMIT 10');
     *   echo count($stmt);  // → 10
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
     * Example:
     *   if ($client->query('SELECT * FROM events WHERE id = 999')->isEmpty()) {
     *       // not found
     *   }
     *
     * @return bool
     */
    public function isEmpty(): bool
    {
        return $this->count() === 0;
    }

    /**
     * Return the raw response body as received from the ClickHouse HTTP interface.
     *
     * Useful for debugging or for passing the raw bytes to a custom parser.
     *
     * @return string
     */
    public function raw(): string
    {
        return $this->raw;
    }

    /**
     * Return the ClickHouse query ID assigned to this request by the server.
     *
     * The query ID appears in the X-ClickHouse-Query-Id response header and can
     * be used to correlate entries in system.query_log or to kill a running query
     * via Client::kill().
     *
     * Example:
     *   $stmt = $client->query('SELECT count() FROM events');
     *   $id   = $stmt->queryId();  // → '5e4d…'
     *
     * @see https://clickhouse.com/docs/en/operations/system-tables/query_log
     *
     * @return string|null The query ID, or null if the header was not present.
     */
    public function queryId(): ?string
    {
        return $this->headers['X-ClickHouse-Query-Id'] ?? null;
    }

    /**
     * Return the execution summary reported by ClickHouse as an associative array.
     *
     * The summary is decoded from the X-ClickHouse-Summary response header and
     * contains keys such as read_rows, read_bytes, written_rows, written_bytes,
     * total_rows_to_read, result_rows, result_bytes, and elapsed_ns.
     *
     * Example:
     *   $summary = $stmt->summary();
     *   echo $summary['read_rows'];   // → '150000'
     *   echo $summary['elapsed_ns'];  // → '42000000'
     *
     * @see https://clickhouse.com/docs/en/interfaces/http#response-headers
     *
     * @return array<string, mixed> Decoded summary map, or an empty array if the header was absent.
     */
    public function summary(): array
    {
        $raw = $this->headers['X-ClickHouse-Summary'] ?? '{}';

        return json_decode($raw, true) ?? [];
    }

    /**
     * Process result rows in batches of $size, calling $callback for each batch.
     *
     * Return false from the callback to stop iteration early. Useful for
     * processing large in-memory result sets without a single allocation.
     *
     * Example:
     *   $statement->chunk(100, function (array $rows): void {
     *       foreach ($rows as $row) {
     *           // process row
     *       }
     *   });
     *
     * Note: this method chunks an already-fetched result set that is held in
     * memory. For streaming large datasets without loading them entirely, use
     * Builder::chunk() which paginates via LIMIT + OFFSET at the query level.
     *
     * @param int      $size     Number of rows per chunk (minimum 1).
     * @param callable $callback Receives an array of rows; return false to stop early.
     * @return void
     */
    public function chunk(int $size, callable $callback): void
    {
        if ($size < 1) {
            $size = 1;
        }

        foreach (array_chunk($this->rows(), $size) as $chunk) {
            if ($callback($chunk) === false) {
                break;
            }
        }
    }

    /**
     * Return an iterator over the result rows.
     *
     * Satisfies the IteratorAggregate interface so that a Statement can be used
     * directly in a foreach loop.
     *
     * Example:
     *   foreach ($client->query('SELECT * FROM events LIMIT 5') as $row) {
     *       echo $row['id'];
     *   }
     *
     * @return Traversable<int, array<string, mixed>>
     */
    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->rows());
    }
}
