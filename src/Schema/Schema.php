<?php

namespace Beeterty\ClickHouse\Schema;

use Beeterty\ClickHouse\Client;

/**
 * Entry point for all ClickHouse schema operations.
 *
 * Obtain an instance via $client->schema().
 *
 * Example — create a table:
 *
 *   $client->schema()->create('events', function (Blueprint $table) {
 *       $table->uint64('id');
 *       $table->string('name')->lowCardinality();
 *       $table->dateTime('occurred_at', 'UTC');
 *       $table->engine(new MergeTree())->orderBy('occurred_at');
 *   });
 *
 * Example — alter a table:
 *
 *   $client->schema()->table('events', function (Blueprint $table) {
 *       $table->uint32('score')->default(0);   // ADD COLUMN
 *       $table->dropColumn('legacy_field');    // DROP COLUMN
 *       $table->renameColumn('ts', 'created_at');
 *   });
 */
class Schema
{
    private readonly Grammar $grammar;

    public function __construct(
        private readonly Client $client,
    ) {
        $this->grammar = new Grammar();
    }

    // ─── Table lifecycle ──────────────────────────────────────────────────────

    /**
     * Create a new table.
     */
    public function create(string $table, callable $callback): void
    {
        $blueprint = new Blueprint();
        $callback($blueprint);

        $this->client->execute($this->grammar->compileCreate($table, $blueprint));
    }

    /**
     * Create a new table only if it does not already exist.
     */
    public function createIfNotExists(string $table, callable $callback): void
    {
        $blueprint = new Blueprint();
        $callback($blueprint);

        $this->client->execute($this->grammar->compileCreateIfNotExists($table, $blueprint));
    }

    /**
     * Drop a table.
     */
    public function drop(string $table): void
    {
        $this->client->execute($this->grammar->compileDrop($table));
    }

    /**
     * Drop a table if it exists.
     */
    public function dropIfExists(string $table): void
    {
        $this->client->execute($this->grammar->compileDropIfExists($table));
    }

    /**
     * Alter an existing table (ADD / DROP / RENAME columns).
     */
    public function table(string $table, callable $callback): void
    {
        $blueprint = new Blueprint();
        $callback($blueprint);

        foreach ($this->grammar->compileAlter($table, $blueprint) as $sql) {
            $this->client->execute($sql);
        }
    }

    /**
     * Rename a table.
     */
    public function rename(string $from, string $to): void
    {
        $this->client->execute($this->grammar->compileRename($from, $to));
    }

    // ─── Introspection ────────────────────────────────────────────────────────

    /**
     * Check whether a table exists in the current database.
     */
    public function hasTable(string $table): bool
    {
        return !$this->client
            ->query(
                'SELECT name FROM system.tables WHERE database = currentDatabase() AND name = :table',
                ['table' => $table],
            )
            ->isEmpty();
    }

    /**
     * Check whether a column exists on the given table.
     */
    public function hasColumn(string $table, string $column): bool
    {
        return !$this->client
            ->query(
                'SELECT name FROM system.columns WHERE database = currentDatabase() AND table = :table AND name = :column',
                ['table' => $table, 'column' => $column],
            )
            ->isEmpty();
    }

    /**
     * Return all column metadata rows for a table.
     *
     * Each row contains: name, type, default_kind, default_expression, comment.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getColumns(string $table): array
    {
        return $this->client
            ->query(
                'SELECT name, type, default_kind, default_expression, comment
                 FROM system.columns
                 WHERE database = currentDatabase() AND table = :table
                 ORDER BY position',
                ['table' => $table],
            )
            ->rows();
    }

    /**
     * Return all table metadata rows for the current database.
     *
     * Each row contains: name, engine, total_rows, total_bytes.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getTables(): array
    {
        return $this->client
            ->query(
                'SELECT name, engine, total_rows, total_bytes
                 FROM system.tables
                 WHERE database = currentDatabase()
                 ORDER BY name',
            )
            ->rows();
    }
}
