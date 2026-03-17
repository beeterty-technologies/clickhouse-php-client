<?php

namespace Beeterty\ClickHouse\Schema;

use Beeterty\ClickHouse\Client;

class Schema
{
    /**
     * The Grammar instance used to compile SQL statements.
     *
     * @var Grammar
     */
    private readonly Grammar $grammar;

    /**
     * Create a new Schema instance.
     *
     * @param Client $client The ClickHouse client used to execute schema operations.
     */
    public function __construct(
        private readonly Client $client,
    ) {
        $this->grammar = new Grammar();
    }

    /**
     * Create a new table.
     *
     * @param string $table The name of the table to create.
     * @param callable $callback A callback that receives a Blueprint instance to define the table's columns and options.
     */
    public function create(string $table, callable $callback): void
    {
        $blueprint = new Blueprint();
        $callback($blueprint);

        $this->client->execute($this->grammar->compileCreate($table, $blueprint));
    }

    /**
     * Create a new table only if it does not already exist.
     *
     * @param string $table The name of the table to create.
     * @param callable $callback A callback that receives a Blueprint instance to define the table's columns and options.
     */
    public function createIfNotExists(string $table, callable $callback): void
    {
        $blueprint = new Blueprint();
        $callback($blueprint);

        $this->client->execute($this->grammar->compileCreateIfNotExists($table, $blueprint));
    }

    /**
     * Drop a table.
     *
     * @param string $table The name of the table to drop.
     */
    public function drop(string $table): void
    {
        $this->client->execute($this->grammar->compileDrop($table));
    }

    /**
     * Drop a table if it exists.
     *
     * @param string $table The name of the table to drop.
     */
    public function dropIfExists(string $table): void
    {
        $this->client->execute($this->grammar->compileDropIfExists($table));
    }

    /**
     * Alter an existing table (ADD / DROP / RENAME columns).
     * 
     * @param string $table The name of the table to alter.
     * @param callable $callback A callback that receives a Blueprint instance to define the table's modifications.
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
     *
     * @param string $from The current name of the table.
     * @param string $to The new name of the table.
     */
    public function rename(string $from, string $to): void
    {
        $this->client->execute($this->grammar->compileRename($from, $to));
    }

    /**
     * Create a materialized view that writes to an existing target table.
     *
     * @param string $name The name of the materialized view to create.
     * @param string $to The name of the target table where the view writes results.
     * @param string $selectSql The SELECT query that defines the view's contents.
     * @param bool $ifNotExists Whether to include IF NOT EXISTS in the statement (default: false).
     * @param bool $populate Whether to include POPULATE in the statement (default: false).
     */
    public function createMaterializedView(
        string $name,
        string $to,
        string $selectSql,
        bool $ifNotExists = false,
        bool $populate = false,
    ): void {
        $this->client->execute(
            $this->grammar->compileMaterializedView(
                $name,
                $to,
                $selectSql,
                $ifNotExists,
                $populate
            )
        );
    }

    /**
     * Drop a view (materialized or regular).
     * 
     * @param string $name The name of the view to drop.
     */
    public function dropView(string $name): void
    {
        $this->client->execute($this->grammar->compileDropView($name));
    }

    /**
     * Drop a view if it exists.
     * 
     * @param string $name The name of the view to drop.
     */
    public function dropViewIfExists(string $name): void
    {
        $this->client->execute($this->grammar->compileDropViewIfExists($name));
    }

    /**
     * Check whether a view exists in the current database.
     * 
     * @param string $name The name of the view to check for.
     * @return bool True if the view exists, false otherwise.
     */
    public function hasView(string $name): bool
    {
        return !$this->client
            ->query(
                "SELECT name FROM system.tables WHERE database = currentDatabase() AND name = :name AND engine LIKE '%View%'",
                ['name' => $name],
            )
            ->isEmpty();
    }

    /**
     * Check whether a table exists in the current database.
     * 
     * @param string $table The name of the table to check for.
     * @return bool True if the table exists, false otherwise.
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
     * 
     * @param string $table The name of the table to check.
     * @param string $column The name of the column to check for.
     * @return bool True if the column exists on the table, false otherwise.
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
     * @param string $table The name of the table to get columns for.
     * @return array<int, array<string, mixed>>
     */
    public function getColumns(string $table): array
    {
        return $this->client
            ->query(
                'SELECT name, type, default_kind, default_expression, comment FROM system.columns WHERE database = currentDatabase() AND table = :table ORDER BY position',
                ['table' => $table]
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
            ->query('SELECT name, engine, total_rows, total_bytes FROM system.tables WHERE database = currentDatabase() ORDER BY name')
            ->rows();
    }
}
