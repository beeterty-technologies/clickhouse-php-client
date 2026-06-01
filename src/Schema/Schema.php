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
     * @return array<int, array<array-key, mixed>>
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
     * @return array<int, array<array-key, mixed>>
     */
    public function getTables(): array
    {
        return $this->client
            ->query('SELECT name, engine, total_rows, total_bytes FROM system.tables WHERE database = currentDatabase() ORDER BY name')
            ->rows();
    }

    /**
     * Create a regular (non-materialized) view.
     *
     * A view stores only the SELECT query definition and evaluates it at read
     * time. No data is stored on disk. Use createMaterializedView() when you
     * need pre-computed, persisted results.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/create/view
     *
     * @param string $name      View name.
     * @param string $selectSql The SELECT query defining the view.
     */
    public function createView(string $name, string $selectSql): void
    {
        $this->client->execute($this->grammar->compileCreateView($name, $selectSql));
    }

    /**
     * Create a regular view only if one with the same name does not already exist.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/create/view
     *
     * @param string $name      View name.
     * @param string $selectSql The SELECT query defining the view.
     */
    public function createViewIfNotExists(string $name, string $selectSql): void
    {
        $this->client->execute($this->grammar->compileCreateView($name, $selectSql, ifNotExists: true));
    }

    /**
     * Attach an existing on-disk table to the server.
     *
     * ATTACH registers a table that already has data on disk without moving
     * any files. Useful after a DETACH or when manually placing data files.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/attach
     *
     * @param string $table Table name.
     */
    public function attach(string $table): void
    {
        $this->client->execute($this->grammar->compileAttach($table));
    }

    /**
     * Attach a table only if it is not already attached.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/attach
     *
     * @param string $table Table name.
     */
    public function attachIfNotExists(string $table): void
    {
        $this->client->execute($this->grammar->compileAttach($table, ifNotExists: true));
    }

    /**
     * Detach a table from the server without deleting its data.
     *
     * The table disappears from the server's in-memory state but its data
     * remains on disk. Re-register it later with attach().
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/detach
     *
     * @param string $table Table name.
     */
    public function detach(string $table): void
    {
        $this->client->execute($this->grammar->compileDetach($table));
    }

    /**
     * Detach a table if it is currently attached, silently succeeding otherwise.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/detach
     *
     * @param string $table Table name.
     */
    public function detachIfExists(string $table): void
    {
        $this->client->execute($this->grammar->compileDetach($table, ifExists: true));
    }

    /**
     * Freeze a specific partition (or all partitions) of a MergeTree table.
     *
     * Creates a local backup snapshot on the server's configured backup path.
     * The partition expression is passed as a raw SQL string; quoting rules
     * depend on the partition key type (integers need no quotes, strings do).
     *
     * Example:
     *   $schema->freeze('events', '202401')            // integer partition key
     *   $schema->freeze('events', "'2024-01-01'")      // string/Date partition key
     *   $schema->freeze('events')                      // freeze all partitions
     *   $schema->freeze('events', '202401', 'jan2024') // with backup name
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#freeze-partition
     *
     * @param string      $table      Table name.
     * @param string|null $partition  Partition expression. Null freezes all partitions.
     * @param string|null $backupName Optional name for the backup snapshot (WITH NAME).
     */
    public function freeze(string $table, ?string $partition = null, ?string $backupName = null): void
    {
        $this->client->execute($this->grammar->compileFreezePartition($table, $partition, $backupName));
    }

    /**
     * Move a partition from one MergeTree table to another on the same server.
     *
     * Both tables must have identical structure and the same MergeTree engine
     * family. The data is moved atomically — no copies are made.
     *
     * Example:
     *   $schema->movePartitionToTable('events', '202401', 'events_archive')
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#move-partition-to-table
     *
     * @param string $table       Source table name.
     * @param string $partition   Partition expression.
     * @param string $targetTable Destination table name.
     */
    public function movePartitionToTable(string $table, string $partition, string $targetTable): void
    {
        $this->client->execute($this->grammar->compileMovePartitionToTable($table, $partition, $targetTable));
    }

    /**
     * Move a partition to a named disk defined in the storage configuration.
     *
     * Example:
     *   $schema->movePartitionToDisk('events', '202401', 'hot_disk')
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#move-partition-to-disk-volume
     *
     * @param string $table     Table name.
     * @param string $partition Partition expression.
     * @param string $disk      Target disk name as defined in the storage policy.
     */
    public function movePartitionToDisk(string $table, string $partition, string $disk): void
    {
        $this->client->execute($this->grammar->compileMovePartitionToDisk($table, $partition, $disk));
    }

    /**
     * Move a partition to a named volume defined in the storage policy.
     *
     * Example:
     *   $schema->movePartitionToVolume('events', '202401', 'cold_volume')
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#move-partition-to-disk-volume
     *
     * @param string $table     Table name.
     * @param string $partition Partition expression.
     * @param string $volume    Target volume name as defined in the storage policy.
     */
    public function movePartitionToVolume(string $table, string $partition, string $volume): void
    {
        $this->client->execute($this->grammar->compileMovePartitionToVolume($table, $partition, $volume));
    }

    /**
     * Drop a dictionary.
     *
     * Note: CREATE DICTIONARY requires complex DDL (SOURCE, LAYOUT, LIFETIME
     * clauses) best expressed as raw SQL via Client::execute(). This method
     * covers the DROP side for symmetry.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/drop#drop-dictionary
     *
     * @param string $name Dictionary name.
     */
    public function dropDictionary(string $name): void
    {
        $this->client->execute($this->grammar->compileDropDictionary($name));
    }

    /**
     * Drop a dictionary if it exists, silently succeeding otherwise.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/drop#drop-dictionary
     *
     * @param string $name Dictionary name.
     */
    public function dropDictionaryIfExists(string $name): void
    {
        $this->client->execute($this->grammar->compileDropDictionaryIfExists($name));
    }
}
