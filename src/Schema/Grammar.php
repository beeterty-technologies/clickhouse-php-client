<?php

namespace Beeterty\ClickHouse\Schema;

/**
 * Compiles Blueprint definitions into raw ClickHouse DDL SQL strings.
 *
 * All statements use backtick-quoted identifiers for compatibility with
 * reserved keywords and mixed-case table/column names.
 *
 * @see https://clickhouse.com/docs/en/sql-reference/statements/create/table
 * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/column
 */
class Grammar
{
    /**
     * Compile a CREATE TABLE statement.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/create/table
     *
     * @param string    $table     Table name.
     * @param Blueprint $blueprint Blueprint describing columns and table options.
     * @return string
     */
    public function compileCreate(string $table, Blueprint $blueprint): string
    {
        return $this->buildCreateStatement("CREATE TABLE `{$table}`", $blueprint);
    }

    /**
     * Compile a CREATE TABLE IF NOT EXISTS statement.
     *
     * Silently skips creation when the table already exists rather than
     * raising a ClickHouse exception.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/create/table
     *
     * @param string    $table     Table name.
     * @param Blueprint $blueprint Blueprint describing columns and table options.
     * @return string
     */
    public function compileCreateIfNotExists(string $table, Blueprint $blueprint): string
    {
        return $this->buildCreateStatement("CREATE TABLE IF NOT EXISTS `{$table}`", $blueprint);
    }

    /**
     * Compile a DROP TABLE statement.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/drop#drop-table
     *
     * @param string $table Table name.
     * @return string
     */
    public function compileDrop(string $table): string
    {
        return "DROP TABLE `{$table}`";
    }

    /**
     * Compile a DROP TABLE IF EXISTS statement.
     *
     * Silently succeeds when the table does not exist.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/drop#drop-table
     *
     * @param string $table Table name.
     * @return string
     */
    public function compileDropIfExists(string $table): string
    {
        return "DROP TABLE IF EXISTS `{$table}`";
    }

    /**
     * Compile one or more ALTER TABLE statements.
     *
     * ClickHouse supports multiple comma-separated actions in a single
     * ALTER TABLE, so additions, drops, and renames are batched into one
     * statement. Returns an empty array when the blueprint has no changes.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/column
     *
     * @param string    $table     Table name.
     * @param Blueprint $blueprint Blueprint containing column modifications.
     * @return string[]
     */
    public function compileAlter(string $table, Blueprint $blueprint): array
    {
        $actions = [];

        foreach ($blueprint->getColumns() as $column) {
            $verb   = $column->isChange() ? 'MODIFY COLUMN' : 'ADD COLUMN';
            $colSql = $column->toSql();

            if ($after = $column->getAfter()) {
                $colSql .= " AFTER `{$after}`";
            }

            $actions[] = "{$verb} {$colSql}";
        }

        foreach ($blueprint->getDrops() as $column) {
            $actions[] = "DROP COLUMN `{$column}`";
        }

        foreach ($blueprint->getRenames() as $rename) {
            $actions[] = "RENAME COLUMN `{$rename['from']}` TO `{$rename['to']}`";
        }

        if (empty($actions)) {
            return [];
        }

        return ["ALTER TABLE `{$table}` " . implode(', ', $actions)];
    }

    /**
     * Compile a RENAME TABLE statement.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/rename#rename-table
     *
     * @param string $from Current table name.
     * @param string $to   New table name.
     * @return string
     */
    public function compileRename(string $from, string $to): string
    {
        return "RENAME TABLE `{$from}` TO `{$to}`";
    }

    /**
     * Compile a CREATE MATERIALIZED VIEW statement.
     *
     * The view appends query results to an existing target table ($to) every
     * time new data is inserted into the source. When $populate is true,
     * ClickHouse back-fills the target from all existing source rows — note
     * that this can be slow and blocks concurrent inserts during the process.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/create/view#materialized-view
     *
     * @param string $name        Name of the materialized view.
     * @param string $to          Name of the target table the view writes into.
     * @param string $selectSql   SELECT query defining the view transformation.
     * @param bool   $ifNotExists Emit IF NOT EXISTS (default: false).
     * @param bool   $populate    Emit POPULATE to back-fill from the source (default: false).
     * @return string
     */
    public function compileMaterializedView(
        string $name,
        string $to,
        string $selectSql,
        bool $ifNotExists = false,
        bool $populate = false,
    ): string {
        $ifNotExists = $ifNotExists ? ' IF NOT EXISTS' : '';
        $populate = $populate    ? ' POPULATE'      : '';

        return "CREATE MATERIALIZED VIEW{$ifNotExists} `{$name}` TO `{$to}`{$populate} AS {$selectSql}";
    }

    /**
     * Compile a DROP VIEW statement.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/drop#drop-view
     *
     * @param string $name View name.
     * @return string
     */
    public function compileDropView(string $name): string
    {
        return "DROP VIEW `{$name}`";
    }

    /**
     * Compile a DROP VIEW IF EXISTS statement.
     *
     * Silently succeeds when the view does not exist.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/drop#drop-view
     *
     * @param string $name View name.
     * @return string
     */
    public function compileDropViewIfExists(string $name): string
    {
        return "DROP VIEW IF EXISTS `{$name}`";
    }

    /**
     * Compile a CREATE VIEW statement.
     *
     * A regular (non-materialized) view stores only the SELECT query and
     * evaluates it on every read. No data is stored on disk.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/create/view
     *
     * @param string $name      View name.
     * @param string $selectSql The SELECT query that defines the view.
     * @param bool   $ifNotExists Emit IF NOT EXISTS (default: false).
     * @return string
     */
    public function compileCreateView(string $name, string $selectSql, bool $ifNotExists = false): string
    {
        $ifNotExists = $ifNotExists ? ' IF NOT EXISTS' : '';

        return "CREATE VIEW{$ifNotExists} `{$name}` AS {$selectSql}";
    }

    /**
     * Compile an ATTACH TABLE statement.
     *
     * Registers an existing on-disk table with the server without moving data.
     * The data directory must already be in the correct location.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/attach
     *
     * @param string $table      Table name.
     * @param bool   $ifNotExists Emit IF NOT EXISTS (default: false).
     * @return string
     */
    public function compileAttach(string $table, bool $ifNotExists = false): string
    {
        $ifNotExists = $ifNotExists ? ' IF NOT EXISTS' : '';

        return "ATTACH TABLE{$ifNotExists} `{$table}`";
    }

    /**
     * Compile a DETACH TABLE statement.
     *
     * Removes the table from the server's in-memory state without deleting data
     * from disk. The table can be re-registered later with ATTACH TABLE.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/detach
     *
     * @param string $table    Table name.
     * @param bool   $ifExists Emit IF EXISTS (default: false).
     * @return string
     */
    public function compileDetach(string $table, bool $ifExists = false): string
    {
        $ifExists = $ifExists ? ' IF EXISTS' : '';

        return "DETACH TABLE{$ifExists} `{$table}`";
    }

    /**
     * Compile an ALTER TABLE … FREEZE [PARTITION] statement.
     *
     * Creates a local backup snapshot of a partition (or the entire table when
     * $partition is null). The snapshot is written to the configured backup
     * directory on the server.
     *
     * The partition expression is passed as a raw string — quoting rules depend
     * on the partition key type (e.g. integers need no quotes, strings do).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#freeze-partition
     *
     * @param string      $table      Table name.
     * @param string|null $partition  Partition expression (e.g. '201901', "'2024-01-01'"). Null freezes all partitions.
     * @param string|null $backupName Optional backup name for the WITH NAME clause.
     * @return string
     */
    public function compileFreezePartition(
        string $table,
        ?string $partition = null,
        ?string $backupName = null,
    ): string {
        $sql = "ALTER TABLE `{$table}` FREEZE";

        if ($partition !== null) {
            $sql .= " PARTITION {$partition}";
        }

        if ($backupName !== null) {
            $escaped = str_replace("'", "\\'", $backupName);
            $sql .= " WITH NAME '{$escaped}'";
        }

        return $sql;
    }

    /**
     * Compile an ALTER TABLE … MOVE PARTITION … TO TABLE statement.
     *
     * Moves a partition from one MergeTree table to another on the same server.
     * Both tables must have identical structure and engine configuration.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#move-partition-to-table
     *
     * @param string $table       Source table name.
     * @param string $partition   Partition expression.
     * @param string $targetTable Destination table name.
     * @return string
     */
    public function compileMovePartitionToTable(string $table, string $partition, string $targetTable): string
    {
        return "ALTER TABLE `{$table}` MOVE PARTITION {$partition} TO TABLE `{$targetTable}`";
    }

    /**
     * Compile an ALTER TABLE … MOVE PARTITION … TO DISK statement.
     *
     * Moves a partition to a named disk defined in the ClickHouse storage configuration.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#move-partition-to-disk-volume
     *
     * @param string $table     Table name.
     * @param string $partition Partition expression.
     * @param string $disk      Target disk name as defined in the storage policy.
     * @return string
     */
    public function compileMovePartitionToDisk(string $table, string $partition, string $disk): string
    {
        $escaped = str_replace("'", "\\'", $disk);

        return "ALTER TABLE `{$table}` MOVE PARTITION {$partition} TO DISK '{$escaped}'";
    }

    /**
     * Compile an ALTER TABLE … MOVE PARTITION … TO VOLUME statement.
     *
     * Moves a partition to a named volume defined in the ClickHouse storage policy.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#move-partition-to-disk-volume
     *
     * @param string $table     Table name.
     * @param string $partition Partition expression.
     * @param string $volume    Target volume name as defined in the storage policy.
     * @return string
     */
    public function compileMovePartitionToVolume(string $table, string $partition, string $volume): string
    {
        $escaped = str_replace("'", "\\'", $volume);

        return "ALTER TABLE `{$table}` MOVE PARTITION {$partition} TO VOLUME '{$escaped}'";
    }

    /**
     * Compile a DROP DICTIONARY statement.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/drop#drop-dictionary
     *
     * @param string $name Dictionary name.
     * @return string
     */
    public function compileDropDictionary(string $name): string
    {
        return "DROP DICTIONARY `{$name}`";
    }

    /**
     * Compile a DROP DICTIONARY IF EXISTS statement.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/drop#drop-dictionary
     *
     * @param string $name Dictionary name.
     * @return string
     */
    public function compileDropDictionaryIfExists(string $name): string
    {
        return "DROP DICTIONARY IF EXISTS `{$name}`";
    }

    /**
     * Build the full CREATE TABLE SQL body from a header prefix and a Blueprint.
     *
     * Assembles columns, ENGINE, PARTITION BY, ORDER BY, PRIMARY KEY,
     * SAMPLE BY, TTL, SETTINGS, and COMMENT clauses in the order required
     * by ClickHouse.
     *
     * @param string    $header    Opening clause, e.g. `"CREATE TABLE \`name\`"`.
     * @param Blueprint $blueprint Blueprint describing the table structure.
     * @return string
     */
    private function buildCreateStatement(string $header, Blueprint $blueprint): string
    {
        $columns = array_map(
            static fn(ColumnDefinition $col) => '    ' . $col->toSql(),
            $blueprint->getColumns(),
        );

        $sql = "{$header}\n(\n";
        $sql .= implode(",\n", $columns);
        $sql .= "\n)";

        if ($engine = $blueprint->getEngine()) {
            $sql .= "\nENGINE = " . $engine->toSql();
        }

        if ($partitionBy = $blueprint->getPartitionBy()) {
            $sql .= "\nPARTITION BY {$partitionBy}";
        }

        if (!empty($blueprint->getOrderBy())) {
            $sql .= "\nORDER BY (" . implode(', ', $blueprint->getOrderBy()) . ')';
        }

        if ($primaryKey = $blueprint->getPrimaryKey()) {
            $sql .= "\nPRIMARY KEY {$primaryKey}";
        }

        if ($sampleBy = $blueprint->getSampleBy()) {
            $sql .= "\nSAMPLE BY {$sampleBy}";
        }

        if ($ttl = $blueprint->getTtl()) {
            $sql .= "\nTTL {$ttl}";
        }

        if (!empty($blueprint->getSettings())) {
            $pairs = [];

            foreach ($blueprint->getSettings() as $key => $value) {
                $valueStr = \is_scalar($value) ? (string) $value : '';
                $pairs[] = "{$key} = {$valueStr}";
            }

            $sql .= "\nSETTINGS " . implode(', ', $pairs);
        }

        if ($comment = $blueprint->getComment()) {
            $sql .= "\nCOMMENT '" . str_replace("'", "\\'", $comment) . "'";
        }

        return $sql;
    }
}
