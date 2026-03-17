<?php

namespace Beeterty\ClickHouse\Schema;

class Grammar
{
    /**
     * Compile a CREATE TABLE statement.
     * 
     * @param string $table The name of the table to create.
     * @param Blueprint $blueprint The table blueprint containing columns and options.
     * @return string The compiled SQL statement.
     */
    public function compileCreate(string $table, Blueprint $blueprint): string
    {
        return $this->buildCreateStatement("CREATE TABLE `{$table}`", $blueprint);
    }

    /**
     * Compile a CREATE TABLE IF NOT EXISTS statement.
     * 
     * @param string $table The name of the table to create.
     * @param Blueprint $blueprint The table blueprint containing columns and options.
     * @return string The compiled SQL statement.
     */
    public function compileCreateIfNotExists(string $table, Blueprint $blueprint): string
    {
        return $this->buildCreateStatement("CREATE TABLE IF NOT EXISTS `{$table}`", $blueprint);
    }

    /**
     * Compile a DROP TABLE statement.
     * 
     * @param string $table The name of the table to drop.
     * @return string The compiled SQL statement.
     */
    public function compileDrop(string $table): string
    {
        return "DROP TABLE `{$table}`";
    }

    /**
     * Compile a DROP TABLE IF EXISTS statement.
     * 
     * @param string $table The name of the table to drop.
     * @return string The compiled SQL statement.
     */
    public function compileDropIfExists(string $table): string
    {
        return "DROP TABLE IF EXISTS `{$table}`";
    }

    /**
     * Compile one or more ALTER TABLE statements.
     *
     * ClickHouse supports multiple comma-separated actions in a single ALTER TABLE,
     * so additions, drops, and renames are all batched into one statement.
     * 
     * @param string $table The name of the table to alter.
     * @param Blueprint $blueprint The table blueprint containing column modifications.
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
     * @param string $from The current name of the table.
     * @param string $to The new name of the table.
     * @return string The compiled SQL statement.
     */
    public function compileRename(string $from, string $to): string
    {
        return "RENAME TABLE `{$from}` TO `{$to}`";
    }

    /**
     * Compile a CREATE MATERIALIZED VIEW statement.
     *
     * The view writes its results to an existing target table ($to).
     * When $populate is true, ClickHouse back-fills from the source table.
     * 
     * @param string $name The name of the materialized view to create.
     * @param string $to The name of the target table where the view writes results.
     * @param string $selectSql The SELECT query that defines the view's contents.
     * @param bool $ifNotExists Whether to include IF NOT EXISTS in the statement (default: false).
     * @param bool $populate Whether to include POPULATE in the statement (default: false).
     * @return string The compiled SQL statement.
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
     * @param string $name The name of the view to drop.
     * @return string The compiled SQL statement.
     */
    public function compileDropView(string $name): string
    {
        return "DROP VIEW `{$name}`";
    }

    /**
     * Compile a DROP VIEW IF EXISTS statement.
     * 
     * @param string $name The name of the view to drop.
     * @return string The compiled SQL statement.
     */
    public function compileDropViewIfExists(string $name): string
    {
        return "DROP VIEW IF EXISTS `{$name}`";
    }

    /**
     * Build the full CREATE TABLE statement with columns and options.
     * 
     * @param string $header The initial part of the statement (e.g., "CREATE TABLE `name`").
     * @param Blueprint $blueprint The table blueprint containing columns and options.
     * @return string The complete SQL statement.
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
                $pairs[] = "{$key} = {$value}";
            }

            $sql .= "\nSETTINGS " . implode(', ', $pairs);
        }

        if ($comment = $blueprint->getComment()) {
            $sql .= "\nCOMMENT '" . str_replace("'", "\\'", $comment) . "'";
        }

        return $sql;
    }
}
