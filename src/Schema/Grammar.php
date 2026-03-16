<?php

namespace Beeterty\ClickHouse\Schema;

/**
 * Compiles Blueprint definitions into ClickHouse DDL SQL strings.
 */
class Grammar
{
    /**
     * Compile a CREATE TABLE statement.
     */
    public function compileCreate(string $table, Blueprint $blueprint): string
    {
        return $this->buildCreateStatement("CREATE TABLE `{$table}`", $blueprint);
    }

    /**
     * Compile a CREATE TABLE IF NOT EXISTS statement.
     */
    public function compileCreateIfNotExists(string $table, Blueprint $blueprint): string
    {
        return $this->buildCreateStatement("CREATE TABLE IF NOT EXISTS `{$table}`", $blueprint);
    }

    /**
     * Compile a DROP TABLE statement.
     */
    public function compileDrop(string $table): string
    {
        return "DROP TABLE `{$table}`";
    }

    /**
     * Compile a DROP TABLE IF EXISTS statement.
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
     * @return string[]
     */
    public function compileAlter(string $table, Blueprint $blueprint): array
    {
        $actions = [];

        foreach ($blueprint->getColumns() as $column) {
            $actions[] = 'ADD COLUMN ' . $column->toSql();
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
     */
    public function compileRename(string $from, string $to): string
    {
        return "RENAME TABLE `{$from}` TO `{$to}`";
    }

    // ─── Internal ─────────────────────────────────────────────────────────────

    private function buildCreateStatement(string $header, Blueprint $blueprint): string
    {
        $columns = array_map(
            static fn(ColumnDefinition $col) => '    ' . $col->toSql(),
            $blueprint->getColumns(),
        );

        $sql  = "{$header}\n(\n";
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
