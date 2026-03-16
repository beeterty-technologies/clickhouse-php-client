<?php

namespace Beeterty\ClickHouse\Schema;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

/**
 * Defines the structure of a ClickHouse table.
 *
 * Used in both CREATE TABLE and ALTER TABLE contexts:
 * - In create()         : column methods define the table columns.
 * - In table() (ALTER)  : column methods become ADD COLUMN; use dropColumn() / renameColumn() for removals.
 */
class Blueprint
{
    /** @var ColumnDefinition[] Columns to create or add. */
    private array $columns = [];

    /** @var string[] Column names to drop (ALTER context). */
    private array $drops = [];

    /** @var array<array{from: string, to: string}> Column renames (ALTER context). */
    private array $renames = [];

    private ?Engine $engine      = null;
    private array   $orderBy     = [];
    private ?string $partitionBy = null;
    private ?string $primaryKey  = null;
    private ?string $sampleBy   = null;
    private array   $settings    = [];
    private ?string $ttl         = null;
    private ?string $comment     = null;

    // ─── Convenience shorthands ───────────────────────────────────────────────

    /**
     * Add a standard UInt64 primary key column.
     */
    public function id(string $name = 'id'): ColumnDefinition
    {
        return $this->uint64($name);
    }

    /**
     * Add nullable `created_at` and `updated_at` DateTime columns.
     */
    public function timestamps(?string $timezone = null): void
    {
        $this->dateTime('created_at', $timezone)->nullable();
        $this->dateTime('updated_at', $timezone)->nullable();
    }

    /**
     * Add a nullable `deleted_at` DateTime column for soft deletes.
     */
    public function softDeletes(string $column = 'deleted_at', ?string $timezone = null): ColumnDefinition
    {
        return $this->dateTime($column, $timezone)->nullable();
    }

    /**
     * Drop the standard timestamp columns (ALTER TABLE context).
     */
    public function dropTimestamps(): static
    {
        $this->drops[] = 'created_at';
        $this->drops[] = 'updated_at';

        return $this;
    }

    /**
     * Drop the soft-delete column (ALTER TABLE context).
     */
    public function dropSoftDeletes(string $column = 'deleted_at'): static
    {
        $this->drops[] = $column;

        return $this;
    }

    /**
     * Add a column using a raw ClickHouse type definition string.
     *
     * Useful as an escape hatch for types not covered by dedicated methods,
     * e.g. rawColumn('data', 'Tuple(UInt32, String)').
     */
    public function rawColumn(string $name, string $definition): ColumnDefinition
    {
        return $this->addColumn($name, $definition);
    }

    // ─── Integer types ────────────────────────────────────────────────────────

    public function uint8(string $name): ColumnDefinition   { return $this->addColumn($name, 'UInt8');   }
    public function uint16(string $name): ColumnDefinition  { return $this->addColumn($name, 'UInt16');  }
    public function uint32(string $name): ColumnDefinition  { return $this->addColumn($name, 'UInt32');  }
    public function uint64(string $name): ColumnDefinition  { return $this->addColumn($name, 'UInt64');  }
    public function uint128(string $name): ColumnDefinition { return $this->addColumn($name, 'UInt128'); }
    public function uint256(string $name): ColumnDefinition { return $this->addColumn($name, 'UInt256'); }
    public function int8(string $name): ColumnDefinition    { return $this->addColumn($name, 'Int8');    }
    public function int16(string $name): ColumnDefinition   { return $this->addColumn($name, 'Int16');   }
    public function int32(string $name): ColumnDefinition   { return $this->addColumn($name, 'Int32');   }
    public function int64(string $name): ColumnDefinition   { return $this->addColumn($name, 'Int64');   }
    public function int128(string $name): ColumnDefinition  { return $this->addColumn($name, 'Int128');  }
    public function int256(string $name): ColumnDefinition  { return $this->addColumn($name, 'Int256');  }

    // ─── Float types ──────────────────────────────────────────────────────────

    public function float32(string $name): ColumnDefinition { return $this->addColumn($name, 'Float32'); }
    public function float64(string $name): ColumnDefinition { return $this->addColumn($name, 'Float64'); }

    // ─── Decimal ──────────────────────────────────────────────────────────────

    /**
     * @param int $precision Total number of significant digits.
     * @param int $scale     Digits after the decimal point.
     */
    public function decimal(string $name, int $precision, int $scale): ColumnDefinition
    {
        return $this->addColumn($name, "Decimal({$precision}, {$scale})");
    }

    // ─── String types ─────────────────────────────────────────────────────────

    public function string(string $name): ColumnDefinition { return $this->addColumn($name, 'String'); }

    public function fixedString(string $name, int $length): ColumnDefinition
    {
        return $this->addColumn($name, "FixedString({$length})");
    }

    // ─── Date / Time ──────────────────────────────────────────────────────────

    public function date(string $name): ColumnDefinition   { return $this->addColumn($name, 'Date');   }
    public function date32(string $name): ColumnDefinition { return $this->addColumn($name, 'Date32'); }

    public function dateTime(string $name, ?string $timezone = null): ColumnDefinition
    {
        $type = $timezone ? "DateTime('{$timezone}')" : 'DateTime';

        return $this->addColumn($name, $type);
    }

    public function dateTime64(string $name, int $precision = 3, ?string $timezone = null): ColumnDefinition
    {
        $type = $timezone
            ? "DateTime64({$precision}, '{$timezone}')"
            : "DateTime64({$precision})";

        return $this->addColumn($name, $type);
    }

    // ─── Boolean / UUID ───────────────────────────────────────────────────────

    public function boolean(string $name): ColumnDefinition { return $this->addColumn($name, 'Bool'); }
    public function uuid(string $name): ColumnDefinition    { return $this->addColumn($name, 'UUID'); }

    // ─── Enum ─────────────────────────────────────────────────────────────────

    /**
     * Define an Enum8 column.
     *
     * Accepts either:
     *   - ['active' => 1, 'inactive' => 2]  (explicit values)
     *   - ['active', 'inactive']             (auto-indexed from 1)
     *
     * @param array<string|int, string|int> $values
     */
    public function enum8(string $name, array $values): ColumnDefinition
    {
        return $this->addColumn($name, $this->buildEnumType('Enum8', $values));
    }

    /**
     * Define an Enum16 column. Same signature as enum8().
     *
     * @param array<string|int, string|int> $values
     */
    public function enum16(string $name, array $values): ColumnDefinition
    {
        return $this->addColumn($name, $this->buildEnumType('Enum16', $values));
    }

    // ─── IP addresses ─────────────────────────────────────────────────────────

    public function ipv4(string $name): ColumnDefinition { return $this->addColumn($name, 'IPv4'); }
    public function ipv6(string $name): ColumnDefinition { return $this->addColumn($name, 'IPv6'); }

    // ─── JSON ─────────────────────────────────────────────────────────────────

    public function json(string $name): ColumnDefinition { return $this->addColumn($name, 'JSON'); }

    // ─── Complex / nested types ───────────────────────────────────────────────

    /**
     * Array(innerType) — e.g. array('tags', 'String')
     */
    public function array(string $name, string $innerType): ColumnDefinition
    {
        return $this->addColumn($name, "Array({$innerType})");
    }

    /**
     * Map(keyType, valueType) — e.g. map('meta', 'String', 'String')
     */
    public function map(string $name, string $keyType, string $valueType): ColumnDefinition
    {
        return $this->addColumn($name, "Map({$keyType}, {$valueType})");
    }

    /**
     * Tuple of types — e.g. tuple('coords', 'Float64', 'Float64')
     */
    public function tuple(string $name, string ...$types): ColumnDefinition
    {
        return $this->addColumn($name, 'Tuple(' . implode(', ', $types) . ')');
    }

    // ─── Engine & table-level options ─────────────────────────────────────────

    public function engine(Engine $engine): static
    {
        $this->engine = $engine;

        return $this;
    }

    /**
     * @param string|string[] $columns
     */
    public function orderBy(string|array $columns): static
    {
        $this->orderBy = (array) $columns;

        return $this;
    }

    public function partitionBy(string $expr): static
    {
        $this->partitionBy = $expr;

        return $this;
    }

    public function primaryKey(string $expr): static
    {
        $this->primaryKey = $expr;

        return $this;
    }

    public function sampleBy(string $expr): static
    {
        $this->sampleBy = $expr;

        return $this;
    }

    public function settings(array $settings): static
    {
        $this->settings = array_merge($this->settings, $settings);

        return $this;
    }

    public function ttl(string $expr): static
    {
        $this->ttl = $expr;

        return $this;
    }

    public function comment(string $text): static
    {
        $this->comment = $text;

        return $this;
    }

    // ─── ALTER helpers ────────────────────────────────────────────────────────

    /**
     * Mark a column to be dropped in an ALTER TABLE context.
     */
    public function dropColumn(string $column): static
    {
        $this->drops[] = $column;

        return $this;
    }

    /**
     * Rename a column in an ALTER TABLE context.
     */
    public function renameColumn(string $from, string $to): static
    {
        $this->renames[] = ['from' => $from, 'to' => $to];

        return $this;
    }

    // ─── Getters (used by Grammar) ────────────────────────────────────────────

    /** @return ColumnDefinition[] */
    public function getColumns(): array    { return $this->columns;     }
    public function getEngine(): ?Engine   { return $this->engine;      }
    public function getOrderBy(): array    { return $this->orderBy;     }
    public function getPartitionBy(): ?string { return $this->partitionBy; }
    public function getPrimaryKey(): ?string  { return $this->primaryKey;  }
    public function getSampleBy(): ?string    { return $this->sampleBy;    }
    public function getSettings(): array      { return $this->settings;    }
    public function getTtl(): ?string         { return $this->ttl;         }
    public function getComment(): ?string     { return $this->comment;     }
    public function getDrops(): array         { return $this->drops;       }
    public function getRenames(): array       { return $this->renames;     }

    // ─── Internal ─────────────────────────────────────────────────────────────

    private function addColumn(string $name, string $type): ColumnDefinition
    {
        $column          = new ColumnDefinition($name, $type);
        $this->columns[] = $column;

        return $column;
    }

    /**
     * @param array<string|int, string|int> $values
     */
    private function buildEnumType(string $enumType, array $values): string
    {
        $pairs = [];

        foreach ($values as $key => $value) {
            // Associative: ['active' => 1, 'inactive' => 2]
            if (is_string($key)) {
                $pairs[] = "'{$key}' = {$value}";
            } else {
                // Sequential: ['active', 'inactive'] — auto-index from 1
                $pairs[] = "'{$value}' = " . ($key + 1);
            }
        }

        return $enumType . '(' . implode(', ', $pairs) . ')';
    }
}
