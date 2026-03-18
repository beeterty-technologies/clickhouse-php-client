<?php

namespace Beeterty\ClickHouse\Schema;

/**
 * Represents a single column definition in a ClickHouse table.
 *
 * Modifiers are applied in ClickHouse's required order:
 *   Nullable → LowCardinality → DEFAULT → COMMENT → CODEC → TTL
 *
 * @see https://clickhouse.com/docs/en/sql-reference/statements/create/table#column-definition
 */
class ColumnDefinition
{
    /**
     * Whether the column type is wrapped in Nullable(T).
     *
     * @var bool
     */
    private bool $isNullable = false;

    /**
     * Whether the column type is wrapped in LowCardinality(T).
     *
     * @var bool
     */
    private bool $isLowCardinality = false;

    /**
     * Whether a DEFAULT expression has been set.
     *
     * @var bool
     */
    private bool $hasDefault = false;

    /**
     * The DEFAULT value or raw SQL expression.
     *
     * @var mixed
     */
    private mixed $defaultValue = null;

    /**
     * When true, $defaultValue is emitted verbatim (raw SQL).
     * When false it is formatted as a quoted literal.
     *
     * @var bool
     */
    private bool $defaultIsRaw = false;

    /**
     * The column COMMENT text, or null if not set.
     *
     * @var string|null
     */
    private ?string $comment = null;

    /**
     * The CODEC expression (e.g. "ZSTD(1)"), or null if not set.
     *
     * @var string|null
     */
    private ?string $codec = null;

    /**
     * The column-level TTL expression, or null if not set.
     *
     * @var string|null
     */
    private ?string $ttl = null;

    /**
     * The AFTER target column for ALTER TABLE positioning, or null if not set.
     *
     * @var string|null
     */
    private ?string $after = null;

    /**
     * When true, produces MODIFY COLUMN instead of ADD COLUMN in ALTER TABLE.
     *
     * @var bool
     */
    private bool $isChange = false;

    /**
     * Create a new column definition.
     *
     * @param string $name Column name.
     * @param string $type Column type, e.g. "String", "Int32", "DateTime".
     */
    public function __construct(
        private readonly string $name,
        private readonly string $type,
    ) {}

    /**
     * Wrap the column type in Nullable(T), allowing NULL values.
     *
     * Note: ClickHouse stores an additional byte per row for nullable columns.
     * Avoid using Nullable on ORDER BY or PRIMARY KEY columns.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/nullable
     *
     * @return static
     */
    public function nullable(): static
    {
        $this->isNullable = true;

        return $this;
    }

    /**
     * Wrap the column type in LowCardinality(T).
     *
     * Converts the column to a dictionary-encoded representation, which
     * significantly reduces storage and improves query performance for
     * columns with a low number of distinct values (e.g. status, country).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/lowcardinality
     *
     * @return static
     */
    public function lowCardinality(): static
    {
        $this->isLowCardinality = true;

        return $this;
    }

    /**
     * Set a scalar DEFAULT value for the column.
     *
     * The value is quoted appropriately: strings are single-quoted, booleans
     * are emitted as 1/0, and NULL becomes the literal NULL keyword. For SQL
     * expressions or ClickHouse functions use {@see defaultRaw()}.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/create/table#default_expr
     *
     * @param mixed $value Scalar default (int, float, bool, string, or null).
     * @return static
     */
    public function default(mixed $value): static
    {
        $this->hasDefault   = true;
        $this->defaultValue = $value;
        $this->defaultIsRaw = false;

        return $this;
    }

    /**
     * Set a raw SQL DEFAULT expression, emitted verbatim without quoting.
     *
     * Use when the default is a ClickHouse function call or expression, e.g.
     * `now()`, `generateUUIDv4()`, `toDate(now())`.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/create/table#default_expr
     *
     * @param string $expr Raw SQL expression.
     * @return static
     */
    public function defaultRaw(string $expr): static
    {
        $this->hasDefault   = true;
        $this->defaultValue = $expr;
        $this->defaultIsRaw = true;

        return $this;
    }

    /**
     * Attach a COMMENT to the column.
     *
     * Stored in the system.columns table and visible in `DESCRIBE TABLE`.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/create/table#column-comment
     *
     * @param string $text Comment text.
     * @return static
     */
    public function comment(string $text): static
    {
        $this->comment = $text;

        return $this;
    }

    /**
     * Apply a column-level compression CODEC.
     *
     * Multiple codecs can be chained, e.g. `"Delta, LZ4"`. Common choices:
     * `ZSTD(1)`, `LZ4`, `Delta`, `DoubleDelta`, `Gorilla`.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/create/table#column-compression-codecs
     *
     * @param string $codec Codec expression, e.g. `"ZSTD(1)"` or `"Delta, LZ4"`.
     * @return static
     */
    public function codec(string $codec): static
    {
        $this->codec = $codec;

        return $this;
    }

    /**
     * Set a column-level TTL expression.
     *
     * The expression must evaluate to a Date or DateTime value. Rows whose
     * TTL has expired are removed or rewritten to the DEFAULT value during
     * a MergeTree merge.
     *
     * @see https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#mergetree-column-ttl
     *
     * @param string $expr TTL expression, e.g. `"created_at + INTERVAL 90 DAY"`.
     * @return static
     */
    public function ttl(string $expr): static
    {
        $this->ttl = $expr;

        return $this;
    }

    /**
     * Position this column immediately after another in ALTER TABLE context.
     *
     * Generates `ADD COLUMN … AFTER \`column\`` or
     * `MODIFY COLUMN … AFTER \`column\``.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/column#alter_add-column
     *
     * @param string $column The column to place this one after.
     * @return static
     */
    public function after(string $column): static
    {
        $this->after = $column;

        return $this;
    }

    /**
     * Mark this column as a modification rather than a new addition.
     *
     * In an ALTER TABLE context this generates `MODIFY COLUMN` instead of
     * `ADD COLUMN`, allowing the type or modifiers of an existing column to
     * be changed.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/column#alter_modify-column
     *
     * @return static
     */
    public function change(): static
    {
        $this->isChange = true;

        return $this;
    }

    /**
     * Return the column name.
     *
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * Return the AFTER target column name, or null if none was set.
     *
     * @return string|null
     */
    public function getAfter(): ?string
    {
        return $this->after;
    }

    /**
     * Return whether this column definition is a modification (MODIFY COLUMN)
     * rather than a new addition (ADD COLUMN).
     *
     * @return bool
     */
    public function isChange(): bool
    {
        return $this->isChange;
    }

    /**
     * Compile the column to its ClickHouse DDL fragment.
     *
     * Produces a string suitable for use inside a CREATE TABLE column list
     * or an ALTER TABLE action, e.g.:
     *
     * ```
     * `created_at` DateTime DEFAULT now() CODEC(ZSTD(1)) TTL created_at + INTERVAL 30 DAY
     * ```
     *
     * @return string
     */
    public function toSql(): string
    {
        $type = $this->type;

        if ($this->isNullable) {
            $type = "Nullable({$type})";
        }

        if ($this->isLowCardinality) {
            $type = "LowCardinality({$type})";
        }

        $sql = "`{$this->name}` {$type}";

        if ($this->hasDefault) {
            $sql .= ' DEFAULT ' . ($this->defaultIsRaw
                ? $this->defaultValue
                : $this->formatDefault($this->defaultValue));
        }

        if ($this->comment !== null) {
            $sql .= " COMMENT '" . str_replace("'", "\\'", $this->comment) . "'";
        }

        if ($this->codec !== null) {
            $sql .= " CODEC({$this->codec})";
        }

        if ($this->ttl !== null) {
            $sql .= " TTL {$this->ttl}";
        }

        return $sql;
    }

    /**
     * Format a scalar default value as a ClickHouse SQL literal.
     *
     * @param mixed $value
     * @return string
     */
    private function formatDefault(mixed $value): string
    {
        return match (true) {
            $value === null   => 'NULL',
            is_bool($value)   => $value ? '1' : '0',
            is_int($value)    => (string) $value,
            is_float($value)  => (string) $value,
            default           => "'" . str_replace("'", "\\'", (string) $value) . "'",
        };
    }
}
