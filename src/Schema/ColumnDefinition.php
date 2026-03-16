<?php

namespace Beeterty\ClickHouse\Schema;

/**
 * Represents a single column definition in a ClickHouse table.
 *
 * Modifiers are applied in ClickHouse's required order:
 *   Nullable → LowCardinality → DEFAULT → COMMENT → CODEC → TTL
 */
class ColumnDefinition
{
    private bool $isNullable        = false;
    private bool $isLowCardinality  = false;
    private bool $hasDefault        = false;
    private mixed $defaultValue     = null;
    private bool $defaultIsRaw      = false;
    private ?string $comment        = null;
    private ?string $codec          = null;
    private ?string $ttl            = null;
    private ?string $after          = null;
    private bool $isChange          = false;

    public function __construct(
        private readonly string $name,
        private readonly string $type,
    ) {}

    // ─── Modifiers ────────────────────────────────────────────────────────────

    /**
     * Wrap the column type in Nullable(T).
     */
    public function nullable(): static
    {
        $this->isNullable = true;

        return $this;
    }

    /**
     * Wrap the column type in LowCardinality(T).
     * Ideal for string columns with low distinct-value counts.
     */
    public function lowCardinality(): static
    {
        $this->isLowCardinality = true;

        return $this;
    }

    /**
     * Set a DEFAULT expression for the column.
     *
     * @param mixed $value Scalar default value (int, float, bool, string literal).
     *                     For SQL expressions/functions use defaultRaw().
     */
    public function default(mixed $value): static
    {
        $this->hasDefault   = true;
        $this->defaultValue = $value;
        $this->defaultIsRaw = false;

        return $this;
    }

    /**
     * Set a raw SQL DEFAULT expression — emitted verbatim without quoting.
     *
     * Use for ClickHouse functions such as now(), generateUUIDv4(), toDate(now()), etc.
     *
     * @param string $expr Raw SQL expression, e.g. 'now()' or 'generateUUIDv4()'.
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
     */
    public function comment(string $text): static
    {
        $this->comment = $text;

        return $this;
    }

    /**
     * Apply a column-level CODEC (e.g. "ZSTD(1)", "Delta, LZ4").
     */
    public function codec(string $codec): static
    {
        $this->codec = $codec;

        return $this;
    }

    /**
     * Set a column-level TTL expression.
     */
    public function ttl(string $expr): static
    {
        $this->ttl = $expr;

        return $this;
    }

    /**
     * Position this column after another column (ALTER TABLE context only).
     *
     * Generates: ADD COLUMN / MODIFY COLUMN ... AFTER `other`.
     */
    public function after(string $column): static
    {
        $this->after = $column;

        return $this;
    }

    /**
     * Mark this column as a modification rather than an addition.
     *
     * In an ALTER TABLE context this generates MODIFY COLUMN instead of ADD COLUMN.
     */
    public function change(): static
    {
        $this->isChange = true;

        return $this;
    }

    // ─── Getters ──────────────────────────────────────────────────────────────

    public function getName(): string
    {
        return $this->name;
    }

    public function getAfter(): ?string
    {
        return $this->after;
    }

    public function isChange(): bool
    {
        return $this->isChange;
    }

    // ─── SQL compilation ──────────────────────────────────────────────────────

    /**
     * Compile the column to its ClickHouse DDL fragment.
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

    // ─── Internal helpers ─────────────────────────────────────────────────────

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
