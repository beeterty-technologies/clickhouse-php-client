<?php

namespace Beeterty\ClickHouse;

/**
 * Fluent SQL query builder for ClickHouse SELECT statements.
 *
 * Obtain an instance via $client->table('table_name'):
 *
 *   $client->table('events')
 *       ->select('user_id', 'count() as total')
 *       ->where('status', 'active')
 *       ->whereBetween('created_at', $from, $to)
 *       ->groupBy('user_id')
 *       ->orderBy('total', 'DESC')
 *       ->limit(100)
 *       ->get();
 */
class QueryBuilder
{
    private string $fromTable = '';

    /** @var string[] */
    private array $selectColumns = ['*'];

    /** @var string[] */
    private array $prewhereConditions = [];

    /** @var string[] */
    private array $whereConditions = [];

    /** @var string[] */
    private array $groupByColumns = [];

    /** @var string[] */
    private array $havingConditions = [];

    /** @var string[] */
    private array $orderByColumns = [];

    private ?int $limitValue  = null;
    private ?int $offsetValue = null;

    public function __construct(
        private readonly Client $client,
    ) {}

    // ─── FROM ─────────────────────────────────────────────────────────────────

    /**
     * Set the table to query.
     */
    public function table(string $table): static
    {
        $this->fromTable = $table;

        return $this;
    }

    // ─── SELECT ───────────────────────────────────────────────────────────────

    /**
     * Set the columns to select. Column names are backtick-quoted automatically.
     *
     *   ->select('id', 'name')  →  SELECT `id`, `name`
     */
    public function select(string ...$columns): static
    {
        $this->selectColumns = array_map($this->wrapColumn(...), $columns);

        return $this;
    }

    /**
     * Set a raw SELECT expression (no quoting applied).
     *
     *   ->selectRaw('count() AS total, avg(score)')
     */
    public function selectRaw(string $expression): static
    {
        $this->selectColumns = [$expression];

        return $this;
    }

    /**
     * Append columns to the existing SELECT list.
     */
    public function addSelect(string ...$columns): static
    {
        if ($this->selectColumns === ['*']) {
            $this->selectColumns = [];
        }

        foreach ($columns as $col) {
            $this->selectColumns[] = $this->wrapColumn($col);
        }

        return $this;
    }

    /**
     * Append a raw expression to the existing SELECT list.
     */
    public function addSelectRaw(string $expression): static
    {
        if ($this->selectColumns === ['*']) {
            $this->selectColumns = [];
        }

        $this->selectColumns[] = $expression;

        return $this;
    }

    // ─── PREWHERE (ClickHouse-specific) ───────────────────────────────────────

    /**
     * Add a PREWHERE condition (ClickHouse-specific pre-filter applied before WHERE).
     *
     * PREWHERE is evaluated before WHERE and reads only the columns it references,
     * making it very efficient for filtering on ORDER BY key columns.
     *
     *   ->prewhere('event_date', '>=', '2024-01-01')
     *   ->prewhere('event_date', $date)   // shorthand for = $date
     */
    public function prewhere(string $column, mixed $operatorOrValue, mixed $value = null): static
    {
        $this->prewhereConditions[] = $this->buildCondition($column, $operatorOrValue, $value);

        return $this;
    }

    /**
     * Add a raw PREWHERE expression.
     */
    public function prewhereRaw(string $expression): static
    {
        $this->prewhereConditions[] = $expression;

        return $this;
    }

    // ─── WHERE ────────────────────────────────────────────────────────────────

    /**
     * Add a WHERE condition.
     *
     *   ->where('status', 'active')        // `status` = 'active'
     *   ->where('age', '>=', 18)           // `age` >= 18
     *   ->where('score', '!=', 0)          // `score` != 0
     */
    public function where(string $column, mixed $operatorOrValue, mixed $value = null): static
    {
        $this->whereConditions[] = $this->buildCondition($column, $operatorOrValue, $value);

        return $this;
    }

    /**
     * Add a raw WHERE expression (no escaping applied).
     *
     *   ->whereRaw('toDate(created_at) = today()')
     */
    public function whereRaw(string $expression): static
    {
        $this->whereConditions[] = $expression;

        return $this;
    }

    /**
     * Add a WHERE … IN (…) condition.
     */
    public function whereIn(string $column, array $values): static
    {
        $escaped = implode(', ', array_map($this->escapeValue(...), $values));
        $this->whereConditions[] = $this->wrapColumn($column) . " IN ({$escaped})";

        return $this;
    }

    /**
     * Add a WHERE … NOT IN (…) condition.
     */
    public function whereNotIn(string $column, array $values): static
    {
        $escaped = implode(', ', array_map($this->escapeValue(...), $values));
        $this->whereConditions[] = $this->wrapColumn($column) . " NOT IN ({$escaped})";

        return $this;
    }

    /**
     * Add a WHERE … BETWEEN … AND … condition.
     */
    public function whereBetween(string $column, mixed $from, mixed $to): static
    {
        $col = $this->wrapColumn($column);
        $this->whereConditions[] = "{$col} BETWEEN "
            . $this->escapeValue($from) . ' AND ' . $this->escapeValue($to);

        return $this;
    }

    /**
     * Add a WHERE … IS NULL condition.
     */
    public function whereNull(string $column): static
    {
        $this->whereConditions[] = $this->wrapColumn($column) . ' IS NULL';

        return $this;
    }

    /**
     * Add a WHERE … IS NOT NULL condition.
     */
    public function whereNotNull(string $column): static
    {
        $this->whereConditions[] = $this->wrapColumn($column) . ' IS NOT NULL';

        return $this;
    }

    // ─── GROUP BY / HAVING ────────────────────────────────────────────────────

    /**
     * Set GROUP BY columns.
     */
    public function groupBy(string ...$columns): static
    {
        $this->groupByColumns = array_map($this->wrapColumn(...), $columns);

        return $this;
    }

    /**
     * Add a HAVING condition (raw expression).
     */
    public function having(string $expression): static
    {
        $this->havingConditions[] = $expression;

        return $this;
    }

    // ─── ORDER BY / LIMIT / OFFSET ────────────────────────────────────────────

    /**
     * Add an ORDER BY clause.
     *
     *   ->orderBy('created_at')         // `created_at` ASC
     *   ->orderBy('score', 'DESC')
     */
    public function orderBy(string $column, string $direction = 'ASC'): static
    {
        $direction = strtoupper($direction) === 'DESC' ? 'DESC' : 'ASC';
        $this->orderByColumns[] = $this->wrapColumn($column) . ' ' . $direction;

        return $this;
    }

    /**
     * Add an ORDER BY … DESC clause.
     */
    public function orderByDesc(string $column): static
    {
        return $this->orderBy($column, 'DESC');
    }

    /**
     * Set the LIMIT.
     */
    public function limit(int $limit): static
    {
        $this->limitValue = $limit;

        return $this;
    }

    /**
     * Set the OFFSET.
     */
    public function offset(int $offset): static
    {
        $this->offsetValue = $offset;

        return $this;
    }

    // ─── Terminal methods ─────────────────────────────────────────────────────

    /**
     * Execute the query and return a Statement with all result rows.
     */
    public function get(): Statement
    {
        return $this->client->query($this->toSql());
    }

    /**
     * Execute the query with LIMIT 1 and return the first row, or null.
     */
    public function first(): ?array
    {
        return $this->limit(1)->get()->first();
    }

    /**
     * Return the total row count for the current query (ignores LIMIT/OFFSET/ORDER BY).
     */
    public function count(): int
    {
        $clone                  = clone $this;
        $clone->selectColumns   = ['count()'];
        $clone->limitValue      = null;
        $clone->offsetValue     = null;
        $clone->orderByColumns  = [];

        return (int) $clone->get()->value();
    }

    /**
     * Execute the query and return the first column of the first row.
     *
     *   $total = $client->table('events')->selectRaw('count()')->value();
     */
    public function value(): mixed
    {
        return $this->limit(1)->get()->value();
    }

    /**
     * Execute the query and return a flat array of values for $column.
     */
    public function pluck(string $column): array
    {
        return $this->get()->pluck($column);
    }

    /**
     * Process results in chunks using LIMIT + OFFSET pagination.
     *
     * Return false from the callback to stop early.
     *
     *   $client->table('events')
     *       ->where('status', 'active')
     *       ->orderBy('id')
     *       ->chunk(1000, function (array $rows) {
     *           foreach ($rows as $row) { ... }
     *       });
     */
    public function chunk(int $size, callable $callback): void
    {
        $offset = 0;

        while (true) {
            $clone = clone $this;
            $rows  = $clone->limit($size)->offset($offset)->get()->rows();

            if (empty($rows)) {
                break;
            }

            if ($callback($rows) === false) {
                break;
            }

            if (count($rows) < $size) {
                break;
            }

            $offset += $size;
        }
    }

    // ─── SQL compilation ──────────────────────────────────────────────────────

    /**
     * Compile the builder state into a raw SQL string.
     */
    public function toSql(): string
    {
        $sql = 'SELECT ' . implode(', ', $this->selectColumns);
        $sql .= " FROM `{$this->fromTable}`";

        if (!empty($this->prewhereConditions)) {
            $sql .= ' PREWHERE ' . implode(' AND ', $this->prewhereConditions);
        }

        if (!empty($this->whereConditions)) {
            $sql .= ' WHERE ' . implode(' AND ', $this->whereConditions);
        }

        if (!empty($this->groupByColumns)) {
            $sql .= ' GROUP BY ' . implode(', ', $this->groupByColumns);
        }

        if (!empty($this->havingConditions)) {
            $sql .= ' HAVING ' . implode(' AND ', $this->havingConditions);
        }

        if (!empty($this->orderByColumns)) {
            $sql .= ' ORDER BY ' . implode(', ', $this->orderByColumns);
        }

        if ($this->limitValue !== null) {
            $sql .= " LIMIT {$this->limitValue}";
        }

        if ($this->offsetValue !== null) {
            $sql .= " OFFSET {$this->offsetValue}";
        }

        return $sql;
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    /**
     * Build a single WHERE/PREWHERE condition string.
     */
    private function buildCondition(string $column, mixed $operatorOrValue, mixed $value): string
    {
        if ($value === null) {
            $operator = '=';
            $value    = $operatorOrValue;
        } else {
            $operator = (string) $operatorOrValue;
        }

        return $this->wrapColumn($column) . " {$operator} " . $this->escapeValue($value);
    }

    /**
     * Backtick-quote a simple column name.
     *
     * Expressions containing parentheses, dots, spaces, backticks, or the
     * wildcard * are returned as-is so that things like count(), toDate(col),
     * or table.column pass through unchanged.
     */
    private function wrapColumn(string $column): string
    {
        if (
            $column === '*'
            || str_contains($column, '(')
            || str_contains($column, '.')
            || str_contains($column, ' ')
            || str_contains($column, '`')
        ) {
            return $column;
        }

        return "`{$column}`";
    }

    /**
     * Escape a PHP value for safe inline inclusion in a SQL string.
     */
    private function escapeValue(mixed $value): string
    {
        return match (true) {
            $value === null   => 'NULL',
            \is_bool($value)  => $value ? '1' : '0',
            \is_int($value)   => (string) $value,
            \is_float($value) => (string) $value,
            \is_array($value) => '[' . implode(', ', array_map($this->escapeValue(...), $value)) . ']',
            default           => "'" . str_replace(["\\", "'"], ["\\\\", "\\'"], (string) $value) . "'",
        };
    }
}
