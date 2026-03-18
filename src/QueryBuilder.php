<?php

namespace Beeterty\ClickHouse;

class QueryBuilder
{
    /** 
     * The table to query.
     *
     * @var string
     */
    private string $fromTable = '';

    /** 
     * The columns to select, as raw SQL expressions. By default, this is ['*'] for SELECT *.
     * 
     * @var string[] 
     */
    private array $selectColumns = ['*'];

    /** 
     * The conditions for the PREWHERE clause.
     * 
     * @var string[]
     */
    private array $prewhereConditions = [];

    /** 
     * The conditions for the WHERE clause.
     * 
     * @var string[]
     */
    private array $whereConditions = [];

    /** 
     * The columns to group by.
     * 
     * @var string[]
     */
    private array $groupByColumns = [];

    /** 
     * The conditions for the HAVING clause.
     * 
     * @var string[]
     */
    private array $havingConditions = [];

    /** 
     * The columns to order by.
     * 
     * @var string[]
     */
    private array $orderByColumns = [];

    /** 
     * The LIMIT value.
     * 
     * @var int|null
     */
    private ?int $limitValue  = null;

    /** 
     * The OFFSET value.
     * 
     * @var int|null
     */
    private ?int $offsetValue = null;

    /** 
     * Whether to use the FINAL modifier.
     * 
     * @var bool
     */
    private bool $finalModifier = false;

    /** 
     * The SAMPLE ratio.
     * 
     * @var float|null
     */
    private ?float $sampleRatio = null;

    /**
     * Create a new QueryBuilder instance.
     *
     * @param Client $client The ClickHouse client instance to execute queries with.
     */
    public function __construct(
        private readonly Client $client,
    ) {
        // 
    }

    /**
     * Set the table to query.
     * 
     * @param string $table
     * @return static
     */
    public function table(string $table): static
    {
        $this->fromTable = $table;

        return $this;
    }

    /**
     * Set the columns to select. Column names are backtick-quoted automatically.
     *
     *   ->select('id', 'name')  →  SELECT `id`, `name`
     * 
     * @param string[] $columns
     * @return static
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
     * 
     * @param string $expression Raw SQL expression for the SELECT clause (e.g. 'count() AS total').
     * @return static
     */
    public function selectRaw(string $expression): static
    {
        $this->selectColumns = [$expression];

        return $this;
    }

    /**
     * Append columns to the existing SELECT list.
     * 
     * @param string[] $columns
     * @return static
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
     * 
     * @param string $expression Raw SQL expression for the SELECT clause (e.g. 'count() AS total').
     * @return static
     */
    public function addSelectRaw(string $expression): static
    {
        if ($this->selectColumns === ['*']) {
            $this->selectColumns = [];
        }

        $this->selectColumns[] = $expression;

        return $this;
    }

    /**
     * Append the FINAL modifier to the FROM clause.
     *
     * Forces `ReplacingMergeTree` and `CollapsingMergeTree` tables to merge
     * duplicate rows at query time, returning a fully deduplicated result set.
     * Has a performance cost — use only when you need guaranteed consistency.
     *
     *   $client->table('users')->final()->where('active', 1)->get();
     *   // → SELECT * FROM `users` FINAL WHERE `active` = 1
     * 
     * @return static
     */
    public function final(): static
    {
        $this->finalModifier = true;

        return $this;
    }

    /**
     * Add a SAMPLE clause for random fractional row sampling.
     *
     * Only available on MergeTree-family tables with a SAMPLE BY key defined.
     * $ratio must be between 0.0 (exclusive) and 1.0 (inclusive).
     *
     *   ->sample(0.1)   // read ~10 % of data
     *   ->sample(1)     // read all data (equivalent to no SAMPLE)
     *
     * @param float $ratio Fraction of data to sample (e.g. 0.1 for 10 %).
     * @return static
     */
    public function sample(float $ratio): static
    {
        $this->sampleRatio = $ratio;

        return $this;
    }

    /**
     * Add a PREWHERE condition (ClickHouse-specific pre-filter applied before WHERE).
     *
     * PREWHERE is evaluated before WHERE and reads only the columns it references,
     * making it very efficient for filtering on ORDER BY key columns.
     *
     *   ->prewhere('event_date', '>=', '2024-01-01')
     *   ->prewhere('event_date', $date)   // shorthand for = $date
     * 
     * @param string $column
     * @param mixed $operatorOrValue
     * @param mixed|null $value
     * @return static
     */
    public function prewhere(string $column, mixed $operatorOrValue, mixed $value = null): static
    {
        $this->prewhereConditions[] = $this->buildCondition($column, $operatorOrValue, $value);

        return $this;
    }

    /**
     * Add a raw PREWHERE expression.
     * 
     * @param string $expression Raw SQL expression for the PREWHERE clause (e.g. 'toDate(event_date) = today()').
     * @return static
     */
    public function prewhereRaw(string $expression): static
    {
        $this->prewhereConditions[] = $expression;

        return $this;
    }

    /**
     * Add a WHERE condition.
     *
     *   ->where('status', 'active')        // `status` = 'active'
     *   ->where('age', '>=', 18)           // `age` >= 18
     *   ->where('score', '!=', 0)          // `score` != 0
     * 
     * @param string $column
     * @param mixed $operatorOrValue
     * @param mixed|null $value
     * @return static
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
     * 
     * @param string $expression Raw SQL expression for the WHERE clause (e.g. 'toDate(created_at) = today()').
     * @return static
     */
    public function whereRaw(string $expression): static
    {
        $this->whereConditions[] = $expression;

        return $this;
    }

    /**
     * Add a WHERE … IN (…) condition.
     * 
     * @param string $column
     * @param array $values
     * @return static
     */
    public function whereIn(string $column, array $values): static
    {
        $escaped = implode(', ', array_map($this->escapeValue(...), $values));
        $this->whereConditions[] = $this->wrapColumn($column) . " IN ({$escaped})";

        return $this;
    }

    /**
     * Add a WHERE … NOT IN (…) condition.
     * 
     * @param string $column
     * @param array $values
     * @return static
     */
    public function whereNotIn(string $column, array $values): static
    {
        $escaped = implode(', ', array_map($this->escapeValue(...), $values));
        $this->whereConditions[] = $this->wrapColumn($column) . " NOT IN ({$escaped})";

        return $this;
    }

    /**
     * Add a WHERE … BETWEEN … AND … condition.
     * 
     * $from and $to can be any value type (string, number, boolean, null, or array) and will be escaped appropriately.
     * Example:
     *   ->whereBetween('created_at', '2024-01-01', '2024-01-31')
     * 
     * Note: ClickHouse does not support parameterized BETWEEN conditions, so you must include any literal values directly in the method arguments (e.g. whereBetween('created_at', '2024-01-01', '2024-01-31'), not whereBetween('created_at', ?, ?)).
     * 
     * @param string $column
     * @param mixed $from Starting value for the BETWEEN condition (inclusive).
     * @param mixed $to Ending value for the BETWEEN condition (inclusive).
     * @return static
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
     * 
     * @param string $column
     * @return static
     */
    public function whereNull(string $column): static
    {
        $this->whereConditions[] = $this->wrapColumn($column) . ' IS NULL';

        return $this;
    }

    /**
     * Add a WHERE … IS NOT NULL condition.
     * 
     * @param string $column
     * @return static
     */
    public function whereNotNull(string $column): static
    {
        $this->whereConditions[] = $this->wrapColumn($column) . ' IS NOT NULL';

        return $this;
    }

    /**
     * Set GROUP BY columns.
     * 
     * @param string[] $columns
     * @return static
     */
    public function groupBy(string ...$columns): static
    {
        $this->groupByColumns = array_map($this->wrapColumn(...), $columns);

        return $this;
    }

    /**
     * Add a HAVING condition (raw expression).
     * 
     * HAVING conditions are not escaped or wrapped, so you must provide a valid SQL expression using the selected columns and/or aggregate functions.
     * 
     * Example:
     *   ->groupBy('user_id')
     *   ->having('count() > 10')    // users with more than 10 events
     * 
     * Note: ClickHouse does not support parameterized HAVING conditions, so you must include any literal values directly in the expression (e.g. count() > 10, not count() > ?).
     * 
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/#having
     * 
     * @param string $expression Raw SQL expression for the HAVING clause (e.g. 'count() > 10').
     * @return static
     */
    public function having(string $expression): static
    {
        $this->havingConditions[] = $expression;

        return $this;
    }

    /**
     * Add an ORDER BY clause.
     *
     *   ->orderBy('created_at')         // `created_at` ASC
     *   ->orderBy('score', 'DESC')
     * 
     * $direction is case-insensitive and defaults to 'ASC' for any value other than 'DESC'.
     * 
     * @param string $column
     * @param string $direction 'ASC' or 'DESC' (case-insensitive, defaults to 'ASC' for any value other than 'DESC')
     * @return static
     */
    public function orderBy(string $column, string $direction = 'ASC'): static
    {
        $direction = strtoupper($direction) === 'DESC' ? 'DESC' : 'ASC';
        $this->orderByColumns[] = $this->wrapColumn($column) . ' ' . $direction;

        return $this;
    }

    /**
     * Add an ORDER BY … DESC clause.
     * 
     * @param string $column
     * @return static
     */
    public function orderByDesc(string $column): static
    {
        return $this->orderBy($column, 'DESC');
    }

    /**
     * Set the LIMIT.
     * 
     * @param int $limit
     * @return static
     */
    public function limit(int $limit): static
    {
        $this->limitValue = $limit;

        return $this;
    }

    /**
     * Set the OFFSET.
     * 
     * @param int $offset
     * @return static
     */
    public function offset(int $offset): static
    {
        $this->offsetValue = $offset;

        return $this;
    }

    /**
     * Execute the query and return a Statement with all result rows.
     * 
     * @return Statement
     */
    public function get(): Statement
    {
        return $this->client->query($this->toSql());
    }

    /**
     * Execute the query with LIMIT 1 and return the first row, or null.
     * 
     * @return array|null
     */
    public function first(): ?array
    {
        return $this->limit(1)->get()->first();
    }

    /**
     * Return the total row count for the current query (ignores LIMIT/OFFSET/ORDER BY).
     * 
     * @return int
     */
    public function count(): int
    {
        $clone = clone $this;
        $clone->selectColumns = ['count()'];
        $clone->limitValue = null;
        $clone->offsetValue = null;
        $clone->orderByColumns = [];

        return (int) $clone->get()->value();
    }

    /**
     * Execute the query and return the first column of the first row.
     *
     *   $total = $client->table('events')->selectRaw('count()')->value();
     * 
     * @return mixed
     */
    public function value(): mixed
    {
        return $this->limit(1)->get()->value();
    }

    /**
     * Execute the query and return a flat array of values for $column.
     * 
     *  $ids = $client->table('users')->where('active', 1)->pluck('id');
     * 
     * @param string $column
     * @return array
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
     * 
     * Note: for large datasets, consider using server-side cursors or keyset pagination instead of this method to avoid performance issues with deep OFFSETs.
     * 
     * @param int $size
     * @param callable $callback
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

    /**
     * Compile the builder state into a raw SQL string.
     */
    public function toSql(): string
    {
        $sql = 'SELECT ' . implode(', ', $this->selectColumns);
        $sql .= " FROM `{$this->fromTable}`";

        if ($this->finalModifier) {
            $sql .= ' FINAL';
        }

        if ($this->sampleRatio !== null) {
            $sql .= ' SAMPLE ' . $this->sampleRatio;
        }

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
            \is_bool($value) => $value ? '1' : '0',
            \is_int($value) => (string) $value,
            \is_float($value) => (string) $value,
            \is_array($value) => '[' . implode(', ', array_map($this->escapeValue(...), $value)) . ']',
            default           => "'" . str_replace(["\\", "'"], ["\\\\", "\\'"], (string) $value) . "'",
        };
    }
}
