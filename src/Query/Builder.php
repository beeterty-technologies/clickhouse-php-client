<?php

namespace Beeterty\ClickHouse\Query;

use Beeterty\ClickHouse\Client;

class Builder
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
    private ?int $limitValue = null;

    /**
     * The OFFSET value.
     *
     * @var int|null
     */
    private ?int $offsetValue = null;

    /**
     * Whether to apply the FINAL modifier to the FROM clause.
     *
     * @var bool
     */
    private bool $finalModifier = false;

    /**
     * The SAMPLE ratio for fractional row sampling.
     *
     * @var float|null
     */
    private ?float $sampleRatio = null;

    /**
     * Create a new Builder instance.
     *
     * The client is used to dispatch terminal methods (get(), first(), count(), etc.)
     * back to ClickHouse. You typically obtain a pre-scoped builder via Client::table()
     * rather than constructing one directly.
     *
     * Example:
     *   $builder = $client->table('events');
     *
     * @param Client $client The ClickHouse client instance used to execute the query.
     */
    public function __construct(
        private readonly Client $client,
    ) {
        //
    }

    /**
     * Set the table to query.
     *
     * The table name is backtick-quoted in the compiled SQL, so plain identifiers
     * are safe. If you need a subquery or a qualified name (e.g. db.table) use
     * the raw form and call toSql() / query() directly.
     *
     * Example:
     *   $client->table('events')->where('active', 1)->get();
     *   // → SELECT * FROM `events` WHERE `active` = 1
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/from
     *
     * @param string $table The unquoted table name.
     * @return static
     */
    public function table(string $table): static
    {
        $this->fromTable = $table;

        return $this;
    }

    /**
     * Set the columns to SELECT. Each name is backtick-quoted automatically.
     *
     * Expressions that contain parentheses, dots, spaces, backticks, or the
     * wildcard * are passed through unquoted so that things like count(),
     * toDate(col), or table.column work without quoting. For everything else
     * prefer selectRaw().
     *
     * Example:
     *   ->select('id', 'name')                // SELECT `id`, `name`
     *   ->select('user_id', 'count() AS n')   // SELECT `user_id`, count() AS n
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select
     *
     * @param string ...$columns Column names to select.
     * @return static
     */
    public function select(string ...$columns): static
    {
        $this->selectColumns = array_map($this->wrapColumn(...), $columns);

        return $this;
    }

    /**
     * Set a raw SELECT expression without any quoting.
     *
     * Use this when you need aggregate functions, aliases, or any SQL expression
     * that should not be backtick-quoted.
     *
     * Example:
     *   ->selectRaw('count() AS total, avg(score) AS avg_score')
     *   // → SELECT count() AS total, avg(score) AS avg_score
     *
     * Note: calling selectRaw() replaces the entire SELECT list. To append a raw
     * expression to an existing list use addSelectRaw().
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select
     *
     * @param string $expression Raw SQL expression for the SELECT clause.
     * @return static
     */
    public function selectRaw(string $expression): static
    {
        $this->selectColumns = [$expression];

        return $this;
    }

    /**
     * Append one or more columns to the existing SELECT list.
     *
     * If the current SELECT list is the default wildcard (*), the wildcard is
     * replaced rather than extended.
     *
     * Example:
     *   ->select('id')->addSelect('name', 'score')
     *   // → SELECT `id`, `name`, `score`
     *
     *   ->addSelect('id')  // replaces the implicit *
     *   // → SELECT `id`
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select
     *
     * @param string ...$columns Column names to append.
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
     * If the current SELECT list is the default wildcard (*), the wildcard is
     * replaced rather than extended.
     *
     * Example:
     *   ->select('status')->addSelectRaw('count() AS n, avg(score) AS avg')
     *   // → SELECT `status`, count() AS n, avg(score) AS avg
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select
     *
     * @param string $expression Raw SQL expression to append to the SELECT clause.
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
     * Apply the FINAL modifier to the FROM clause.
     *
     * Forces ReplacingMergeTree and CollapsingMergeTree tables to merge duplicate
     * rows at query time, returning a fully deduplicated result set. FINAL reads
     * more data than a normal scan and should only be used when consistency is
     * required — for analytics workloads that tolerate eventual deduplication,
     * prefer relying on background merges instead.
     *
     * Example:
     *   $client->table('users')->final()->where('active', 1)->get();
     *   // → SELECT * FROM `users` FINAL WHERE `active` = 1
     *
     * Note: FINAL is not supported on all engine types. Using it on a plain
     * MergeTree table has no deduplication effect but still forces an extra merge
     * pass, which incurs unnecessary overhead.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/final
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
     * Only available on MergeTree-family tables that have a SAMPLE BY key defined
     * in their table DDL. The ratio must be between 0.0 (exclusive) and 1.0
     * (inclusive); values outside this range are accepted by the builder but will
     * be rejected by ClickHouse at execution time.
     *
     * Example:
     *   ->sample(0.1)   // read ~10% of rows (fast, approximate results)
     *   ->sample(1.0)   // read all rows (equivalent to no SAMPLE clause)
     *
     * Note: SAMPLE returns approximate results. For exact aggregates across the
     * full dataset, omit this clause and let ClickHouse scan all parts.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/sample
     *
     * @param float $ratio Fraction of data to sample, between 0.0 (exclusive) and 1.0 (inclusive).
     * @return static
     */
    public function sample(float $ratio): static
    {
        $this->sampleRatio = $ratio;

        return $this;
    }

    /**
     * Add a PREWHERE condition — ClickHouse's pre-filter applied before WHERE.
     *
     * PREWHERE is evaluated before WHERE and reads only the columns it references,
     * making it significantly more efficient when filtering on a subset of the
     * table's columns (particularly ORDER BY key columns). ClickHouse may
     * automatically move eligible WHERE conditions to PREWHERE; use this method
     * when you want to force the behaviour explicitly.
     *
     * Example:
     *   ->prewhere('event_date', '2024-01-01')      // `event_date` = '2024-01-01'
     *   ->prewhere('event_date', '>=', '2024-01-01') // `event_date` >= '2024-01-01'
     *
     * Note: PREWHERE is a ClickHouse extension and is not supported by standard SQL
     * databases. Multiple calls are combined with AND.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/prewhere
     *
     * @param string $column          Column name to filter on.
     * @param mixed  $operatorOrValue Comparison operator (e.g. '>=') or the value when using the two-argument shorthand.
     * @param mixed  $value           Comparison value when an explicit operator is provided.
     * @return static
     */
    public function prewhere(string $column, mixed $operatorOrValue, mixed $value = null): static
    {
        $this->prewhereConditions[] = $this->buildCondition($column, $operatorOrValue, $value);

        return $this;
    }

    /**
     * Add a raw PREWHERE expression without any escaping or quoting.
     *
     * Use this for ClickHouse functions, date arithmetic, or any expression that
     * cannot be expressed through prewhere().
     *
     * Example:
     *   ->prewhereRaw('toDate(event_time) = today()')
     *   ->prewhereRaw("event_date BETWEEN '2024-01-01' AND '2024-01-31'")
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/prewhere
     *
     * @param string $expression Raw SQL expression for the PREWHERE clause.
     * @return static
     */
    public function prewhereRaw(string $expression): static
    {
        $this->prewhereConditions[] = $expression;

        return $this;
    }

    /**
     * Add a WHERE condition. String and date values are single-quoted; integers,
     * floats, booleans, and nulls are formatted without quotes.
     *
     * When called with two arguments the operator defaults to =. Pass a third
     * argument to use a different comparison operator. Multiple calls are
     * combined with AND.
     *
     * Example:
     *   ->where('status', 'active')       // `status` = 'active'
     *   ->where('age', '>=', 18)          // `age` >= 18
     *   ->where('deleted_at', null)       // `deleted_at` = NULL
     *   ->where('active', true)           // `active` = 1
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/where
     *
     * @param string $column          Column name to filter on.
     * @param mixed  $operatorOrValue Comparison operator (e.g. '!=', '<') or the value when using the two-argument shorthand.
     * @param mixed  $value           Comparison value when an explicit operator is provided.
     * @return static
     */
    public function where(string $column, mixed $operatorOrValue, mixed $value = null): static
    {
        $this->whereConditions[] = $this->buildCondition($column, $operatorOrValue, $value);

        return $this;
    }

    /**
     * Add a raw WHERE expression without any escaping or quoting.
     *
     * Use this for ClickHouse functions, expressions, or conditions that cannot
     * be expressed through where(). Multiple calls are combined with AND.
     *
     * Example:
     *   ->whereRaw('toDate(created_at) = today()')
     *   ->whereRaw('cityHash64(user_id) % 10 = 0')  // 10 % sample by hash
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/where
     *
     * @param string $expression Raw SQL expression for the WHERE clause.
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
     * All values in the array are escaped using the same rules as where():
     * strings are single-quoted, integers and floats are unquoted, booleans
     * become 1/0, and nulls become NULL.
     *
     * Example:
     *   ->whereIn('status', ['active', 'pending'])
     *   // → `status` IN ('active', 'pending')
     *
     *   ->whereIn('id', [1, 2, 3])
     *   // → `id` IN (1, 2, 3)
     *
     * @see https://clickhouse.com/docs/en/sql-reference/operators/in
     *
     * @param string $column Column name to filter on.
     * @param array  $values List of values to match against.
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
     * All values in the array are escaped using the same rules as where():
     * strings are single-quoted, integers and floats are unquoted, booleans
     * become 1/0, and nulls become NULL.
     *
     * Example:
     *   ->whereNotIn('status', ['deleted', 'banned'])
     *   // → `status` NOT IN ('deleted', 'banned')
     *
     * @see https://clickhouse.com/docs/en/sql-reference/operators/in
     *
     * @param string $column Column name to filter on.
     * @param array  $values List of values to exclude.
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
     * Both boundary values are inclusive and escaped using the same rules as
     * where(). The BETWEEN range is always closed (i.e. col >= from AND col <= to).
     *
     * Example:
     *   ->whereBetween('score', 50, 100)
     *   // → `score` BETWEEN 50 AND 100
     *
     *   ->whereBetween('created_at', '2024-01-01', '2024-01-31')
     *   // → `created_at` BETWEEN '2024-01-01' AND '2024-01-31'
     *
     * Note: ClickHouse does not support parameterized BETWEEN — literal values
     * must be passed directly rather than as placeholders.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/operators#operator-between
     *
     * @param string $column Column name to filter on.
     * @param mixed  $from   Lower bound of the range (inclusive).
     * @param mixed  $to     Upper bound of the range (inclusive).
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
     * Example:
     *   ->whereNull('deleted_at')
     *   // → `deleted_at` IS NULL
     *
     * Note: ClickHouse Nullable columns must be declared with Nullable(T) in the
     * table DDL before IS NULL will return meaningful results.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/where
     *
     * @param string $column Column name to check for NULL.
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
     * Example:
     *   ->whereNotNull('published_at')
     *   // → `published_at` IS NOT NULL
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/where
     *
     * @param string $column Column name to check for non-NULL.
     * @return static
     */
    public function whereNotNull(string $column): static
    {
        $this->whereConditions[] = $this->wrapColumn($column) . ' IS NOT NULL';

        return $this;
    }

    /**
     * Set the GROUP BY columns. Each name is backtick-quoted automatically.
     *
     * Multiple columns produce a composite grouping key. Calling groupBy()
     * more than once replaces the previous list rather than appending to it.
     *
     * Example:
     *   ->select('status')->addSelectRaw('count() AS n')->groupBy('status')
     *   // → SELECT `status`, count() AS n … GROUP BY `status`
     *
     *   ->groupBy('region', 'device_type')
     *   // → GROUP BY `region`, `device_type`
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/group-by
     *
     * @param string ...$columns Column names to group by.
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
     * HAVING conditions are not escaped or wrapped, so you must provide a valid
     * SQL expression that references columns or aggregate functions present in
     * the SELECT list. Multiple calls are combined with AND.
     *
     * Example:
     *   ->groupBy('user_id')
     *   ->having('count() > 10')      // users with more than 10 events
     *   ->having('avg(score) >= 80')  // with an average score of 80+
     *
     * Note: ClickHouse does not support parameterized HAVING conditions — literal
     * values must be embedded directly in the expression (e.g. count() > 10,
     * not count() > ?).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/having
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
     * Add an ORDER BY column with an optional sort direction.
     *
     * The column name is backtick-quoted unless it contains parentheses, dots,
     * spaces, or backticks — those pass through unquoted so that expressions
     * like toDate(ts) or t.col work correctly. Multiple calls produce a
     * multi-column ORDER BY.
     *
     * Example:
     *   ->orderBy('created_at')          // `created_at` ASC
     *   ->orderBy('score', 'DESC')       // `score` DESC
     *   ->orderBy('toDate(ts)', 'ASC')   // toDate(ts) ASC
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/order-by
     *
     * @param string $column    Column name or SQL expression to sort by.
     * @param string $direction Sort direction — 'ASC' or 'DESC' (case-insensitive, defaults to 'ASC').
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
     * Shorthand for orderBy($column, 'DESC').
     *
     * Example:
     *   ->orderByDesc('created_at')   // `created_at` DESC
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/order-by
     *
     * @param string $column Column name or SQL expression to sort by descending.
     * @return static
     */
    public function orderByDesc(string $column): static
    {
        return $this->orderBy($column, 'DESC');
    }

    /**
     * Set the maximum number of rows to return.
     *
     * Example:
     *   ->limit(100)
     *   // → LIMIT 100
     *
     * Note: ClickHouse also has a LIMIT BY clause (limit per group) which is
     * not covered by this method. Use selectRaw() / whereRaw() for advanced
     * pagination patterns.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/limit
     *
     * @param int $limit Maximum number of rows to return.
     * @return static
     */
    public function limit(int $limit): static
    {
        $this->limitValue = $limit;

        return $this;
    }

    /**
     * Set the number of rows to skip before returning results.
     *
     * Must be used together with limit() — ClickHouse requires a LIMIT clause
     * when OFFSET is present.
     *
     * Example:
     *   ->limit(20)->offset(40)   // third page of 20 rows
     *   // → LIMIT 20 OFFSET 40
     *
     * Note: deep offsets on large tables are expensive because ClickHouse must
     * still read and discard the skipped rows. For large datasets consider
     * keyset pagination (WHERE id > :last_id ORDER BY id LIMIT n) instead.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/limit
     *
     * @param int $offset Number of rows to skip.
     * @return static
     */
    public function offset(int $offset): static
    {
        $this->offsetValue = $offset;

        return $this;
    }

    /**
     * Execute the query and return a Statement containing all result rows.
     *
     * Appends the configured FORMAT clause and dispatches the compiled SQL to
     * ClickHouse via the client. The default format is JsonEachRow unless
     * overridden at the client level.
     *
     * Example:
     *   $stmt = $client->table('events')->where('active', 1)->get();
     *   foreach ($stmt as $row) { ... }
     *
     * @return Statement
     */
    public function get(): Statement
    {
        return $this->client->query($this->toSql());
    }

    /**
     * Execute the query with LIMIT 1 and return the first row, or null if the
     * result is empty.
     *
     * Example:
     *   $user = $client->table('users')->where('email', $email)->first();
     *   // → ['id' => 1, 'email' => '...', ...]  or null
     *
     * @return array<string, mixed>|null
     */
    public function first(): ?array
    {
        return $this->limit(1)->get()->first();
    }

    /**
     * Return the total number of rows that match the current WHERE / PREWHERE
     * conditions, ignoring any LIMIT, OFFSET, SELECT, or ORDER BY clauses.
     *
     * Example:
     *   $total = $client->table('events')->where('status', 'active')->count();
     *
     * @return int
     */
    public function count(): int
    {
        $clone = clone $this;
        $clone->selectColumns = ['count()'];
        $clone->limitValue    = null;
        $clone->offsetValue   = null;
        $clone->orderByColumns = [];

        return (int) $clone->get()->value();
    }

    /**
     * Execute the query and return the first column of the first row as a scalar.
     *
     * Ideal for single-value aggregate queries.
     *
     * Example:
     *   $max = $client->table('events')->selectRaw('max(score)')->value();
     *   $count = $client->table('users')->selectRaw('count()')->value();
     *
     * @return mixed The scalar value, or null if the result is empty.
     */
    public function value(): mixed
    {
        return $this->limit(1)->get()->value();
    }

    /**
     * Execute the query and return a flat array of values for a single column.
     *
     * Example:
     *   $ids = $client->table('users')->where('active', 1)->pluck('id');
     *   // → [1, 2, 3, ...]
     *
     * @param string $column The column whose values to extract.
     * @return array<int, mixed>
     */
    public function pluck(string $column): array
    {
        return $this->get()->pluck($column);
    }

    /**
     * Process results in chunks using LIMIT + OFFSET pagination.
     *
     * Calls $callback with an array of rows for each chunk. Return false from the
     * callback to stop iteration early. Useful for processing large result sets
     * without loading all rows into memory at once.
     *
     * Example:
     *   $client->table('events')
     *       ->where('status', 'active')
     *       ->orderBy('id')
     *       ->chunk(1000, function (array $rows): void {
     *           foreach ($rows as $row) { ... }
     *       });
     *
     * Note: for very large datasets, deep OFFSET pagination is expensive in
     * ClickHouse because it must read and discard skipped rows. Consider keyset
     * pagination (WHERE id > :last_id) for better performance at scale.
     *
     * @param int      $size     Number of rows per chunk.
     * @param callable $callback Receives an array of rows; return false to stop early.
     * @return void
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
     * Compile the current builder state into a raw SQL string.
     *
     * Clauses are emitted in ClickHouse's required order:
     * SELECT → FROM → FINAL → SAMPLE → PREWHERE → WHERE → GROUP BY → HAVING → ORDER BY → LIMIT → OFFSET.
     *
     * Example:
     *   $sql = $client->table('events')
     *       ->select('user_id')
     *       ->addSelectRaw('count() AS n')
     *       ->where('status', 'active')
     *       ->groupBy('user_id')
     *       ->having('n > 5')
     *       ->orderByDesc('n')
     *       ->limit(10)
     *       ->toSql();
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select
     *
     * @return string The compiled SQL query string without a FORMAT clause.
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
     * Build a single WHERE / PREWHERE condition string.
     *
     * When called with two arguments the operator defaults to =.
     * When called with three arguments, $operatorOrValue is treated as the
     * operator and $value as the right-hand side.
     *
     * @param string $column
     * @param mixed  $operatorOrValue
     * @param mixed  $value
     * @return string
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
     *
     * @param string $column
     * @return string
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
     *
     * - null   → NULL
     * - bool   → 1 / 0
     * - int    → unquoted integer
     * - float  → unquoted float
     * - array  → [val, val, …] (ClickHouse array literal)
     * - string → single-quoted with backslash and single-quote escaped
     *
     * @param mixed $value
     * @return string
     */
    private function escapeValue(mixed $value): string
    {
        return match (true) {
            $value === null    => 'NULL',
            \is_bool($value)  => $value ? '1' : '0',
            \is_int($value)   => (string) $value,
            \is_float($value) => (string) $value,
            \is_array($value) => '[' . implode(', ', array_map($this->escapeValue(...), $value)) . ']',
            default           => "'" . str_replace(["\\", "'"], ["\\\\", "\\'"], (string) $value) . "'",
        };
    }
}
