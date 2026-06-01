<?php

namespace Beeterty\ClickHouse\Query;

/**
 * Represents the ON condition set for a JOIN clause.
 *
 * An instance of this class is passed to the closure form of Builder::join(),
 * leftJoin(), and related methods when multiple ON conditions are required or
 * when a non-equality operator is needed.
 *
 * Example:
 *   ->join('orders', function (JoinClause $join): void {
 *       $join->on('users.id', '=', 'orders.user_id')
 *            ->on('users.tenant_id', '=', 'orders.tenant_id');
 *   })
 *   // → INNER JOIN `orders`
 *   //     ON users.id = orders.user_id
 *   //    AND users.tenant_id = orders.tenant_id
 *
 * @see https://clickhouse.com/docs/en/sql-reference/statements/select/join
 */
class JoinClause
{
    /**
     * The raw ON condition expressions, combined with AND on compile.
     *
     * @var string[]
     */
    private array $conditions = [];

    /**
     * Add an ON condition to this join.
     *
     * Column references are not quoted automatically — use qualified names
     * (e.g. 'users.id') or backtick-quoted identifiers as needed.
     *
     * Example:
     *   $join->on('users.id', '=', 'orders.user_id')
     *        ->on('users.tenant_id', '=', 'orders.tenant_id')
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/select/join
     *
     * @param string $first    Left-hand column reference (e.g. 'users.id').
     * @param string $operator Comparison operator (e.g. '=', '!=', '<', '>').
     * @param string $second   Right-hand column reference (e.g. 'orders.user_id').
     * @return static
     */
    public function on(string $first, string $operator, string $second): static
    {
        $this->conditions[] = "{$first} {$operator} {$second}";

        return $this;
    }

    /**
     * Compile all ON conditions into a single SQL fragment joined with AND.
     *
     * @return string
     */
    public function compile(): string
    {
        return implode(' AND ', $this->conditions);
    }
}
