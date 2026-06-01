<?php

namespace Beeterty\ClickHouse\Tests\Unit;

use Beeterty\ClickHouse\Client;
use Beeterty\ClickHouse\Query\Builder;
use Beeterty\ClickHouse\Query\JoinClause;
use PHPUnit\Framework\TestCase;

class QueryBuilderTest extends TestCase
{
    private function builder(): Builder
    {
        return new Builder($this->createMock(Client::class));
    }

    // ─── SELECT ───────────────────────────────────────────────────────────────

    public function test_default_select_is_wildcard(): void
    {
        $sql = $this->builder()->table('events')->toSql();
        $this->assertSame('SELECT * FROM `events`', $sql);
    }

    public function test_select_wraps_column_names(): void
    {
        $sql = $this->builder()->table('events')->select('id', 'name')->toSql();
        $this->assertSame('SELECT `id`, `name` FROM `events`', $sql);
    }

    public function test_select_raw_passes_expression_unchanged(): void
    {
        $sql = $this->builder()->table('events')->selectRaw('count() AS total')->toSql();
        $this->assertSame('SELECT count() AS total FROM `events`', $sql);
    }

    public function test_add_select_appends_columns(): void
    {
        $sql = $this->builder()->table('t')->select('id')->addSelect('name')->toSql();
        $this->assertSame('SELECT `id`, `name` FROM `t`', $sql);
    }

    public function test_add_select_replaces_wildcard(): void
    {
        $sql = $this->builder()->table('t')->addSelect('id')->toSql();
        $this->assertSame('SELECT `id` FROM `t`', $sql);
    }

    public function test_add_select_raw_appends_expression(): void
    {
        $sql = $this->builder()->table('t')->select('id')->addSelectRaw('count() AS n')->toSql();
        $this->assertSame('SELECT `id`, count() AS n FROM `t`', $sql);
    }

    // ─── WHERE ────────────────────────────────────────────────────────────────

    public function test_where_equality_shorthand(): void
    {
        $sql = $this->builder()->table('t')->where('status', 'active')->toSql();
        $this->assertSame("SELECT * FROM `t` WHERE `status` = 'active'", $sql);
    }

    public function test_where_with_explicit_operator(): void
    {
        $sql = $this->builder()->table('t')->where('age', '>=', 18)->toSql();
        $this->assertSame('SELECT * FROM `t` WHERE `age` >= 18', $sql);
    }

    public function test_where_integer_is_not_quoted(): void
    {
        $sql = $this->builder()->table('t')->where('id', 42)->toSql();
        $this->assertSame('SELECT * FROM `t` WHERE `id` = 42', $sql);
    }

    public function test_where_null_value_becomes_null(): void
    {
        $sql = $this->builder()->table('t')->where('deleted_at', null)->toSql();
        $this->assertSame('SELECT * FROM `t` WHERE `deleted_at` = NULL', $sql);
    }

    public function test_where_bool_true_becomes_1(): void
    {
        $sql = $this->builder()->table('t')->where('active', true)->toSql();
        $this->assertSame('SELECT * FROM `t` WHERE `active` = 1', $sql);
    }

    public function test_where_bool_false_becomes_0(): void
    {
        $sql = $this->builder()->table('t')->where('active', false)->toSql();
        $this->assertSame('SELECT * FROM `t` WHERE `active` = 0', $sql);
    }

    public function test_multiple_wheres_joined_with_and(): void
    {
        $sql = $this->builder()->table('t')
            ->where('status', 'active')
            ->where('age', '>=', 18)
            ->toSql();
        $this->assertSame("SELECT * FROM `t` WHERE `status` = 'active' AND `age` >= 18", $sql);
    }

    public function test_where_raw_passes_through_unchanged(): void
    {
        $sql = $this->builder()->table('t')->whereRaw('toDate(created_at) = today()')->toSql();
        $this->assertSame('SELECT * FROM `t` WHERE toDate(created_at) = today()', $sql);
    }

    public function test_where_in(): void
    {
        $sql = $this->builder()->table('t')->whereIn('status', ['active', 'pending'])->toSql();
        $this->assertSame("SELECT * FROM `t` WHERE `status` IN ('active', 'pending')", $sql);
    }

    public function test_where_not_in(): void
    {
        $sql = $this->builder()->table('t')->whereNotIn('id', [1, 2, 3])->toSql();
        $this->assertSame('SELECT * FROM `t` WHERE `id` NOT IN (1, 2, 3)', $sql);
    }

    public function test_where_between(): void
    {
        $sql = $this->builder()->table('t')->whereBetween('score', 10, 100)->toSql();
        $this->assertSame('SELECT * FROM `t` WHERE `score` BETWEEN 10 AND 100', $sql);
    }

    public function test_where_null(): void
    {
        $sql = $this->builder()->table('t')->whereNull('deleted_at')->toSql();
        $this->assertSame('SELECT * FROM `t` WHERE `deleted_at` IS NULL', $sql);
    }

    public function test_where_not_null(): void
    {
        $sql = $this->builder()->table('t')->whereNotNull('published_at')->toSql();
        $this->assertSame('SELECT * FROM `t` WHERE `published_at` IS NOT NULL', $sql);
    }

    // ─── PREWHERE ─────────────────────────────────────────────────────────────

    public function test_prewhere_appears_before_where(): void
    {
        $sql = $this->builder()->table('t')
            ->prewhere('event_date', '2024-01-01')
            ->where('user_id', 42)
            ->toSql();
        $this->assertSame(
            "SELECT * FROM `t` PREWHERE `event_date` = '2024-01-01' WHERE `user_id` = 42",
            $sql
        );
    }

    public function test_prewhere_raw(): void
    {
        $sql = $this->builder()->table('t')->prewhereRaw('event_date >= today()')->toSql();
        $this->assertSame('SELECT * FROM `t` PREWHERE event_date >= today()', $sql);
    }

    // ─── GROUP BY / HAVING ────────────────────────────────────────────────────

    public function test_group_by(): void
    {
        $sql = $this->builder()->table('t')->selectRaw('status, count()')->groupBy('status')->toSql();
        $this->assertSame('SELECT status, count() FROM `t` GROUP BY `status`', $sql);
    }

    public function test_group_by_multiple_columns(): void
    {
        $sql = $this->builder()->table('t')->groupBy('a', 'b')->toSql();
        $this->assertSame('SELECT * FROM `t` GROUP BY `a`, `b`', $sql);
    }

    public function test_having(): void
    {
        $sql = $this->builder()->table('t')
            ->selectRaw('status, count() AS n')
            ->groupBy('status')
            ->having('n > 10')
            ->toSql();
        $this->assertSame(
            'SELECT status, count() AS n FROM `t` GROUP BY `status` HAVING n > 10',
            $sql
        );
    }

    // ─── ORDER BY ─────────────────────────────────────────────────────────────

    public function test_order_by_defaults_to_asc(): void
    {
        $sql = $this->builder()->table('t')->orderBy('created_at')->toSql();
        $this->assertSame('SELECT * FROM `t` ORDER BY `created_at` ASC', $sql);
    }

    public function test_order_by_desc(): void
    {
        $sql = $this->builder()->table('t')->orderByDesc('score')->toSql();
        $this->assertSame('SELECT * FROM `t` ORDER BY `score` DESC', $sql);
    }

    public function test_order_by_multiple_columns(): void
    {
        $sql = $this->builder()->table('t')
            ->orderByDesc('score')
            ->orderBy('name')
            ->toSql();
        $this->assertSame('SELECT * FROM `t` ORDER BY `score` DESC, `name` ASC', $sql);
    }

    // ─── LIMIT / OFFSET ───────────────────────────────────────────────────────

    public function test_limit(): void
    {
        $sql = $this->builder()->table('t')->limit(10)->toSql();
        $this->assertSame('SELECT * FROM `t` LIMIT 10', $sql);
    }

    public function test_offset(): void
    {
        $sql = $this->builder()->table('t')->limit(10)->offset(20)->toSql();
        $this->assertSame('SELECT * FROM `t` LIMIT 10 OFFSET 20', $sql);
    }

    // ─── Full query ───────────────────────────────────────────────────────────

    public function test_full_query_compiles_correctly(): void
    {
        $sql = $this->builder()
            ->table('events')
            ->select('user_id')
            ->addSelectRaw('count() AS total')
            ->prewhere('event_date', '>=', '2024-01-01')
            ->where('status', 'active')
            ->whereNotNull('published_at')
            ->groupBy('user_id')
            ->having('total > 5')
            ->orderByDesc('total')
            ->limit(50)
            ->offset(100)
            ->toSql();

        $this->assertSame(
            "SELECT `user_id`, count() AS total FROM `events`"
            . " PREWHERE `event_date` >= '2024-01-01'"
            . " WHERE `status` = 'active' AND `published_at` IS NOT NULL"
            . " GROUP BY `user_id`"
            . " HAVING total > 5"
            . " ORDER BY `total` DESC"
            . " LIMIT 50 OFFSET 100",
            $sql
        );
    }

    // ─── wrapColumn edge cases ────────────────────────────────────────────────

    public function test_wildcard_is_not_quoted(): void
    {
        $sql = $this->builder()->table('t')->select('*')->toSql();
        $this->assertSame('SELECT * FROM `t`', $sql);
    }

    public function test_expression_with_parens_is_not_quoted(): void
    {
        $sql = $this->builder()->table('t')->orderBy('toDate(ts)')->toSql();
        $this->assertSame('SELECT * FROM `t` ORDER BY toDate(ts) ASC', $sql);
    }

    public function test_dotted_column_is_not_quoted(): void
    {
        $sql = $this->builder()->table('t')->orderBy('t.id')->toSql();
        $this->assertSame('SELECT * FROM `t` ORDER BY t.id ASC', $sql);
    }

    // ─── FINAL / SAMPLE ───────────────────────────────────────────────────────

    public function test_final_modifier(): void
    {
        $sql = $this->builder()->table('users')->final()->toSql();
        $this->assertSame('SELECT * FROM `users` FINAL', $sql);
    }

    public function test_sample_clause(): void
    {
        $sql = $this->builder()->table('events')->sample(0.1)->toSql();
        $this->assertSame('SELECT * FROM `events` SAMPLE 0.1', $sql);
    }

    // ─── JOIN ─────────────────────────────────────────────────────────────────

    public function test_join_three_arg_shorthand(): void
    {
        $sql = $this->builder()->table('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `users` INNER JOIN `orders` ON users.id = orders.user_id',
            $sql
        );
    }

    public function test_join_four_arg_explicit_operator(): void
    {
        $sql = $this->builder()->table('users')
            ->join('orders', 'users.id', '=', 'orders.user_id')
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `users` INNER JOIN `orders` ON users.id = orders.user_id',
            $sql
        );
    }

    public function test_inner_join_is_alias_for_join(): void
    {
        $a = $this->builder()->table('users')->join('orders', 'users.id', 'orders.user_id')->toSql();
        $b = $this->builder()->table('users')->innerJoin('orders', 'users.id', 'orders.user_id')->toSql();
        $this->assertSame($a, $b);
    }

    public function test_left_join(): void
    {
        $sql = $this->builder()->table('users')
            ->leftJoin('profiles', 'users.id', 'profiles.user_id')
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `users` LEFT JOIN `profiles` ON users.id = profiles.user_id',
            $sql
        );
    }

    public function test_right_join(): void
    {
        $sql = $this->builder()->table('users')
            ->rightJoin('events', 'users.id', 'events.user_id')
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `users` RIGHT JOIN `events` ON users.id = events.user_id',
            $sql
        );
    }

    public function test_full_join(): void
    {
        $sql = $this->builder()->table('a')
            ->fullJoin('b', 'a.id', 'b.id')
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `a` FULL JOIN `b` ON a.id = b.id',
            $sql
        );
    }

    public function test_cross_join(): void
    {
        $sql = $this->builder()->table('facts')->crossJoin('dimensions')->toSql();
        $this->assertSame('SELECT * FROM `facts` CROSS JOIN `dimensions`', $sql);
    }

    public function test_join_any_strictness(): void
    {
        $sql = $this->builder()->table('users')
            ->join('orders', 'users.id', 'orders.user_id', strictness: 'ANY')
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `users` ANY INNER JOIN `orders` ON users.id = orders.user_id',
            $sql
        );
    }

    public function test_join_all_strictness_is_omitted(): void
    {
        $sql = $this->builder()->table('users')
            ->join('orders', 'users.id', 'orders.user_id', strictness: 'ALL')
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `users` INNER JOIN `orders` ON users.id = orders.user_id',
            $sql
        );
    }

    public function test_join_semi_strictness(): void
    {
        $sql = $this->builder()->table('users')
            ->leftJoin('orders', 'users.id', 'orders.user_id', strictness: 'SEMI')
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `users` SEMI LEFT JOIN `orders` ON users.id = orders.user_id',
            $sql
        );
    }

    public function test_join_asof_strictness(): void
    {
        $sql = $this->builder()->table('prices')
            ->join('ticks', 'prices.symbol', 'ticks.symbol', strictness: 'ASOF')
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `prices` ASOF INNER JOIN `ticks` ON prices.symbol = ticks.symbol',
            $sql
        );
    }

    public function test_join_closure_multiple_on_conditions(): void
    {
        $sql = $this->builder()->table('users')
            ->join('orders', function (JoinClause $join): void {
                $join->on('users.id', '=', 'orders.user_id')
                     ->on('users.tenant_id', '=', 'orders.tenant_id');
            })
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `users` INNER JOIN `orders`'
            . ' ON users.id = orders.user_id AND users.tenant_id = orders.tenant_id',
            $sql
        );
    }

    public function test_multiple_joins_are_chained(): void
    {
        $sql = $this->builder()->table('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->leftJoin('profiles', 'users.id', 'profiles.user_id')
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `users`'
            . ' INNER JOIN `orders` ON users.id = orders.user_id'
            . ' LEFT JOIN `profiles` ON users.id = profiles.user_id',
            $sql
        );
    }

    public function test_join_qualified_table_name_not_re_quoted(): void
    {
        $sql = $this->builder()->table('users')
            ->join('db.orders', 'users.id', 'db.orders.user_id')
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `users` INNER JOIN db.orders ON users.id = db.orders.user_id',
            $sql
        );
    }

    // ─── ARRAY JOIN ───────────────────────────────────────────────────────────

    public function test_array_join_single_column(): void
    {
        $sql = $this->builder()->table('events')->arrayJoin('tags')->toSql();
        $this->assertSame('SELECT * FROM `events` ARRAY JOIN `tags`', $sql);
    }

    public function test_array_join_multiple_columns(): void
    {
        $sql = $this->builder()->table('events')->arrayJoin('tags', 'scores')->toSql();
        $this->assertSame('SELECT * FROM `events` ARRAY JOIN `tags`, `scores`', $sql);
    }

    public function test_left_array_join(): void
    {
        $sql = $this->builder()->table('events')->leftArrayJoin('tags')->toSql();
        $this->assertSame('SELECT * FROM `events` LEFT ARRAY JOIN `tags`', $sql);
    }

    public function test_array_join_appears_after_regular_joins(): void
    {
        $sql = $this->builder()->table('events')
            ->join('users', 'events.user_id', 'users.id')
            ->arrayJoin('tags')
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `events`'
            . ' INNER JOIN `users` ON events.user_id = users.id'
            . ' ARRAY JOIN `tags`',
            $sql
        );
    }

    // ─── WITH / CTEs ──────────────────────────────────────────────────────────

    public function test_with_raw_string(): void
    {
        $sql = $this->builder()
            ->with('totals', 'SELECT user_id, count() AS n FROM events GROUP BY user_id')
            ->table('totals')
            ->where('n', '>', 5)
            ->toSql();
        $this->assertSame(
            'WITH totals AS (SELECT user_id, count() AS n FROM events GROUP BY user_id)'
            . " SELECT * FROM `totals` WHERE `n` > 5",
            $sql
        );
    }

    public function test_with_builder_instance(): void
    {
        $sub = $this->builder()->table('events')->select('user_id')->where('active', 1);

        $sql = $this->builder()
            ->with('active_users', $sub)
            ->table('active_users')
            ->toSql();
        $this->assertSame(
            "WITH active_users AS (SELECT `user_id` FROM `events` WHERE `active` = 1)"
            . ' SELECT * FROM `active_users`',
            $sql
        );
    }

    public function test_multiple_withs_are_comma_separated(): void
    {
        $sql = $this->builder()
            ->with('a', 'SELECT 1')
            ->with('b', 'SELECT 2')
            ->table('a')
            ->toSql();
        $this->assertSame(
            'WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM `a`',
            $sql
        );
    }

    // ─── UNION ────────────────────────────────────────────────────────────────

    public function test_union_all_with_builder(): void
    {
        $a = $this->builder()->table('events_2023')->select('id', 'name');
        $b = $this->builder()->table('events_2024')->select('id', 'name');

        $sql = $a->unionAll($b)->toSql();
        $this->assertSame(
            'SELECT `id`, `name` FROM `events_2023`'
            . ' UNION ALL SELECT `id`, `name` FROM `events_2024`',
            $sql
        );
    }

    public function test_union_distinct_with_builder(): void
    {
        $a = $this->builder()->table('a')->select('id');
        $b = $this->builder()->table('b')->select('id');

        $sql = $a->unionDistinct($b)->toSql();
        $this->assertSame(
            'SELECT `id` FROM `a` UNION DISTINCT SELECT `id` FROM `b`',
            $sql
        );
    }

    public function test_union_all_with_raw_string(): void
    {
        $sql = $this->builder()->table('a')->select('id')
            ->unionAll('SELECT `id` FROM `b`')
            ->toSql();
        $this->assertSame(
            'SELECT `id` FROM `a` UNION ALL SELECT `id` FROM `b`',
            $sql
        );
    }

    public function test_multiple_unions_are_chained(): void
    {
        $a = $this->builder()->table('a')->select('id');
        $b = $this->builder()->table('b')->select('id');
        $c = $this->builder()->table('c')->select('id');

        $sql = $a->unionAll($b)->unionAll($c)->toSql();
        $this->assertSame(
            'SELECT `id` FROM `a`'
            . ' UNION ALL SELECT `id` FROM `b`'
            . ' UNION ALL SELECT `id` FROM `c`',
            $sql
        );
    }

    // ─── Subqueries ───────────────────────────────────────────────────────────

    public function test_where_in_with_subquery(): void
    {
        $sub = $this->builder()->table('admins')->select('id');

        $sql = $this->builder()->table('users')
            ->whereIn('id', $sub)
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `users` WHERE `id` IN (SELECT `id` FROM `admins`)',
            $sql
        );
    }

    public function test_where_not_in_with_subquery(): void
    {
        $sub = $this->builder()->table('banned')->select('user_id');

        $sql = $this->builder()->table('users')
            ->whereNotIn('id', $sub)
            ->toSql();
        $this->assertSame(
            'SELECT * FROM `users` WHERE `id` NOT IN (SELECT `user_id` FROM `banned`)',
            $sql
        );
    }

    public function test_where_in_still_works_with_array(): void
    {
        $sql = $this->builder()->table('t')->whereIn('status', ['a', 'b'])->toSql();
        $this->assertSame("SELECT * FROM `t` WHERE `status` IN ('a', 'b')", $sql);
    }

    public function test_where_not_in_still_works_with_array(): void
    {
        $sql = $this->builder()->table('t')->whereNotIn('id', [1, 2])->toSql();
        $this->assertSame('SELECT * FROM `t` WHERE `id` NOT IN (1, 2)', $sql);
    }
}
