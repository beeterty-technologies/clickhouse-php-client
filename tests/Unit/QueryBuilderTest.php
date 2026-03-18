<?php

namespace Beeterty\ClickHouse\Tests\Unit;

use Beeterty\ClickHouse\Client;
use Beeterty\ClickHouse\Query\Builder;
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
}
