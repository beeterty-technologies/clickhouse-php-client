<?php

namespace Beeterty\ClickHouse\Schema;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

/**
 * Defines the structure of a ClickHouse table.
 *
 * Used in both CREATE TABLE and ALTER TABLE contexts:
 * - In create()        : column methods define the table columns.
 * - In table() (ALTER) : column methods become ADD COLUMN; use dropColumn() / renameColumn() for removals.
 *
 * @see https://clickhouse.com/docs/en/sql-reference/statements/create/table
 * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/column
 */
class Blueprint
{
    /**
     * Columns to create or add.
     *
     * @var ColumnDefinition[]
     */
    private array $columns = [];

    /**
     * Column names to drop in an ALTER TABLE context.
     *
     * @var string[]
     */
    private array $drops = [];

    /**
     * Column renames queued for an ALTER TABLE context.
     *
     * @var array<array{from: string, to: string}>
     */
    private array $renames = [];

    /**
     * The storage engine for this table (e.g. MergeTree, ReplacingMergeTree).
     *
     * @var Engine|null
     */
    private ?Engine $engine = null;

    /**
     * The ORDER BY key columns — defines sort order and the sparse primary index.
     *
     * @var string[]
     */
    private array $orderBy = [];

    /**
     * The PARTITION BY expression, or null if not set.
     *
     * @var string|null
     */
    private ?string $partitionBy = null;

    /**
     * The PRIMARY KEY expression when it differs from the ORDER BY key, or null.
     *
     * @var string|null
     */
    private ?string $primaryKey = null;

    /**
     * The SAMPLE BY expression for fractional row sampling, or null if not set.
     *
     * @var string|null
     */
    private ?string $sampleBy = null;

    /**
     * Engine-level SETTINGS key-value pairs.
     *
     * @var array<string, mixed>
     */
    private array $settings = [];

    /**
     * The table-level TTL expression, or null if not set.
     *
     * @var string|null
     */
    private ?string $ttl = null;

    /**
     * The human-readable table COMMENT, or null if not set.
     *
     * @var string|null
     */
    private ?string $comment = null;

    /**
     * Add a standard UInt64 primary key column named 'id' (or a custom name).
     *
     * @param string $name Column name — defaults to 'id'.
     * @return ColumnDefinition
     */
    public function id(string $name = 'id'): ColumnDefinition
    {
        return $this->uint64($name);
    }

    /**
     * Add nullable `created_at` and `updated_at` DateTime columns.
     *
     * Both columns are wrapped in Nullable(DateTime) so they accept NULL for
     * rows where the timestamp is unknown.
     *
     * @param string|null $timezone Optional timezone string, e.g. 'UTC' or 'Europe/London'.
     * @return static
     */
    public function timestamps(?string $timezone = null): static
    {
        $this->dateTime('created_at', $timezone)->nullable();
        $this->dateTime('updated_at', $timezone)->nullable();

        return $this;
    }

    /**
     * Add a nullable `deleted_at` DateTime column for soft deletes.
     *
     * @param string      $column   Column name — defaults to 'deleted_at'.
     * @param string|null $timezone Optional timezone string.
     * @return ColumnDefinition
     */
    public function softDeletes(string $column = 'deleted_at', ?string $timezone = null): ColumnDefinition
    {
        return $this->dateTime($column, $timezone)->nullable();
    }

    /**
     * Drop the standard timestamp columns (ALTER TABLE context).
     *
     * @return static
     */
    public function dropTimestamps(): static
    {
        $this->drops[] = 'created_at';
        $this->drops[] = 'updated_at';

        return $this;
    }

    /**
     * Drop the soft-delete column (ALTER TABLE context).
     *
     * @param string $column Column name — defaults to 'deleted_at'.
     * @return static
     */
    public function dropSoftDeletes(string $column = 'deleted_at'): static
    {
        $this->drops[] = $column;

        return $this;
    }

    /**
     * Add a column using a raw ClickHouse type string.
     *
     * Use this as an escape hatch for types not covered by dedicated methods,
     * such as Tuple(UInt32, String) or AggregateFunction(sum, UInt64).
     *
     * Example:
     *   $table->rawColumn('coords', 'Tuple(Float64, Float64)');
     *   $table->rawColumn('state',  'AggregateFunction(sum, UInt64)');
     *
     * @param string $name       Column name.
     * @param string $definition Raw ClickHouse type string.
     * @return ColumnDefinition
     */
    public function rawColumn(string $name, string $definition): ColumnDefinition
    {
        return $this->addColumn($name, $definition);
    }

    /**
     * Add a UInt8 column (unsigned 8-bit integer, range 0–255).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function uint8(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'UInt8');
    }

    /**
     * Add a UInt16 column (unsigned 16-bit integer, range 0–65 535).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function uint16(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'UInt16');
    }

    /**
     * Add a UInt32 column (unsigned 32-bit integer, range 0–4 294 967 295).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function uint32(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'UInt32');
    }

    /**
     * Add a UInt64 column (unsigned 64-bit integer, range 0–18 446 744 073 709 551 615).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function uint64(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'UInt64');
    }

    /**
     * Add a UInt128 column (unsigned 128-bit integer).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function uint128(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'UInt128');
    }

    /**
     * Add a UInt256 column (unsigned 256-bit integer).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function uint256(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'UInt256');
    }

    /**
     * Add an Int8 column (signed 8-bit integer, range -128–127).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function int8(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'Int8');
    }

    /**
     * Add an Int16 column (signed 16-bit integer, range -32 768–32 767).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function int16(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'Int16');
    }

    /**
     * Add an Int32 column (signed 32-bit integer, range -2 147 483 648–2 147 483 647).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function int32(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'Int32');
    }

    /**
     * Add an Int64 column (signed 64-bit integer).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function int64(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'Int64');
    }

    /**
     * Add an Int128 column (signed 128-bit integer).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function int128(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'Int128');
    }

    /**
     * Add an Int256 column (signed 256-bit integer).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function int256(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'Int256');
    }

    /**
     * Add a Float32 column (single-precision IEEE 754 floating-point number).
     *
     * Note: floating-point types may introduce rounding errors. Use Decimal for
     * financial or precise numeric data.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/float
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function float32(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'Float32');
    }

    /**
     * Add a Float64 column (double-precision IEEE 754 floating-point number).
     *
     * Note: floating-point types may introduce rounding errors. Use Decimal for
     * financial or precise numeric data.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/float
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function float64(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'Float64');
    }

    /**
     * Add a Decimal(precision, scale) column for exact numeric storage.
     *
     * Use this for monetary values or any data where floating-point rounding is
     * unacceptable. ClickHouse supports Decimal32, Decimal64, Decimal128, and
     * Decimal256 internally — the engine picks the smallest that fits.
     *
     * Example:
     *   $table->decimal('price', 18, 4);  // 18 significant digits, 4 decimal places
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/decimal
     *
     * @param string $name      Column name.
     * @param int    $precision Total number of significant digits (1–76).
     * @param int    $scale     Digits after the decimal point (0–precision).
     * @return ColumnDefinition
     */
    public function decimal(string $name, int $precision, int $scale): ColumnDefinition
    {
        return $this->addColumn($name, "Decimal({$precision}, {$scale})");
    }

    /**
     * Add a String column — an arbitrary-length byte sequence with no encoding constraint.
     *
     * Unlike VARCHAR in other databases, ClickHouse String has no length limit and
     * stores raw bytes. Use LowCardinality(String) for columns with few distinct values.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/string
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function string(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'String');
    }

    /**
     * Add a FixedString(N) column — a fixed-length byte sequence of exactly N bytes.
     *
     * Shorter values are padded with null bytes; values longer than N are rejected.
     * Useful for hashes, short codes, or fixed-width identifiers where storage
     * efficiency matters.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/fixedstring
     *
     * @param string $name   Column name.
     * @param int    $length Exact byte length of the stored value.
     * @return ColumnDefinition
     */
    public function fixedString(string $name, int $length): ColumnDefinition
    {
        return $this->addColumn($name, "FixedString({$length})");
    }

    /**
     * Add a Date column (calendar date with day precision, range 1970-01-01–2149-06-06).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/date
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function date(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'Date');
    }

    /**
     * Add a Date32 column (wider calendar date range, 1900-01-01–2299-12-31).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/date32
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function date32(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'Date32');
    }

    /**
     * Add a DateTime column with second precision.
     *
     * When $timezone is provided the column stores UTC internally and converts on
     * display. Omit it to store local/unzoned timestamps.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/datetime
     *
     * @param string      $name     Column name.
     * @param string|null $timezone IANA timezone string, e.g. 'UTC' or 'America/New_York'.
     * @return ColumnDefinition
     */
    public function dateTime(string $name, ?string $timezone = null): ColumnDefinition
    {
        $type = $timezone ? "DateTime('{$timezone}')" : 'DateTime';

        return $this->addColumn($name, $type);
    }

    /**
     * Add a DateTime64 column with sub-second precision.
     *
     * $precision specifies the number of sub-second digits: 3 = milliseconds,
     * 6 = microseconds, 9 = nanoseconds.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/datetime64
     *
     * @param string      $name      Column name.
     * @param int         $precision Sub-second precision (0–9). Defaults to 3 (milliseconds).
     * @param string|null $timezone  IANA timezone string.
     * @return ColumnDefinition
     */
    public function dateTime64(string $name, int $precision = 3, ?string $timezone = null): ColumnDefinition
    {
        $type = $timezone
            ? "DateTime64({$precision}, '{$timezone}')"
            : "DateTime64({$precision})";

        return $this->addColumn($name, $type);
    }

    /**
     * Add a Bool column (stored as UInt8, values 0/1).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/boolean
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function boolean(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'Bool');
    }

    /**
     * Add a UUID column (stored as two UInt64 values, displayed as a hyphenated string).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/uuid
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function uuid(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'UUID');
    }

    /**
     * Define an Enum8 column — up to 255 named constants backed by Int8.
     *
     * Accepts either an associative map of name => value or a sequential list
     * of names (auto-indexed from 1).
     *
     * Example:
     *   $table->enum8('status', ['active' => 1, 'inactive' => 2]);
     *   $table->enum8('status', ['active', 'inactive']);  // same result
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/enum
     *
     * @param string                        $name   Column name.
     * @param array<string|int, string|int> $values Enum member definitions.
     * @return ColumnDefinition
     */
    public function enum8(string $name, array $values): ColumnDefinition
    {
        return $this->addColumn($name, $this->buildEnumType('Enum8', $values));
    }

    /**
     * Define an Enum16 column — up to 65 535 named constants backed by Int16.
     * Same signature as enum8() but with a larger value range.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/enum
     *
     * @param string                        $name   Column name.
     * @param array<string|int, string|int> $values Enum member definitions.
     * @return ColumnDefinition
     */
    public function enum16(string $name, array $values): ColumnDefinition
    {
        return $this->addColumn($name, $this->buildEnumType('Enum16', $values));
    }

    /**
     * Add an IPv4 column (stored as UInt32, displayed as dotted-decimal notation).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/ipv4
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function ipv4(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'IPv4');
    }

    /**
     * Add an IPv6 column (stored as a 16-byte fixed binary, displayed in colon notation).
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/ipv6
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function ipv6(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'IPv6');
    }

    /**
     * Add a JSON column for semi-structured data with dynamic sub-columns.
     *
     * Note: the JSON type is experimental in some ClickHouse versions.
     * Consider using String + JSONExtract functions for compatibility.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/json
     *
     * @param string $name Column name.
     * @return ColumnDefinition
     */
    public function json(string $name): ColumnDefinition
    {
        return $this->addColumn($name, 'JSON');
    }

    /**
     * Add an Array(T) column — a variable-length array of elements of the given inner type.
     *
     * Example:
     *   $table->array('tags', 'String');       // Array(String)
     *   $table->array('scores', 'UInt32');     // Array(UInt32)
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/array
     *
     * @param string $name      Column name.
     * @param string $innerType ClickHouse type string for array elements.
     * @return ColumnDefinition
     */
    public function array(string $name, string $innerType): ColumnDefinition
    {
        return $this->addColumn($name, "Array({$innerType})");
    }

    /**
     * Add a Map(K, V) column — a key-value store with homogeneous key and value types.
     *
     * Example:
     *   $table->map('meta', 'String', 'String');     // Map(String, String)
     *   $table->map('counts', 'String', 'UInt64');   // Map(String, UInt64)
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/map
     *
     * @param string $name      Column name.
     * @param string $keyType   ClickHouse type string for map keys.
     * @param string $valueType ClickHouse type string for map values.
     * @return ColumnDefinition
     */
    public function map(string $name, string $keyType, string $valueType): ColumnDefinition
    {
        return $this->addColumn($name, "Map({$keyType}, {$valueType})");
    }

    /**
     * Add a Tuple of typed elements.
     *
     * Example:
     *   $table->tuple('coords', 'Float64', 'Float64');           // Tuple(Float64, Float64)
     *   $table->tuple('point',  'String', 'UInt32', 'Float64');  // Tuple(String, UInt32, Float64)
     *
     * @see https://clickhouse.com/docs/en/sql-reference/data-types/tuple
     *
     * @param string $name     Column name.
     * @param string ...$types ClickHouse type strings for each tuple element.
     * @return ColumnDefinition
     */
    public function tuple(string $name, string ...$types): ColumnDefinition
    {
        return $this->addColumn($name, 'Tuple(' . implode(', ', $types) . ')');
    }

    /**
     * Set the table storage engine.
     *
     * The engine determines how data is stored, merged, and queried.
     * MergeTree and its variants are the recommended choice for most analytical tables.
     *
     * Example:
     *   $table->engine(new MergeTree())->orderBy('id');
     *   $table->engine(new ReplacingMergeTree('version'))->orderBy(['user_id', 'ts']);
     *
     * @see https://clickhouse.com/docs/en/engines/table-engines
     *
     * @param Engine $engine Engine instance.
     * @return static
     */
    public function engine(Engine $engine): static
    {
        $this->engine = $engine;

        return $this;
    }

    /**
     * Set the ORDER BY key — defines the primary sort order and the sparse index.
     *
     * All MergeTree-family engines require an ORDER BY clause. Use a single column
     * string or an array for composite keys. The order of columns matters: put the
     * most selective column last for optimal compression and index efficiency.
     *
     * Example:
     *   $table->orderBy('id');
     *   $table->orderBy(['tenant_id', 'created_at', 'id']);
     *
     * @see https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#order_by
     *
     * @param string|string[] $columns Column name or array of column names.
     * @return static
     */
    public function orderBy(string|array $columns): static
    {
        $this->orderBy = (array) $columns;

        return $this;
    }

    /**
     * Set the PARTITION BY expression to split data into physical partitions.
     *
     * Partitioning by a time unit (e.g. toYYYYMM(created_at)) allows ClickHouse to
     * skip entire partitions during queries and enables efficient partition-level
     * operations (DROP PARTITION, ATTACH, etc.).
     *
     * Example:
     *   $table->partitionBy('toYYYYMM(created_at)');
     *   $table->partitionBy('toDate(event_time)');
     *
     * @see https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/custom-partitioning-key
     *
     * @param string $expr Raw SQL partitioning expression.
     * @return static
     */
    public function partitionBy(string $expr): static
    {
        $this->partitionBy = $expr;

        return $this;
    }

    /**
     * Set the PRIMARY KEY expression (sparse index separate from ORDER BY).
     *
     * In most cases you do not need to set this explicitly — ClickHouse uses ORDER BY
     * as the primary key by default. Specify it only when you want a primary key that
     * is a strict prefix of the ORDER BY key.
     *
     * @see https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#primary-key
     *
     * @param string $expr Raw SQL primary key expression.
     * @return static
     */
    public function primaryKey(string $expr): static
    {
        $this->primaryKey = $expr;

        return $this;
    }

    /**
     * Set the SAMPLE BY expression for random fractional sampling at query time.
     *
     * The expression must be included in (or be a function of) the ORDER BY key.
     * Once set, queries can use SAMPLE 0.1 to read ~10% of the data efficiently.
     *
     * Example:
     *   $table->orderBy(['user_id', 'ts'])->sampleBy('user_id');
     *
     * @see https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#sample-by
     *
     * @param string $expr Column name or expression used as the sampling key.
     * @return static
     */
    public function sampleBy(string $expr): static
    {
        $this->sampleBy = $expr;

        return $this;
    }

    /**
     * Merge additional ClickHouse table-level SETTINGS into the blueprint.
     *
     * Settings control engine-specific behaviour such as merge thresholds,
     * deduplication windows, and storage policies.
     *
     * Example:
     *   $table->settings(['index_granularity' => 8192, 'merge_max_block_size' => 8192]);
     *
     * @see https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#settings
     *
     * @param array<string, mixed> $settings Key-value pairs of setting name => value.
     * @return static
     */
    public function settings(array $settings): static
    {
        $this->settings = array_merge($this->settings, $settings);

        return $this;
    }

    /**
     * Set the table-level TTL expression for automatic row expiry or data movement.
     *
     * The expression must evaluate to a Date or DateTime. Rows whose TTL has passed
     * are removed during merges. You can also use TTL to move data between storage
     * volumes (tiered storage).
     *
     * Example:
     *   $table->ttl('created_at + INTERVAL 90 DAY');
     *   $table->ttl('event_date + INTERVAL 1 YEAR DELETE');
     *
     * @see https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#ttl
     *
     * @param string $expr Raw SQL TTL expression.
     * @return static
     */
    public function ttl(string $expr): static
    {
        $this->ttl = $expr;

        return $this;
    }

    /**
     * Set a human-readable COMMENT on the table.
     *
     * @param string $text Comment text stored in system.tables.comment.
     * @return static
     */
    public function comment(string $text): static
    {
        $this->comment = $text;

        return $this;
    }

    /**
     * Mark a column to be dropped in an ALTER TABLE context.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/column#drop-column
     *
     * @param string $column Column name to drop.
     * @return static
     */
    public function dropColumn(string $column): static
    {
        $this->drops[] = $column;

        return $this;
    }

    /**
     * Rename a column in an ALTER TABLE context.
     *
     * @see https://clickhouse.com/docs/en/sql-reference/statements/alter/column#rename-column
     *
     * @param string $from Current column name.
     * @param string $to   New column name.
     * @return static
     */
    public function renameColumn(string $from, string $to): static
    {
        $this->renames[] = ['from' => $from, 'to' => $to];

        return $this;
    }

    /**
     * Return all column definitions queued for creation or addition.
     *
     * @return ColumnDefinition[]
     */
    public function getColumns(): array
    {
        return $this->columns;
    }

    /**
     * Return the configured storage engine, or null if not set.
     *
     * @return Engine|null
     */
    public function getEngine(): ?Engine
    {
        return $this->engine;
    }

    /**
     * Return the ORDER BY key columns.
     *
     * @return string[]
     */
    public function getOrderBy(): array
    {
        return $this->orderBy;
    }

    /**
     * Return the PARTITION BY expression, or null if not set.
     *
     * @return string|null
     */
    public function getPartitionBy(): ?string
    {
        return $this->partitionBy;
    }

    /**
     * Return the PRIMARY KEY expression, or null if not set.
     *
     * @return string|null
     */
    public function getPrimaryKey(): ?string
    {
        return $this->primaryKey;
    }

    /**
     * Return the SAMPLE BY expression, or null if not set.
     *
     * @return string|null
     */
    public function getSampleBy(): ?string
    {
        return $this->sampleBy;
    }

    /**
     * Return the engine-level SETTINGS key-value pairs.
     *
     * @return array<string, mixed>
     */
    public function getSettings(): array
    {
        return $this->settings;
    }

    /**
     * Return the table-level TTL expression, or null if not set.
     *
     * @return string|null
     */
    public function getTtl(): ?string
    {
        return $this->ttl;
    }

    /**
     * Return the table COMMENT, or null if not set.
     *
     * @return string|null
     */
    public function getComment(): ?string
    {
        return $this->comment;
    }

    /**
     * Return the column names queued to be dropped.
     *
     * @return string[]
     */
    public function getDrops(): array
    {
        return $this->drops;
    }

    /**
     * Return the column renames queued for ALTER TABLE.
     *
     * @return array<array{from: string, to: string}>
     */
    public function getRenames(): array
    {
        return $this->renames;
    }

    /**
     * Create a new ColumnDefinition and append it to the column list.
     *
     * @param string $name Column name.
     * @param string $type Raw ClickHouse type string.
     * @return ColumnDefinition
     */
    private function addColumn(string $name, string $type): ColumnDefinition
    {
        $column          = new ColumnDefinition($name, $type);
        $this->columns[] = $column;

        return $column;
    }

    /**
     * Build a ClickHouse Enum type string from a name-value map.
     *
     * @param string                        $enumType 'Enum8' or 'Enum16'.
     * @param array<string|int, string|int> $values   Enum member definitions.
     * @return string
     */
    private function buildEnumType(string $enumType, array $values): string
    {
        $pairs = [];

        foreach ($values as $key => $value) {
            if (is_string($key)) {
                $pairs[] = "'{$key}' = {$value}";
            } else {
                $pairs[] = "'{$value}' = " . ($key + 1);
            }
        }

        return $enumType . '(' . implode(', ', $pairs) . ')';
    }
}
