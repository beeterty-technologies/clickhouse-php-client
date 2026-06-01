<?php

namespace Beeterty\ClickHouse;

use Beeterty\ClickHouse\Format\Contracts\Format;
use Beeterty\ClickHouse\Format\JsonEachRow;

/**
 * Represents an in-memory temporary table sent alongside a query as external data.
 *
 * ClickHouse can receive temporary table data as part of a multipart POST
 * request. The table exists only for the duration of the query and is
 * referenced by name in the SQL (e.g. in a JOIN or WHERE IN clause).
 *
 * Create from a pre-encoded string:
 *   new ExternalTable('users', 'id UInt64, name String', "1\tAlice\n2\tBob")
 *
 * Or from row arrays using a Format instance:
 *   ExternalTable::fromRows('users', 'id UInt64, name String', [
 *       ['id' => 1, 'name' => 'Alice'],
 *       ['id' => 2, 'name' => 'Bob'],
 *   ])
 *
 * @see https://clickhouse.com/docs/en/engines/table-engines/special/external-data
 */
final class ExternalTable
{
    /**
     * Create an ExternalTable from a pre-encoded data string.
     *
     * @param string $name      Temporary table name referenced in the SQL query.
     * @param string $structure Column definitions in ClickHouse DDL syntax,
     *                          e.g. "id UInt64, name String, active UInt8".
     * @param string $data      Table data already encoded in the specified format.
     * @param string $format    ClickHouse format name of the encoded data (default: TabSeparated).
     */
    public function __construct(
        public readonly string $name,
        public readonly string $structure,
        public readonly string $data,
        public readonly string $format = 'TabSeparated',
    ) {
        //
    }

    /**
     * Create an ExternalTable by encoding row arrays with a Format instance.
     *
     * Example:
     *   ExternalTable::fromRows(
     *       name: 'lookup',
     *       structure: 'id UInt64, label String',
     *       rows: [
     *           ['id' => 1, 'label' => 'foo'],
     *           ['id' => 2, 'label' => 'bar'],
     *       ],
     *   )
     *
     * @see https://clickhouse.com/docs/en/engines/table-engines/special/external-data
     *
     * @param string $name      Temporary table name referenced in the SQL query.
     * @param string $structure Column definitions, e.g. "id UInt64, label String".
     * @param array<int, array<string, mixed>> $rows Row data as associative arrays.
     * @param Format|null $format Format used to encode the rows (default: JsonEachRow).
     * @return self
     */
    public static function fromRows(
        string $name,
        string $structure,
        array $rows,
        ?Format $format = null,
    ): self {
        $format ??= new JsonEachRow();

        return new self(
            name:      $name,
            structure: $structure,
            data:      $format->encode($rows),
            format:    $format->name(),
        );
    }
}
