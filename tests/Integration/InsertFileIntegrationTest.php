<?php

namespace Beeterty\ClickHouse\Tests\Integration;

use Beeterty\ClickHouse\Format\TabSeparated;
use Beeterty\ClickHouse\Schema\Blueprint;
use Beeterty\ClickHouse\Schema\Engine\MergeTree;

class InsertFileIntegrationTest extends IntegrationTestCase
{
    private string $table;

    private array $tmpFiles = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->table = 'insert_file_test_' . substr(md5((string) microtime(true)), 0, 8);

        $this->client->schema()->create($this->table, function (Blueprint $table): void {
            $table->uint32('id');
            $table->string('name');
            $table->int32('score');
            $table->engine(new MergeTree())->orderBy('id');
        });
    }

    protected function tearDown(): void
    {
        $this->dropTableSilently($this->table);

        foreach ($this->tmpFiles as $file) {
            if (file_exists($file)) {
                unlink($file);
            }
        }
    }

    private function tmpFile(string $content): string
    {
        $path = tempnam(sys_get_temp_dir(), 'ch_test_');
        assert(is_string($path));

        file_put_contents($path, $content);
        $this->tmpFiles[] = $path;

        return $path;
    }

    public function test_insert_file_csv_inserts_rows(): void
    {
        $csv = implode("\n", [
            'id,name,score',
            '1,Alice,90',
            '2,Bob,80',
            '3,Charlie,70',
        ]);

        $path = $this->tmpFile($csv);

        $result = $this->client->insertFile($this->table, $path);

        $this->assertTrue($result);

        $count = $this->client->table($this->table)->count();
        $this->assertSame(3, $count);
    }

    public function test_insert_file_returns_true_on_success(): void
    {
        $csv  = "id,name,score\n10,Dave,55";
        $path = $this->tmpFile($csv);

        $this->assertTrue($this->client->insertFile($this->table, $path));
    }

    public function test_insert_file_data_is_queryable_after_insert(): void
    {
        $csv = implode("\n", [
            'id,name,score',
            '42,Eve,100',
        ]);

        $path = $this->tmpFile($csv);
        $this->client->insertFile($this->table, $path);

        $row = $this->client->table($this->table)->where('id', 42)->first();

        $this->assertNotNull($row);
        $this->assertSame('Eve', $row['name']);
        $this->assertSame('100', (string) $row['score']);
    }

    public function test_insert_file_tsv_with_explicit_format(): void
    {
        $tsv = implode("\n", [
            "id\tname\tscore",
            "7\tFrank\t65",
            "8\tGrace\t75",
        ]);

        $path = $this->tmpFile($tsv);

        $result = $this->client->insertFile($this->table, $path, new TabSeparated());

        $this->assertTrue($result);

        $count = $this->client->table($this->table)->count();
        $this->assertSame(2, $count);
    }

    public function test_insert_file_throws_on_missing_file(): void
    {
        $this->expectException(\RuntimeException::class);

        $this->client->insertFile($this->table, '/nonexistent/path/to/file.csv');
    }

    public function test_insert_file_empty_csv_after_header(): void
    {
        $csv  = "id,name,score";
        $path = $this->tmpFile($csv);

        $result = $this->client->insertFile($this->table, $path);

        $this->assertTrue($result);
        $this->assertSame(0, $this->client->table($this->table)->count());
    }

    public function test_insert_file_large_batch_streams_correctly(): void
    {
        $lines = ['id,name,score'];

        for ($i = 1; $i <= 500; $i++) {
            $lines[] = "{$i},User{$i},{$i}";
        }

        $path = $this->tmpFile(implode("\n", $lines));

        $this->client->insertFile($this->table, $path);

        $this->assertSame(500, $this->client->table($this->table)->count());
    }
}
