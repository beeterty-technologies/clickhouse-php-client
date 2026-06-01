<?php

namespace Beeterty\ClickHouse\Tests\Unit;

use Beeterty\ClickHouse\Client;
use Beeterty\ClickHouse\Config;
use PHPUnit\Framework\TestCase;

class InsertStreamTest extends TestCase
{
    public function test_insert_stream_throws_for_invalid_source(): void
    {
        $client = new Client(new Config());

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/insertStream\(\) expects a resource or \\\\Generator/');

        // @phpstan-ignore-next-line (intentional wrong type for test)
        $client->insertStream('events', 'not-a-resource-or-generator');
    }

    public function test_insert_stream_accepts_generator(): void
    {
        // Verify that passing a Generator does not throw an InvalidArgumentException.
        // We cannot test the actual HTTP call without a live server, so we just
        // confirm the path reaches curl (which will fail with a connection error,
        // not an argument error).
        $client = new Client(new Config(host: '127.0.0.1', port: 1));

        $gen = (function (): \Generator {
            yield ['id' => 1];
        })();

        try {
            $client->insertStream('events', $gen);
        } catch (\InvalidArgumentException $e) {
            $this->fail('insertStream() should not throw InvalidArgumentException for a Generator');
        } catch (\Throwable) {
            // Connection failure expected — not an argument error.
        }

        $this->assertTrue(true);
    }

    public function test_insert_stream_accepts_resource(): void
    {
        $client = new Client(new Config(host: '127.0.0.1', port: 1));

        $fh = fopen('php://memory', 'rb');
        \assert($fh !== false);

        try {
            $client->insertStream('events', $fh);
        } catch (\InvalidArgumentException $e) {
            $this->fail('insertStream() should not throw InvalidArgumentException for a resource');
        } catch (\Throwable) {
            // Connection failure expected — not an argument error.
        } finally {
            fclose($fh);
        }

        $this->assertTrue(true);
    }
}
