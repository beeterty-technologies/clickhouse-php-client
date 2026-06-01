<?php

namespace Beeterty\ClickHouse\Tests\Unit;

use Beeterty\ClickHouse\Client;
use Beeterty\ClickHouse\Config;
use Beeterty\ClickHouse\Session;
use PHPUnit\Framework\TestCase;

class SessionTest extends TestCase
{
    private function session(string $id = 'test-session', int $timeout = 60): Session
    {
        $client = $this->createMock(Client::class);

        return new Session($client, $id, $timeout);
    }

    public function test_id_returns_session_id(): void
    {
        $this->assertSame('my-session', $this->session('my-session')->id());
    }

    public function test_default_timeout_is_60(): void
    {
        $session = new Session($this->createMock(Client::class), 'sid');
        $this->assertSame('sid', $session->id());
    }

    public function test_client_session_factory_returns_session_instance(): void
    {
        $client  = new Client(new Config());
        $session = $client->session('abc', timeout: 120);

        $this->assertInstanceOf(Session::class, $session);
        $this->assertSame('abc', $session->id());
    }

    public function test_session_execute_passes_session_id_to_client(): void
    {
        $client = $this->createMock(Client::class);

        $client->expects($this->once())
            ->method('execute')
            ->with(
                'CREATE TEMPORARY TABLE tmp (id UInt64) ENGINE = Memory',
                [],
                $this->callback(function (array $settings): bool {
                    return $settings['session_id'] === 'my-session'
                        && $settings['session_timeout'] === 300;
                }),
            )
            ->willReturn(true);

        $session = new Session($client, 'my-session', 300);
        $session->execute('CREATE TEMPORARY TABLE tmp (id UInt64) ENGINE = Memory');
    }

    public function test_session_execute_merges_caller_settings(): void
    {
        $client = $this->createMock(Client::class);

        $client->expects($this->once())
            ->method('execute')
            ->with(
                'SELECT 1',
                [],
                $this->callback(function (array $settings): bool {
                    return $settings['session_id'] === 'sid'
                        && $settings['max_threads'] === 4;
                }),
            )
            ->willReturn(true);

        $session = new Session($client, 'sid');
        $session->execute('SELECT 1', settings: ['max_threads' => 4]);
    }

    public function test_session_query_passes_session_params(): void
    {
        $client = $this->createMock(Client::class);

        $client->expects($this->once())
            ->method('query')
            ->with(
                'SELECT 1',
                [],
                null,
                $this->callback(fn(array $s): bool => $s['session_id'] === 'sid'),
                [],
            );

        $session = new Session($client, 'sid');
        $session->query('SELECT 1');
    }

    public function test_session_insert_passes_session_params(): void
    {
        $client = $this->createMock(Client::class);

        $client->expects($this->once())
            ->method('insert')
            ->with(
                'tmp',
                [['id' => 1]],
                null,
                $this->callback(fn(array $s): bool => $s['session_id'] === 'sid'),
            )
            ->willReturn(true);

        $session = new Session($client, 'sid');
        $session->insert('tmp', [['id' => 1]]);
    }
}
