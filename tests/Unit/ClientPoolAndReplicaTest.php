<?php

namespace Beeterty\ClickHouse\Tests\Unit;

use Beeterty\ClickHouse\Client;
use Beeterty\ClickHouse\Config;
use PHPUnit\Framework\TestCase;

/**
 * Tests for connection-pool and read-replica routing added in 1.2.0.
 */
class ClientPoolAndReplicaTest extends TestCase
{
    // ─── Connection pool ──────────────────────────────────────────────────────

    public function test_constructor_default_pool_size_does_not_throw(): void
    {
        $client = new Client(new Config());

        $this->assertInstanceOf(Client::class, $client);
    }

    public function test_constructor_explicit_pool_size_does_not_throw(): void
    {
        $client = new Client(new Config(), poolSize: 5);

        $this->assertInstanceOf(Client::class, $client);
    }

    public function test_pool_size_one_is_backward_compatible(): void
    {
        // poolSize: 1 is the default; constructing without the parameter should
        // behave identically to passing 1 explicitly.
        $implicit = new Client(new Config());
        $explicit  = new Client(new Config(), poolSize: 1);

        $this->assertInstanceOf(Client::class, $implicit);
        $this->assertInstanceOf(Client::class, $explicit);
    }

    public function test_pool_size_zero_is_coerced_to_one(): void
    {
        // poolSize < 1 must not cause failures; the implementation clamps to 1.
        $client = new Client(new Config(), poolSize: 0);

        $this->assertInstanceOf(Client::class, $client);
    }

    public function test_pool_size_large_does_not_throw(): void
    {
        $client = new Client(new Config(), poolSize: 20);

        $this->assertInstanceOf(Client::class, $client);
    }

    // ─── Read-replica constructor ─────────────────────────────────────────────

    public function test_constructor_accepts_replicas_array(): void
    {
        $client = new Client(
            new Config(host: 'primary'),
            replicas: [
                new Config(host: 'replica1'),
                new Config(host: 'replica2'),
            ],
        );

        $this->assertInstanceOf(Client::class, $client);
    }

    public function test_constructor_empty_replicas_does_not_throw(): void
    {
        $client = new Client(new Config(), replicas: []);

        $this->assertInstanceOf(Client::class, $client);
    }

    // ─── selectReplica() via Reflection ──────────────────────────────────────

    /**
     * Invoke the private selectReplica() method via reflection.
     */
    private function callSelectReplica(Client $client): Config
    {
        $method = new \ReflectionMethod(Client::class, 'selectReplica');
        $result = $method->invoke($client);

        $this->assertInstanceOf(Config::class, $result);

        return $result;
    }

    public function test_select_replica_falls_back_to_primary_when_no_replicas(): void
    {
        $primary = new Config(host: 'primary');
        $client  = new Client($primary);

        $selected = $this->callSelectReplica($client);

        $this->assertSame($primary, $selected);
    }

    public function test_select_replica_round_robins_over_replicas(): void
    {
        $primary  = new Config(host: 'primary');
        $replica1 = new Config(host: 'replica1');
        $replica2 = new Config(host: 'replica2');

        $client = new Client($primary, replicas: [$replica1, $replica2]);

        $first  = $this->callSelectReplica($client);
        $second = $this->callSelectReplica($client);
        $third  = $this->callSelectReplica($client);  // wraps back to replica1

        $this->assertSame($replica1, $first);
        $this->assertSame($replica2, $second);
        $this->assertSame($replica1, $third);
    }

    public function test_select_replica_with_single_replica_always_returns_that_replica(): void
    {
        $primary = new Config(host: 'primary');
        $replica = new Config(host: 'replica');

        $client = new Client($primary, replicas: [$replica]);

        $this->assertSame($replica, $this->callSelectReplica($client));
        $this->assertSame($replica, $this->callSelectReplica($client));
        $this->assertSame($replica, $this->callSelectReplica($client));
    }

    // ─── Routing: read methods use replica, write methods use primary ─────────

    public function test_primary_is_used_when_no_replicas_for_read(): void
    {
        // When replicas is empty, selectReplica() must return the primary config.
        $primary = new Config(host: 'primary');
        $client  = new Client($primary);

        $this->assertSame('primary', $this->callSelectReplica($client)->host);
    }

    public function test_replica_host_is_different_from_primary_host(): void
    {
        $primary = new Config(host: 'primary');
        $replica = new Config(host: 'replica');

        $client   = new Client($primary, replicas: [$replica]);
        $selected = $this->callSelectReplica($client);

        $this->assertSame('replica', $selected->host);
        $this->assertNotSame('primary', $selected->host);
    }

    // ─── Pool + replicas combined ─────────────────────────────────────────────

    public function test_pool_and_replicas_can_be_combined(): void
    {
        $client = new Client(
            new Config(host: 'primary'),
            poolSize: 3,
            replicas: [
                new Config(host: 'replica1'),
                new Config(host: 'replica2'),
            ],
        );

        $this->assertInstanceOf(Client::class, $client);
    }
}
