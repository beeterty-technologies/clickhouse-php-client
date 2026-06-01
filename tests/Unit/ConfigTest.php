<?php

namespace Beeterty\ClickHouse\Tests\Unit;

use Beeterty\ClickHouse\Config;
use PHPUnit\Framework\TestCase;

class ConfigTest extends TestCase
{
    // ─── dataSource() ─────────────────────────────────────────────────────────

    public function test_data_source_returns_http_url(): void
    {
        $config = new Config(host: '127.0.0.1', port: 8123);

        $this->assertSame('http://127.0.0.1:8123', $config->dataSource());
    }

    public function test_data_source_returns_https_url(): void
    {
        $config = new Config(host: 'ch.example.com', port: 8443, https: true);

        $this->assertSame('https://ch.example.com:8443', $config->dataSource());
    }

    // ─── Constructor defaults ─────────────────────────────────────────────────

    public function test_default_host(): void
    {
        $this->assertSame('127.0.0.1', (new Config())->host);
    }

    public function test_default_port(): void
    {
        $this->assertSame(8123, (new Config())->port);
    }

    public function test_default_database(): void
    {
        $this->assertSame('default', (new Config())->database);
    }

    public function test_default_username(): void
    {
        $this->assertSame('default', (new Config())->username);
    }

    public function test_default_password_is_empty(): void
    {
        $this->assertSame('', (new Config())->password);
    }

    public function test_default_https_is_false(): void
    {
        $this->assertFalse((new Config())->https);
    }

    public function test_default_timeout(): void
    {
        $this->assertSame(30, (new Config())->timeout);
    }

    public function test_default_connect_timeout(): void
    {
        $this->assertSame(5, (new Config())->connectTimeout);
    }

    // ─── fromArray() ──────────────────────────────────────────────────────────

    public function test_from_array_maps_all_values(): void
    {
        $config = Config::fromArray([
            'host'            => 'ch.example.com',
            'port'            => 9000,
            'database'        => 'mydb',
            'username'        => 'admin',
            'password'        => 'secret',
            'https'           => true,
            'timeout'         => 60,
            'connect_timeout' => 10,
        ]);

        $this->assertSame('ch.example.com', $config->host);
        $this->assertSame(9000, $config->port);
        $this->assertSame('mydb', $config->database);
        $this->assertSame('admin', $config->username);
        $this->assertSame('secret', $config->password);
        $this->assertTrue($config->https);
        $this->assertSame(60, $config->timeout);
        $this->assertSame(10, $config->connectTimeout);
    }

    public function test_from_array_uses_defaults_for_missing_keys(): void
    {
        $config = Config::fromArray([]);

        $this->assertSame('127.0.0.1', $config->host);
        $this->assertSame(8123, $config->port);
        $this->assertSame('default', $config->database);
        $this->assertSame('default', $config->username);
        $this->assertSame('', $config->password);
        $this->assertFalse($config->https);
        $this->assertSame(30, $config->timeout);
        $this->assertSame(5, $config->connectTimeout);
    }

    public function test_from_array_casts_port_to_int(): void
    {
        $config = Config::fromArray(['port' => '9000']);

        $this->assertSame(9000, $config->port);
    }

    // ─── New defaults (retries / retryDelay / compression) ────────────────────

    public function test_default_retries_is_zero(): void
    {
        $this->assertSame(0, (new Config())->retries);
    }

    public function test_default_retry_delay_is_100ms(): void
    {
        $this->assertSame(100, (new Config())->retryDelay);
    }

    public function test_default_compression_is_false(): void
    {
        $this->assertFalse((new Config())->compression);
    }

    public function test_retries_can_be_set(): void
    {
        $this->assertSame(3, (new Config(retries: 3))->retries);
    }

    public function test_compression_can_be_enabled(): void
    {
        $this->assertTrue((new Config(compression: true))->compression);
    }

    public function test_from_array_maps_retries_and_retry_delay(): void
    {
        $config = Config::fromArray(['retries' => 3, 'retry_delay' => 500]);

        $this->assertSame(3, $config->retries);
        $this->assertSame(500, $config->retryDelay);
    }

    public function test_from_array_maps_compression(): void
    {
        $this->assertTrue(Config::fromArray(['compression' => true])->compression);
    }

    public function test_from_array_defaults_retries_to_zero(): void
    {
        $this->assertSame(0, Config::fromArray([])->retries);
    }

    public function test_from_array_defaults_compression_to_false(): void
    {
        $this->assertFalse(Config::fromArray([])->compression);
    }

    // ─── withSettings() ───────────────────────────────────────────────────────

    public function test_default_settings_is_empty_array(): void
    {
        $this->assertSame([], (new Config())->settings);
    }

    public function test_with_settings_replaces_settings(): void
    {
        $config = (new Config())->withSettings(['max_threads' => 4, 'max_execution_time' => 60]);

        $this->assertSame(['max_threads' => 4, 'max_execution_time' => 60], $config->settings);
    }

    public function test_with_settings_is_immutable(): void
    {
        $original = new Config();
        $new      = $original->withSettings(['max_threads' => 4]);

        $this->assertSame([], $original->settings);
        $this->assertSame(['max_threads' => 4], $new->settings);
    }

    public function test_with_settings_preserves_other_fields(): void
    {
        $config = (new Config(host: 'ch.example.com', database: 'mydb'))
            ->withSettings(['max_threads' => 2]);

        $this->assertSame('ch.example.com', $config->host);
        $this->assertSame('mydb', $config->database);
    }

    public function test_from_array_maps_settings(): void
    {
        $config = Config::fromArray(['settings' => ['max_threads' => 4]]);

        $this->assertSame(['max_threads' => 4], $config->settings);
    }

    public function test_from_array_defaults_settings_to_empty(): void
    {
        $this->assertSame([], Config::fromArray([])->settings);
    }

    // ─── withRole() ───────────────────────────────────────────────────────────

    public function test_default_roles_is_empty_array(): void
    {
        $this->assertSame([], (new Config())->roles);
    }

    public function test_with_role_single(): void
    {
        $config = (new Config())->withRole('analyst');

        $this->assertSame(['analyst'], $config->roles);
    }

    public function test_with_role_multiple(): void
    {
        $config = (new Config())->withRole('analyst', 'reader');

        $this->assertSame(['analyst', 'reader'], $config->roles);
    }

    public function test_with_role_is_immutable(): void
    {
        $original = new Config();
        $new      = $original->withRole('analyst');

        $this->assertSame([], $original->roles);
        $this->assertSame(['analyst'], $new->roles);
    }

    public function test_from_array_maps_roles(): void
    {
        $config = Config::fromArray(['roles' => ['analyst', 'reader']]);

        $this->assertSame(['analyst', 'reader'], $config->roles);
    }

    public function test_from_array_defaults_roles_to_empty(): void
    {
        $this->assertSame([], Config::fromArray([])->roles);
    }

    // ─── withProfile() ────────────────────────────────────────────────────────

    public function test_default_profile_is_null(): void
    {
        $this->assertNull((new Config())->profile);
    }

    public function test_with_profile_sets_profile(): void
    {
        $config = (new Config())->withProfile('readonly');

        $this->assertSame('readonly', $config->profile);
    }

    public function test_with_profile_is_immutable(): void
    {
        $original = new Config();
        $new      = $original->withProfile('readonly');

        $this->assertNull($original->profile);
        $this->assertSame('readonly', $new->profile);
    }

    public function test_from_array_maps_profile(): void
    {
        $config = Config::fromArray(['profile' => 'readonly']);

        $this->assertSame('readonly', $config->profile);
    }

    public function test_from_array_defaults_profile_to_null(): void
    {
        $this->assertNull(Config::fromArray([])->profile);
    }

    // ─── withQuotaKey() ───────────────────────────────────────────────────────

    public function test_default_quota_key_is_null(): void
    {
        $this->assertNull((new Config())->quotaKey);
    }

    public function test_with_quota_key_sets_quota_key(): void
    {
        $config = (new Config())->withQuotaKey('tenant-abc-123');

        $this->assertSame('tenant-abc-123', $config->quotaKey);
    }

    public function test_with_quota_key_is_immutable(): void
    {
        $original = new Config();
        $new      = $original->withQuotaKey('tenant-abc');

        $this->assertNull($original->quotaKey);
        $this->assertSame('tenant-abc', $new->quotaKey);
    }

    public function test_from_array_maps_quota_key(): void
    {
        $config = Config::fromArray(['quota_key' => 'tenant-xyz']);

        $this->assertSame('tenant-xyz', $config->quotaKey);
    }

    public function test_from_array_defaults_quota_key_to_null(): void
    {
        $this->assertNull(Config::fromArray([])->quotaKey);
    }
}
