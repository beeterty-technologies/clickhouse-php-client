<?php

namespace Beeterty\ClickHouse;

final class Config
{
    /**
     * Create a new ClickHouse connection configuration.
     *
     * @param string $host           ClickHouse host (default: 127.0.0.1).
     * @param int    $port           HTTP interface port (default: 8123).
     * @param string $database       Default database (default: default).
     * @param string $username       ClickHouse user (default: default).
     * @param string $password       ClickHouse password (default: empty).
     * @param bool   $https          Use HTTPS instead of HTTP (default: false).
     * @param int    $timeout        cURL transfer timeout in seconds (default: 30).
     * @param int    $connectTimeout cURL connect timeout in seconds (default: 5).
     * @param int    $retries        Number of extra attempts on connection failure (default: 0).
     * @param int    $retryDelay     Delay between retries in milliseconds (default: 100).
     * @param bool   $compression    Gzip-compress INSERT bodies sent to ClickHouse (default: false).
     * @param array<string, int|float|string|bool> $settings Global ClickHouse query settings appended as URL parameters on every request (e.g. ['max_threads' => 4, 'max_execution_time' => 60]).
     * @param string[] $roles        Roles to activate before each query via the `role` URL parameter (ClickHouse 24.4+). Multiple roles are supported.
     * @param string|null $profile   ClickHouse settings profile to apply to every request via the `profile` URL parameter.
     * @param string|null $quotaKey  Quota key sent as the `quota_key` URL parameter for per-tenant rate limiting.
     */
    public function __construct(
        public readonly string $host = '127.0.0.1',
        public readonly int $port = 8123,
        public readonly string $database = 'default',
        public readonly string $username = 'default',
        public readonly string $password = '',
        public readonly bool $https = false,
        public readonly int $timeout = 30,
        public readonly int $connectTimeout = 5,
        public readonly int $retries = 0,
        public readonly int $retryDelay = 100,
        public readonly bool $compression = false,
        public readonly array $settings = [],
        public readonly array $roles = [],
        public readonly ?string $profile = null,
        public readonly ?string $quotaKey = null,
    ) {
        //
    }

    /**
     * Return a new Config with the given host.
     *
     * @param string $host ClickHouse host (e.g. IP address or domain name).
     * @return static
     */
    public function withHost(string $host): static
    {
        return new static(
            host:           $host,
            port:           $this->port,
            database:       $this->database,
            username:       $this->username,
            password:       $this->password,
            https:          $this->https,
            timeout:        $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries:        $this->retries,
            retryDelay:     $this->retryDelay,
            compression:    $this->compression,
            settings:       $this->settings,
            roles:          $this->roles,
            profile:        $this->profile,
            quotaKey:       $this->quotaKey,
        );
    }

    /**
     * Return a new Config with the given port.
     *
     * @param int $port HTTP interface port (default: 8123).
     * @return static
     */
    public function withPort(int $port): static
    {
        return new static(
            host:           $this->host,
            port:           $port,
            database:       $this->database,
            username:       $this->username,
            password:       $this->password,
            https:          $this->https,
            timeout:        $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries:        $this->retries,
            retryDelay:     $this->retryDelay,
            compression:    $this->compression,
            settings:       $this->settings,
            roles:          $this->roles,
            profile:        $this->profile,
            quotaKey:       $this->quotaKey,
        );
    }

    /**
     * Return a new Config targeting the given database.
     *
     * @param string $database Default database name.
     * @return static
     */
    public function withDatabase(string $database): static
    {
        return new static(
            host:           $this->host,
            port:           $this->port,
            database:       $database,
            username:       $this->username,
            password:       $this->password,
            https:          $this->https,
            timeout:        $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries:        $this->retries,
            retryDelay:     $this->retryDelay,
            compression:    $this->compression,
            settings:       $this->settings,
            roles:          $this->roles,
            profile:        $this->profile,
            quotaKey:       $this->quotaKey,
        );
    }

    /**
     * Return a new Config with the given credentials.
     *
     * @param string $username ClickHouse username.
     * @param string $password ClickHouse password.
     * @return static
     */
    public function withCredentials(string $username, string $password = ''): static
    {
        return new static(
            host:           $this->host,
            port:           $this->port,
            database:       $this->database,
            username:       $username,
            password:       $password,
            https:          $this->https,
            timeout:        $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries:        $this->retries,
            retryDelay:     $this->retryDelay,
            compression:    $this->compression,
            settings:       $this->settings,
            roles:          $this->roles,
            profile:        $this->profile,
            quotaKey:       $this->quotaKey,
        );
    }

    /**
     * Return a new Config with HTTPS enabled or disabled.
     *
     * @param bool $https Use HTTPS instead of HTTP (default: true).
     * @return static
     */
    public function withHttps(bool $https = true): static
    {
        return new static(
            host:           $this->host,
            port:           $this->port,
            database:       $this->database,
            username:       $this->username,
            password:       $this->password,
            https:          $https,
            timeout:        $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries:        $this->retries,
            retryDelay:     $this->retryDelay,
            compression:    $this->compression,
            settings:       $this->settings,
            roles:          $this->roles,
            profile:        $this->profile,
            quotaKey:       $this->quotaKey,
        );
    }

    /**
     * Return a new Config with the given cURL transfer timeout (seconds).
     *
     * @param int $timeout cURL transfer timeout in seconds.
     * @return static
     */
    public function withTimeout(int $timeout): static
    {
        return new static(
            host:           $this->host,
            port:           $this->port,
            database:       $this->database,
            username:       $this->username,
            password:       $this->password,
            https:          $this->https,
            timeout:        $timeout,
            connectTimeout: $this->connectTimeout,
            retries:        $this->retries,
            retryDelay:     $this->retryDelay,
            compression:    $this->compression,
            settings:       $this->settings,
            roles:          $this->roles,
            profile:        $this->profile,
            quotaKey:       $this->quotaKey,
        );
    }

    /**
     * Return a new Config with the given cURL connect timeout (seconds).
     *
     * @param int $connectTimeout cURL connect timeout in seconds.
     * @return static
     */
    public function withConnectTimeout(int $connectTimeout): static
    {
        return new static(
            host:           $this->host,
            port:           $this->port,
            database:       $this->database,
            username:       $this->username,
            password:       $this->password,
            https:          $this->https,
            timeout:        $this->timeout,
            connectTimeout: $connectTimeout,
            retries:        $this->retries,
            retryDelay:     $this->retryDelay,
            compression:    $this->compression,
            settings:       $this->settings,
            roles:          $this->roles,
            profile:        $this->profile,
            quotaKey:       $this->quotaKey,
        );
    }

    /**
     * Return a new Config with retry settings.
     *
     * @param int $retries Number of extra attempts after the first failure.
     * @param int $delayMs Milliseconds to wait between attempts.
     * @return static
     */
    public function withRetries(int $retries, int $delayMs = 100): static
    {
        return new static(
            host:           $this->host,
            port:           $this->port,
            database:       $this->database,
            username:       $this->username,
            password:       $this->password,
            https:          $this->https,
            timeout:        $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries:        $retries,
            retryDelay:     $delayMs,
            compression:    $this->compression,
            settings:       $this->settings,
            roles:          $this->roles,
            profile:        $this->profile,
            quotaKey:       $this->quotaKey,
        );
    }

    /**
     * Return a new Config with gzip compression enabled or disabled for INSERT bodies.
     *
     * @param bool $compression Gzip-compress INSERT bodies sent to ClickHouse (default: true).
     * @return static
     */
    public function withCompression(bool $compression = true): static
    {
        return new static(
            host:           $this->host,
            port:           $this->port,
            database:       $this->database,
            username:       $this->username,
            password:       $this->password,
            https:          $this->https,
            timeout:        $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries:        $this->retries,
            retryDelay:     $this->retryDelay,
            compression:    $compression,
            settings:       $this->settings,
            roles:          $this->roles,
            profile:        $this->profile,
            quotaKey:       $this->quotaKey,
        );
    }

    /**
     * Return a new Config with global ClickHouse query settings.
     *
     * Settings are appended as URL parameters on every request. Per-request
     * settings passed to query() / execute() / insert() are merged on top of
     * these globals, with per-request values taking precedence.
     *
     * Example:
     *   $config->withSettings(['max_threads' => 4, 'max_execution_time' => 60])
     *   // → ?max_threads=4&max_execution_time=60 on every request
     *
     * @see https://clickhouse.com/docs/en/operations/settings/settings
     *
     * @param array<string, int|float|string|bool> $settings Map of setting name to value.
     * @return static
     */
    public function withSettings(array $settings): static
    {
        return new static(
            host:           $this->host,
            port:           $this->port,
            database:       $this->database,
            username:       $this->username,
            password:       $this->password,
            https:          $this->https,
            timeout:        $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries:        $this->retries,
            retryDelay:     $this->retryDelay,
            compression:    $this->compression,
            settings:       $settings,
            roles:          $this->roles,
            profile:        $this->profile,
            quotaKey:       $this->quotaKey,
        );
    }

    /**
     * Return a new Config with the given roles activated on every request.
     *
     * Roles are passed via the `role` URL parameter (supported since ClickHouse
     * 24.4). Multiple roles can be provided; each is sent as a separate `role`
     * parameter in the request URL.
     *
     * Example:
     *   $config->withRole('analyst')
     *   $config->withRole('analyst', 'reader')
     *
     * @see https://clickhouse.com/docs/en/interfaces/http#setting-role-with-query-parameters
     *
     * @param string ...$roles One or more role names to activate.
     * @return static
     */
    public function withRole(string ...$roles): static
    {
        return new static(
            host:           $this->host,
            port:           $this->port,
            database:       $this->database,
            username:       $this->username,
            password:       $this->password,
            https:          $this->https,
            timeout:        $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries:        $this->retries,
            retryDelay:     $this->retryDelay,
            compression:    $this->compression,
            settings:       $this->settings,
            roles:          $roles,
            profile:        $this->profile,
            quotaKey:       $this->quotaKey,
        );
    }

    /**
     * Return a new Config with the given ClickHouse settings profile applied globally.
     *
     * The profile name is sent as the `profile` URL parameter on every request.
     * Profiles are defined in the ClickHouse server configuration
     * (users.xml / user-defined profiles).
     *
     * Example:
     *   $config->withProfile('readonly')
     *   // → ?profile=readonly on every request
     *
     * @see https://clickhouse.com/docs/en/operations/settings/profiles
     *
     * @param string $profile The profile name as defined in the ClickHouse server config.
     * @return static
     */
    public function withProfile(string $profile): static
    {
        return new static(
            host:           $this->host,
            port:           $this->port,
            database:       $this->database,
            username:       $this->username,
            password:       $this->password,
            https:          $this->https,
            timeout:        $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries:        $this->retries,
            retryDelay:     $this->retryDelay,
            compression:    $this->compression,
            settings:       $this->settings,
            roles:          $this->roles,
            profile:        $profile,
            quotaKey:       $this->quotaKey,
        );
    }

    /**
     * Return a new Config with the given quota key applied globally.
     *
     * The quota key is sent as the `quota_key` URL parameter on every request,
     * allowing ClickHouse quotas to be enforced per tenant, user, or any
     * arbitrary string token.
     *
     * Example:
     *   $config->withQuotaKey('tenant-abc-123')
     *   // → ?quota_key=tenant-abc-123 on every request
     *
     * @see https://clickhouse.com/docs/en/operations/quotas
     *
     * @param string $quotaKey Arbitrary string key used to identify this quota bucket.
     * @return static
     */
    public function withQuotaKey(string $quotaKey): static
    {
        return new static(
            host:           $this->host,
            port:           $this->port,
            database:       $this->database,
            username:       $this->username,
            password:       $this->password,
            https:          $this->https,
            timeout:        $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries:        $this->retries,
            retryDelay:     $this->retryDelay,
            compression:    $this->compression,
            settings:       $this->settings,
            roles:          $this->roles,
            profile:        $this->profile,
            quotaKey:       $quotaKey,
        );
    }

    /**
     * Get the base data source URL (scheme + host + port).
     *
     * @return string
     */
    public function dataSource(): string
    {
        $scheme = $this->https ? 'https' : 'http';

        return "{$scheme}://{$this->host}:{$this->port}";
    }

    /**
     * Create a Config from an associative array.
     *
     * Recognized keys: host, port, database, username, password, https,
     * timeout, connect_timeout, retries, retry_delay, compression,
     * settings, roles, profile, quota_key.
     *
     * @param array<string, mixed> $config
     * @return self
     */
    public static function fromArray(array $config): self
    {
        return new self(
            host:           self::s($config['host'] ?? '127.0.0.1'),
            port:           self::i($config['port'] ?? 8123),
            database:       self::s($config['database'] ?? 'default'),
            username:       self::s($config['username'] ?? 'default'),
            password:       self::s($config['password'] ?? ''),
            https:          self::b($config['https'] ?? false),
            timeout:        self::i($config['timeout'] ?? 30),
            connectTimeout: self::i($config['connect_timeout'] ?? 5),
            retries:        self::i($config['retries'] ?? 0),
            retryDelay:     self::i($config['retry_delay'] ?? 100),
            compression:    self::b($config['compression'] ?? false),
            settings:       self::settings($config['settings'] ?? []),
            roles:          self::roles($config['roles'] ?? []),
            profile:        isset($config['profile']) && \is_scalar($config['profile']) ? (string) $config['profile'] : null,
            quotaKey:       isset($config['quota_key']) && \is_scalar($config['quota_key']) ? (string) $config['quota_key'] : null,
        );
    }

    /**
     * Coerce a mixed value to string.
     *
     * @param mixed $value
     * @return string
     */
    private static function s(mixed $value): string
    {
        return \is_scalar($value) ? (string) $value : '';
    }

    /**
     * Coerce a mixed value to int.
     *
     * @param mixed $value
     * @return int
     */
    private static function i(mixed $value): int
    {
        return \is_scalar($value) ? (int) $value : 0;
    }

    /**
     * Coerce a mixed value to bool.
     *
     * @param mixed $value
     * @return bool
     */
    private static function b(mixed $value): bool
    {
        return \is_scalar($value) ? (bool) $value : false;
    }

    /**
     * Coerce a mixed value to a settings array.
     *
     * @param mixed $value
     * @return array<string, int|float|string|bool>
     */
    private static function settings(mixed $value): array
    {
        if (!\is_array($value)) {
            return [];
        }

        $result = [];

        foreach ($value as $k => $v) {
            if (\is_string($k) && \is_scalar($v)) {
                /** @var int|float|string|bool $v */
                $result[$k] = $v;
            }
        }

        return $result;
    }

    /**
     * Coerce a mixed value to a list of role strings.
     *
     * @param mixed $value
     * @return string[]
     */
    private static function roles(mixed $value): array
    {
        if (!\is_array($value)) {
            return [];
        }

        $result = [];

        foreach ($value as $v) {
            if (\is_string($v)) {
                $result[] = $v;
            }
        }

        return $result;
    }
}
