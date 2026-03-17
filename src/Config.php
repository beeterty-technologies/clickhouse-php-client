<?php

namespace Beeterty\ClickHouse;

class Config
{
    /**
     * Create a new ClickHouse connection configuration.
     *
     * @param string $host           ClickHouse host (default: 127.0.0.1)
     * @param int    $port           HTTP interface port (default: 8123)
     * @param string $database       Default database (default: default)
     * @param string $username       ClickHouse user (default: default)
     * @param string $password       ClickHouse password (default: empty)
     * @param bool   $https          Use HTTPS instead of HTTP (default: false)
     * @param int    $timeout        cURL transfer timeout in seconds (default: 30)
     * @param int    $connectTimeout cURL connect timeout in seconds (default: 5)
     * @param int    $retries        Number of extra attempts on connection failure (default: 0)
     * @param int    $retryDelay     Delay between retries in milliseconds (default: 100)
     * @param bool   $compression    Gzip-compress INSERT bodies sent to ClickHouse (default: false)
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
    ) {
        //
    }

    /**
     * Return a new Config with the given host.
     * 
     * @param string $host  ClickHouse host (e.g. IP address or domain name)
     * @return static
     */
    public function withHost(string $host): static
    {
        return new static(
            host: $host,
            port: $this->port,
            database: $this->database,
            username: $this->username,
            password: $this->password,
            https: $this->https,
            timeout: $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries: $this->retries,
            retryDelay: $this->retryDelay,
            compression: $this->compression,
        );
    }

    /**
     * Return a new Config with the given port.
     * 
     * @param int $port  HTTP interface port (default: 8123)
     * @return static
     */
    public function withPort(int $port): static
    {
        return new static(
            host: $this->host,
            port: $port,
            database: $this->database,
            username: $this->username,
            password: $this->password,
            https: $this->https,
            timeout: $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries: $this->retries,
            retryDelay: $this->retryDelay,
            compression: $this->compression,
        );
    }

    /**
     * Return a new Config targeting the given database.
     * 
     * @param string $database  Default database (default: default)
     * @return static
     */
    public function withDatabase(string $database): static
    {
        return new static(
            host: $this->host,
            port: $this->port,
            database: $database,
            username: $this->username,
            password: $this->password,
            https: $this->https,
            timeout: $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries: $this->retries,
            retryDelay: $this->retryDelay,
            compression: $this->compression,
        );
    }

    /**
     * Return a new Config with the given credentials.
     *
     * @param string $username  ClickHouse username
     * @param string $password  ClickHouse password
     * @return static
     */
    public function withCredentials(string $username, string $password = ''): static
    {
        return new static(
            host: $this->host,
            port: $this->port,
            database: $this->database,
            username: $username,
            password: $password,
            https: $this->https,
            timeout: $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries: $this->retries,
            retryDelay: $this->retryDelay,
            compression: $this->compression,
        );
    }

    /**
     * Return a new Config with HTTPS enabled or disabled.
     * 
     * @param bool $https  Use HTTPS instead of HTTP (default: true)
     * @return static
     */
    public function withHttps(bool $https = true): static
    {
        return new static(
            host: $this->host,
            port: $this->port,
            database: $this->database,
            username: $this->username,
            password: $this->password,
            https: $https,
            timeout: $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries: $this->retries,
            retryDelay: $this->retryDelay,
            compression: $this->compression,
        );
    }

    /**
     * Return a new Config with the given cURL transfer timeout (seconds).
     *
     * @param int $timeout  cURL transfer timeout in seconds
     * @return static
     */
    public function withTimeout(int $timeout): static
    {
        return new static(
            host: $this->host,
            port: $this->port,
            database: $this->database,
            username: $this->username,
            password: $this->password,
            https: $this->https,
            timeout: $timeout,
            connectTimeout: $this->connectTimeout,
            retries: $this->retries,
            retryDelay: $this->retryDelay,
            compression: $this->compression,
        );
    }

    /**
     * Return a new Config with the given cURL connect timeout (seconds).
     */
    public function withConnectTimeout(int $connectTimeout): static
    {
        return new static(
            host: $this->host,
            port: $this->port,
            database: $this->database,
            username: $this->username,
            password: $this->password,
            https: $this->https,
            timeout: $this->timeout,
            connectTimeout: $connectTimeout,
            retries: $this->retries,
            retryDelay: $this->retryDelay,
            compression: $this->compression,
        );
    }

    /**
     * Return a new Config with retry settings.
     *
     * @param int $retries   Number of extra attempts after the first failure
     * @param int $delayMs   Milliseconds to wait between attempts
     */
    public function withRetries(int $retries, int $delayMs = 100): static
    {
        return new static(
            host: $this->host,
            port: $this->port,
            database: $this->database,
            username: $this->username,
            password: $this->password,
            https: $this->https,
            timeout: $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries: $retries,
            retryDelay: $delayMs,
            compression: $this->compression,
        );
    }

    /**
     * Return a new Config with gzip compression enabled or disabled for INSERT bodies.
     * 
     * @param bool $compression  Gzip-compress INSERT bodies sent to ClickHouse (default: true)
     * @return static
     */
    public function withCompression(bool $compression = true): static
    {
        return new static(
            host: $this->host,
            port: $this->port,
            database: $this->database,
            username: $this->username,
            password: $this->password,
            https: $this->https,
            timeout: $this->timeout,
            connectTimeout: $this->connectTimeout,
            retries: $this->retries,
            retryDelay: $this->retryDelay,
            compression: $compression,
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
     * timeout, connect_timeout, retries, retry_delay, compression.
     *
     * @param array<string, mixed> $config
     * @return self
     */
    public static function fromArray(array $config): self
    {
        return new self(
            host: (string) ($config['host'] ?? '127.0.0.1'),
            port: (int) ($config['port'] ?? 8123),
            database: (string) ($config['database'] ?? 'default'),
            username: (string) ($config['username'] ?? 'default'),
            password: (string) ($config['password'] ?? ''),
            https: (bool) ($config['https'] ?? false),
            timeout: (int) ($config['timeout'] ?? 30),
            connectTimeout: (int) ($config['connect_timeout'] ?? 5),
            retries: (int) ($config['retries'] ?? 0),
            retryDelay: (int) ($config['retry_delay'] ?? 100),
            compression: (bool) ($config['compression'] ?? false),
        );
    }
}
