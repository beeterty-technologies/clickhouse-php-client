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
     * Get the data source URL for the ClickHouse client.
     * 
     * @return string
     */
    public function dataSource(): string
    {
        $scheme = $this->https ? 'https' : 'http';

        return "{$scheme}://{$this->host}:{$this->port}";
    }

    /**
     * Create a new ClickHouse client instance from an associative array of configuration options.
     * 
     * @param array $config
     * @return self
     */
    public static function fromArray(array $config): self
    {
        return new self(
            host: $config['host'] ?? '127.0.0.1',
            port: (int) ($config['port'] ?? 8123),
            database: $config['database'] ?? 'default',
            username: $config['username'] ?? 'default',
            password: $config['password'] ?? '',
            https: (bool) ($config['https'] ?? false),
            timeout: (int) ($config['timeout'] ?? 30),
            connectTimeout: (int) ($config['connect_timeout'] ?? 5),
            retries: (int) ($config['retries'] ?? 0),
            retryDelay: (int) ($config['retry_delay'] ?? 100),
            compression: (bool) ($config['compression'] ?? false),
        );
    }
}
