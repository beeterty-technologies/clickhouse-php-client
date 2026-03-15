<?php

namespace Beeterty\ClickHouse;

class Config
{
    /**
     * Create a new ClickHouse client instance. 
     * 
     * @param string $host
     * @param int $port
     * @param string $database
     * @param string $username
     * @param string $password
     * @param bool $https
     * @param int $timeout
     * @param int $connectTimeout
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
        );
    }
}
