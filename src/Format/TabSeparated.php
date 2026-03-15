<?php

namespace Beeterty\ClickHouse\Format;

use Beeterty\ClickHouse\Format\Contracts\Format;

final class TabSeparated implements Format
{
    /**
     * The headers from the TabSeparated format.
     * 
     * @var array
     */
    private array $headers = [];

    /**
     * @inheritDoc
     */
    public function name(): string
    {
        return 'TabSeparatedWithNames';
    }

    /**
     * @inheritDoc
     */
    public function encode(array $rows): string
    {
        if (empty($rows)) {
            return '';
        }

        $headers = array_keys($rows[0]);
        $lines   = [implode("\t", $headers)];

        foreach ($rows as $row) {
            $lines[] = implode("\t", array_values($row));
        }

        return implode("\n", $lines);
    }

    /**
     * @inheritDoc
     */
    public function decode(string $raw): array
    {
        if (trim($raw) === '') {
            return [];
        }

        $lines = explode("\n", trim($raw));

        $this->headers = explode("\t", array_shift($lines));

        $rows = [];

        foreach ($lines as $line) {
            if (trim($line) === '') {
                continue;
            }

            $values = explode("\t", $line);

            $rows[] = array_combine($this->headers, $values);
        }

        return $rows;
    }

    /**
     * Get the headers from the TabSeparated format.
     * 
     * @return array
     */
    public function headers(): array
    {
        return $this->headers;
    }
}
