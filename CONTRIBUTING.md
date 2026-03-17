# Contributing

Thank you for taking the time to contribute. This document covers everything you need to get a working dev environment, run the tests, and open a pull request.

> **Not sure what to work on?** Check [ROADMAP.md](ROADMAP.md) — every unchecked item is a candidate. Open an issue before starting significant work so we can align on the approach.

---

## Prerequisites

- PHP 8.2 or higher
- [Composer](https://getcomposer.org)
- A running ClickHouse instance (for integration tests)
- Git

---

## Local setup

```bash
git clone https://github.com/beeterty/clickhouse-php-client.git
cd clickhouse-php-client
composer install
```

---

## Running ClickHouse locally

The fastest way is the official install script:

```bash
curl https://clickhouse.com/install.sh | sh
clickhouse server   # starts on port 8123 by default
```

Verify it's up:

```bash
curl -s http://127.0.0.1:8123/ping   # → Ok.
```

No password is needed for the default user on a fresh local install.

---

## Running tests

### Unit tests only (no ClickHouse needed)

```bash
./vendor/bin/phpunit --testsuite Unit
```

### Integration tests (requires a running ClickHouse)

```bash
./vendor/bin/phpunit --testsuite Integration
```

Environment variables control the connection. The defaults match a standard local install:

| Variable | Default |
|---|---|
| `CLICKHOUSE_HOST` | `127.0.0.1` |
| `CLICKHOUSE_PORT` | `8123` |
| `CLICKHOUSE_DB` | `default` |
| `CLICKHOUSE_USERNAME` | `default` |
| `CLICKHOUSE_PASSWORD` | *(empty)* |

Override any of them as needed:

```bash
CLICKHOUSE_HOST=my-server CLICKHOUSE_PASSWORD=secret \
    ./vendor/bin/phpunit --testsuite Integration
```

### Full suite

```bash
./vendor/bin/phpunit
```

### Static analysis

```bash
./vendor/bin/phpstan analyse
```

The project runs PHPStan at level 8. Pull requests must pass with zero errors.

### Code style

```bash
./vendor/bin/php-cs-fixer fix --dry-run --diff   # preview
./vendor/bin/php-cs-fixer fix                    # apply
```

---

## Project layout

```
src/
├── Client.php              Public API: query, insert, execute, parallel, insertFile, …
├── Config.php              Connection settings
├── QueryBuilder.php        Fluent SELECT builder
├── Statement.php           Query result wrapper
├── Format/
│   ├── Contracts/Format.php  encode() / decode() / name() interface
│   ├── JsonEachRow.php
│   ├── Csv.php               CSVWithNames
│   └── TabSeparated.php      TabSeparatedWithNames
├── Schema/
│   ├── Schema.php            DDL entry point: create, alter, drop, materialized views
│   ├── Grammar.php           SQL compilation
│   ├── Blueprint.php         Column/option definitions
│   ├── ColumnDefinition.php  Fluent column modifiers: nullable, default, comment
│   └── Engine/               MergeTree, ReplacingMergeTree, SummingMergeTree, …
└── Exception/
    ├── ClickHouseException.php
    ├── ConnectionException.php
    └── QueryException.php

tests/
├── Unit/                   Pure unit tests — no ClickHouse required
└── Integration/            Tests run against a real ClickHouse instance
```

---

## Adding a new format

1. Create `src/Format/MyFormat.php` implementing `Format`:

```php
namespace Beeterty\ClickHouse\Format;

use Beeterty\ClickHouse\Format\Contracts\Format;

final class MyFormat implements Format
{
    public function name(): string   { return 'MyFormatName'; }
    public function encode(array $rows): string { /* … */ }
    public function decode(string $raw): array  { /* … */ }
}
```

2. Add unit tests in `tests/Unit/MyFormatTest.php`.

---

## Adding a new engine

1. Create `src/Schema/Engine/MyEngine.php` implementing `Engine`:

```php
namespace Beeterty\ClickHouse\Schema\Engine;

use Beeterty\ClickHouse\Schema\Contracts\Engine;

class MyEngine implements Engine
{
    public function compile(): string
    {
        return 'MyEngine()';
    }
}
```

2. Add coverage in `tests/Unit/Schema/GrammarTest.php`.

---

## Pull request checklist

Before opening a PR, please make sure all of the following pass locally:

- [ ] `./vendor/bin/phpunit` — all tests green
- [ ] `./vendor/bin/phpstan analyse` — zero errors
- [ ] `./vendor/bin/php-cs-fixer fix --dry-run --diff` — no outstanding diffs

PRs that fail CI will not be merged.

---

## Branching and commits

- Branch off `main` for all work: `git checkout -b feat/my-feature`
- Keep commits focused — one logical change per commit
- Use conventional commit prefixes where it helps: `feat:`, `fix:`, `test:`, `refactor:`, `docs:`
- All changes must include tests (unit tests at minimum; integration tests for anything that touches `Client`, `Schema`, or a `Format`)

---

## Releasing

Releases are driven by semver git tags. Pushing a tag triggers the release workflow, which first runs the full test matrix (PHP 8.2 and 8.3) and only proceeds to create the GitHub Release if all tests pass.

```bash
git tag v1.2.0
git push origin v1.2.0
```

The tag name is the Packagist version — there is no `"version"` field in `composer.json`.

Pre-releases use the `-alpha.N` / `-beta.N` / `-rc.N` suffix conventions:

```bash
git tag v1.2.0-beta.1
git push origin v1.2.0-beta.1
```
