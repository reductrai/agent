# Contributing to ReductrAI Agent

Thank you for your interest in contributing to ReductrAI!

## Code of Conduct

Be respectful, constructive, and professional.

## What We Accept

### Encouraged Contributions

- **Bug fixes** - Found a bug? Fix it!
- **Performance improvements** - Make it faster
- **New telemetry formats** - Add support for more formats
- **Documentation** - Improve clarity, fix typos, add examples
- **Tests** - Increase coverage

### Requires Discussion First

Open an issue before working on:

- New features
- Architectural changes
- Changes to compression algorithms
- Breaking API changes

## How to Contribute

### 1. Fork and Clone

```bash
git clone https://github.com/YOUR_USERNAME/agent.git
cd agent
```

### 2. Create a Branch

```bash
git checkout -b fix/your-bug-fix
# or
git checkout -b feat/your-feature
```

### 3. Make Changes

- Write clear, readable code
- Follow existing code style
- Add tests for new functionality

### 4. Test

```bash
make test
make build
```

### 5. Commit

Use clear, descriptive commit messages:

```
fix: Correct metric parsing for Prometheus format

The Prometheus parser was incorrectly handling multi-line metrics.
This fixes the issue by properly tracking line continuations.

Fixes #123
```

### 6. Push and Create PR

```bash
git push origin your-branch
```

Then create a Pull Request on GitHub.

## Code Style

### Go Code

- Run `gofmt` before committing
- Follow [Effective Go](https://golang.org/doc/effective_go)
- Use meaningful variable names
- Keep functions focused and small

### Comments

- Explain "why", not "what"
- Document public APIs
- Use complete sentences

### Tests

- Required for new features
- Test edge cases
- Use table-driven tests when appropriate

## Pull Request Guidelines

1. **One feature/fix per PR** - Keep PRs focused
2. **Update documentation** - If behavior changes
3. **Add tests** - For new code paths
4. **Pass CI** - All tests must pass
5. **Respond to feedback** - Be open to suggestions

## Development Setup

```bash
# Install dependencies
make deps

# Build
make build

# Test
make test

# Run locally
./dist/reductrai start --local
```

## Architecture Overview

```
agent/
├── cmd/reductrai/       # CLI entry point
├── internal/
│   ├── ingest/          # Telemetry ingestion server
│   ├── storage/         # DuckDB storage layer
│   ├── compression/     # Compression algorithms
│   ├── detector/        # Anomaly detection
│   ├── context/         # Context extraction
│   └── cloud/           # Cloud communication
└── pkg/types/           # Shared types
```

## License

By contributing, you agree to license your contributions under the SSPL (Server Side Public License).

## Questions?

- Open an issue for bugs or feature requests
- Email: support@reductrai.com

---

Thank you for helping make ReductrAI better!
