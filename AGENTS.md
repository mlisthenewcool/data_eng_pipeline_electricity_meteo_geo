# AGENTS.md

This file contains guidelines and commands for agentic coding agents working in this repository.

## Development Commands

### Environment Setup

```bash
# Install dependencies
uv sync

# Activate virtual environment
source .venv/bin/activate
```

### Code Quality

```bash
# Lint and fix code
uv run ruff check --fix

# Format code
uv run ruff format

# Type checking
uv run ty check

# Run all quality checks (pre-commit)
uv run pre-commit run --all-files
```

### Testing

```bash
# Run all tests with coverage
uv run pytest

# Run single test file
uv run pytest tests/test_logger.py

# Run single test function
uv run pytest tests/test_logger.py::TestLoguruLogger::test_singleton_returns_same_instance

# Run tests with coverage report
uv run pytest --cov=de_projet_perso --cov-report=term-missing

# Run tests in verbose mode
uv run pytest -v
```

### Docker & Airflow

```bash
# Start services
docker compose up --detach

# View Airflow logs
docker compose logs airflow

# Stop services
docker compose down
```

## Code Style Guidelines

### Project Structure

- `src/de_projet_perso/`: Main package code
- `tests/`: Test files mirroring src structure
- `data/`: Data pipeline directories (landing/bronze/silver/gold)
- `airflow/dags/`: Airflow DAG definitions

### Import Organization

- Use `isort` (handled by ruff) for import sorting
- Group imports: standard library, third-party, local
- Use absolute imports for local modules: `from de_projet_perso.core.logger import logger`

### Type Hints

- Use type hints for all function parameters and return values
- Use `TYPE_CHECKING` for forward references
- Prefer specific types over generic `Any`
- Use `ParamSpec` and `TypeVar` for generic functions

### Naming Conventions

- **Variables/Functions**: `snake_case`
- **Classes**: `PascalCase`
- **Constants**: `UPPER_SNAKE_CASE`
- **Private methods**: `_leading_underscore`
- **Dunder methods**: `__dunder__`

### Documentation

- Use Google-style docstrings (configured in pyproject.toml)
- Include `Args:`, `Returns:`, `Raises:` sections
- Use `# noqa: D103` for simple test functions
- Document complex logic with inline comments

### Error Handling

- Use custom exceptions from `de_projet_perso.core.exceptions`
- Include context in error messages with `extra={}` for logging
- Use specific exception types, avoid bare `except:`
- Log errors with structured context: `logger.error("Failed", extra={"context": value})`

### Async/Await

- Use async/await for I/O operations (HTTP, file operations)
- Use `aiohttp` for HTTP requests with proper session management
- Use `aiofiles` for async file operations
- Use `ThreadPoolExecutor` for CPU-bound operations in async context

### Logging

- Use the pre-configured logger from `de_projet_perso.core.logger`
- Include structured context with `extra={}` parameter
- Use appropriate log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
- Use `logger.exception()` in exception handlers

### Testing

- Use `pytest` with fixtures for setup/teardown
- Use `@pytest.mark.parametrize` for data-driven tests
- Use `mocker` for mocking external dependencies
- Test both success and failure scenarios
- Use `autouse=True` fixtures for cross-cutting concerns

### Configuration

- Store configuration in `de_projet_perso.core.settings`
- Use environment variables for secrets and deployment-specific values
- Use Pydantic for configuration validation
- Keep sensitive data in `secrets/` directory (git-ignored)

### Data Pipeline Patterns

- Use `ExistingFileAction` enum for file handling strategies
- Validate file integrity (e.g., SQLite headers for GeoPackage files)
- Use atomic operations for file moves
- Include progress bars for long-running operations

### Airflow Integration

- Import DAG functions from `de_projet_perso.example` for testing
- Use the logger's Airflow-compatible mode in DAG contexts
- Handle both Airflow and terminal environments gracefully

## Dependencies

### Core Libraries

- `aiohttp`: Async HTTP client
- `aiofiles`: Async file operations
- `polars`: Data manipulation
- `duckdb`: Analytical database
- `loguru`: Structured logging
- `pydantic`: Data validation
- `py7zr`: 7z archive handling

### Development Tools

- `pytest`: Testing framework
- `ruff`: Linting and formatting
- `ty`: Static type checking
- `pre-commit`: Git hooks

## File Patterns

- **Python files**: `*.py`
- **Configuration**: `pyproject.toml`, `docker-compose.yaml`
- **Data**: `*.gpkg`, `*.7z`, `*.parquet`
- **Tests**: `test_*.py`
- **Documentation**: `*.md`

## Git Workflow

- Use conventional commit messages
- Run pre-commit hooks before committing
- Ensure all tests pass before pushing
- Keep PRs focused and well-documented