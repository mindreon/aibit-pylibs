# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `aibit-pylibs`, a collection of Python utilities for data versioning workflows. The package provides integrated tools for:

- **Git repository management** (`GitRepoUtils`) - Complete Git operations including commits, tags, remotes, and branching
- **DVC (Data Version Control) integration** (`DvcUtils`) - Dataset initialization, versioning, and S3-backed storage 
- **Gitea provider** (`GiteaProvider`) - Organization and repository management via Gitea API
- **File utilities** (`FileUtils`) - Archive extraction supporting zip, tar, rar, 7z formats
- **Structured logging** (`get_logger`) - JSON logging with correlation IDs and application context
- **Retry mechanisms** (`retry_with_backoff`, `CircuitBreaker`) - Resilient external service operations

## Architecture

The codebase follows a utility-based architecture where each module provides a specific domain of functionality:

```
src/aibit_pylibs/
├── __init__.py          # Public API exports
├── dvc_utils.py         # DVC workflow orchestration
├── git_utils.py         # Git operations wrapper
├── gitea_provider.py    # Gitea API client
├── file_util.py         # File handling and schemas
├── logging.py           # Centralized logging setup
└── retry.py             # Retry patterns and circuit breaker
```

### Key Integration Patterns

1. **DVC + Git + S3 Workflow**: `DvcUtils` orchestrates complete dataset versioning by combining Git repository management, DVC tracking, and S3 storage in a single workflow.

2. **Async HTTP Operations**: `GiteaProvider` uses `httpx.AsyncClient` for all API operations with retry decorators. When working with this class, maintain async/await patterns and ensure proper client cleanup with `await close()`.

3. **Pydantic Data Models**: File browser functionality uses Pydantic models (`FileTreeNode`, `FileTree`) for type-safe data structures with automatic validation.

4. **Structured Logging**: All modules use `get_logger(__name__)` which provides structured JSON logging with automatic service context. Use `bind_context()` and `clear_context()` for request correlation.

5. **Resilience Patterns**: External service calls use `@retry_with_backoff()` decorator with configurable exponential backoff and circuit breaker protection.

## Development Commands

### Package Building
```bash
# Install in development mode
pip install -e .

# Build distribution
python -m build
```

### Dependencies
The project uses core dependencies:
- `gitpython>=3.1.45` - Git operations
- `httpx>=0.28.1` - HTTP client for Gitea API
- `dvc>=3.61.0` + `dvc-s3>=3.2.2` - Data version control
- `structlog>=25.4.0` - Structured logging
- `pydantic>=2.11.7` - Data validation
- Archive support: `rarfile>=4.0`, `py7zr>=0.20.0`

### Publishing
GitHub Actions handle automated publishing:
- **Development builds**: Triggered on `main` branch pushes, published to GitHub Pages
- **Release builds**: Triggered on `v*` tags, creates GitHub releases and optionally publishes to PyPI

## Code Conventions

1. **Chinese Comments**: Git utility classes contain Chinese documentation comments - maintain this pattern for consistency
2. **Async Context Management**: Always use `async with` patterns for `GiteaProvider` operations
3. **Error Handling**: Use structured logging for errors rather than print statements
4. **Type Hints**: All new code should include comprehensive type annotations
5. **Archive Security**: File extraction includes safety validation against path traversal and zip bombs

## Working with Modified Files

Current repository has modifications to:
- `src/aibit_pylibs/__init__.py:1` - Package exports including retry utilities
- `src/aibit_pylibs/dvc_utils.py:1` - DVC workflow implementation  
- `src/aibit_pylibs/file_util.py:1` - File utilities with Pydantic models
- `src/aibit_pylibs/git_utils.py:1` - Git operations with Chinese documentation

When making changes, ensure compatibility with the existing async patterns and maintain the integrated workflow approach.