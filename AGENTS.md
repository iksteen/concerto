# Agent Instructions

## Project Defaults
- Language: Python
- Packaging and environment management: `uv`
- Concurrency model: prefer `asyncio` over threading
- Preferred HTTP framework: `fastapi`
- Preferred HTTP client: `aiohttp`

## Quality Checks
- Run linting and type checking with:
  - `uv run pre-commit run -a`
