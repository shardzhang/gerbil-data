# Contributing to gerbil-data

Thank you for your interest in contributing! We welcome contributions from everyone.

## How to Contribute

### Reporting Bugs

Before submitting a bug report, please check the existing issues to see if the problem has already been reported. When filing a bug report, include:

- A clear, descriptive title
- Steps to reproduce the issue
- Expected vs actual behavior
- Your environment (OS, Java version, Spark version, Scala version)
- Any relevant logs or error messages

### Suggesting Features

Feature suggestions are welcome! When suggesting a feature:

- Explain why the feature would be useful
- Describe how it should work
- If possible, outline how it might be implemented

### Pull Requests

1. **Fork** the repository and create your branch from `main`
2. **Install prerequisites** — Java 8+, Maven, Scala 2.12
3. **Make your changes** — Follow the existing code style and conventions
4. **Add or update tests** as needed
5. **Build and test** your changes:
   ```bash
   mvn clean package
   ```
6. **Commit** your changes — use a clear, descriptive message following [Conventional Commits](https://www.conventionalcommits.org/):
   ```
   <type>: <short description>
   
   [optional body]
   ```
   Types: `feat` (new feature), `fix` (bug fix), `refactor` (code change), `test` (tests), `docs` (documentation), `chore` (build/config). Examples:
   - `feat: add time-based train/val/test split`
   - `fix: handle null pointer in movie feature parsing`
   - `docs: update encoding spec for cross features`
7. **Push** to your fork and submit a pull request

### Development Setup

```bash
git clone https://github.com/shardzhang/gerbil-data.git
cd gerbil-data

# Enable commit message template
git config commit.template .gitmessage

# Enable commit-msg validation hook
git config core.hooksPath .githooks

# Build the project
mvn clean package -DskipTests
```

## Code Style

- Follow Scala style conventions used in the existing codebase
- Keep classes focused and methods concise
- Write meaningful comments for non-obvious logic
- Use descriptive variable and function names

## Testing

- Add tests for new functionality
- Ensure all existing tests pass before submitting a PR
- Tests are located in `src/test/scala/`

## Code of Conduct

Please note that this project is released with a [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you agree to abide by its terms.

## Questions?

Feel free to open an issue for any questions or discussions.
