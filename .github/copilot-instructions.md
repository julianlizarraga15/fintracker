# General preferences
- Keep changes minimal: avoid touching unrelated lines.
- Favor simple, clear code over fancy optimizations.
- Don’t refactor unless explicitly asked or it’s clearly beneficial.
- Don’t use magic numbers; assign them to well-named constants.

# Naming conventions
- Use descriptive names for variables, functions, classes, etc.
- Use **snake_case** for functions and methods (e.g. `calculate_total`).
- Use **PascalCase** for class names.
- All code and comments in English.

# Design & structure
- Prefer separation of concerns; use OOP when it makes sense — don’t overdo it.
- Comments should explain context, trade-offs, and rationale, but keep them minimal.
- If you add a class or aggregation, consider domain logic: e.g.  
  - A **Product** aggregates **Items** or **UserProducts**  
  - A **UserProduct** aggregates Items for a given seller with pricing / discount rules  
  - A Product can have multiple UserProducts (from different sellers)  
  - A Product can have multiple Items (from different sellers)

# Project tooling / environment
- Use **Poetry** for environment & package management.
- Use **pytest** for testing.  
  - Test files: `test_*.py`  
  - Test functions: `test_<behavior>()`  
  - Tests should be small and focused (one behavior per test)  
  - Use the **Arrange → Act → Assert** pattern  
  - Mock as little as possible — only external dependencies (network, DB, file system, randomness)

# Constraints & defaults
- Always preserve existing style unless asked otherwise.
- Don’t introduce new dependencies without justification.
- If uncertain, ask first rather than assuming major changes.

# Code execution
- Run the code in the virtual environment with `.venv/bin/python <script>` to ensure compatibility.
