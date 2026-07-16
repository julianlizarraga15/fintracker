# General preferences
- NEVER inspect or read the contents of the `.env` file. This is a strict security rule.
- Keep changes minimal: avoid touching unrelated lines.
- Favor simple, clear code over fancy optimizations.
- Don’t refactor unless explicitly asked or it’s clearly beneficial.
- Don’t use magic numbers; assign them to well-named constants.
- Be brutally honest when answering a question or providing a recommendation.

# Design & structure
- Prefer separation of concerns; use OOP when it makes sense — don’t overdo it.
- Comments should explain context, trade-offs, and rationale, but keep them minimal. Don't add unnecessary comments.

# Testing & TDD
- For backend financial logic, parsers, transforms, auth behavior, job state, and bug fixes, prefer TDD: add or update a focused failing pytest first, then implement the smallest change needed to pass it.
- Run `scripts/run_tests.sh` before finishing backend behavior changes when practical.
- Don’t force strict TDD for exploratory work, live API scripts, deployment plumbing, or visual frontend polish; use targeted tests where they add real confidence.

# Visual frontend checks
- Playwright is available as the default lightweight visual check for frontend changes.
- In a fresh environment, install Node dependencies with `npm install`, then install Chromium with `npx playwright install --with-deps chromium` (or `npm run playwright:install`).
- Before or after frontend UI changes, start the app locally (for example `docker compose up -d frontend` or the full stack if API data is needed), then run `npm run visual:check:local` from the repo root.
- To target a different running app, use `APP_URL=<url> npm run visual:check`.
- The visual check captures desktop and mobile screenshots under `artifacts/visual-check/`; review them before finishing perceptible frontend changes.
