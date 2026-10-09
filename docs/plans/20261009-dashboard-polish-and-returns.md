# Dashboard polish and asset returns

## Goal and scope

Refine the entire existing dashboard into a cohesive, calm financial workspace. Add current snapshot unit prices and replace the small holdings charts with 30-day and one-year price changes. Preserve login, account selection, filters, source grouping, source details, manual holdings editing, job links, and accessible dialogs. Do not write tests or deploy changes.

## Design direction

- Use a warm off-white page, white surfaces, dark ink text, muted secondary text, and a restrained forest green accent. Avoid decorative gradients, excessive shadows, and borders around every piece of content.
- Establish a consistent spacing and typography scale, tabular numbers, aligned numeric columns, clear heading hierarchy, and generous whitespace.
- Refine the header, login, portfolio overview, allocation summaries, holdings controls/table, empty and error states, and account/manual/history dialogs together.
- Use a small consistent inline SVG line icon vocabulary for actions; retain visible labels and accessible names. Decorative icons are hidden from assistive technology.
- Use subtle dividers and modest corner radii. Distinguish positive/negative changes with signs as well as color. Keep keyboard focus visible and text readable.
- On mobile, use intentionally laid out holding cards/rows with labeled metrics; avoid a cramped desktop table or horizontal page overflow.

## Asset metrics and semantics

1. Holdings columns: Asset, Price, 30 days, 1 year, Value, Weight, Sources. Remove inline sparklines and their redundant mobile controls. An optional explicit history action may retain the existing larger history dialog.
2. Price means latest snapshot unit price, not a live quote. Display its currency and make the snapshot/as-of context clear. Prefer a consistent base-currency unit price for grouped holdings; never average unlike currencies or derive a unit price from incompatible rows. Missing or conflicting comparable prices show an explanatory unavailable state. Show precise enough formatting for small asset prices.
3. Returns mean asset unit price change, not portfolio value change, and use the same displayed currency. Use adjusted history for known corporate actions. Never mix currencies or raw/adjusted endpoints.
4. Anchor each comparison to the snapshot date, with the latest comparable point on or before that date. Compare against 30 calendar days earlier and the same calendar date one year earlier (handle leap years).
5. Require historical coverage: select a baseline on or before the target, within a named small calendar-day tolerance for weekends/holidays (7 days). Require a recent endpoint within that tolerance as well. Never label a few days of history as a full year. Fetch enough days for the year plus the tolerance and inclusive endpoints.
6. Missing endpoints, zero/nonpositive baselines, nonfinite values, unavailable FX, incompatible series, request failures, and insufficient coverage yield an em dash with a concise accessible explanation. Loading is distinct from unavailable; never substitute zero.
7. Fetch history once per unique symbol/currency/window, cache it, bound concurrent requests, and prevent stale responses from updating a new account/session/render. No external historical price provider or invented backfill.
8. Source summary view retains source-level totals; asset-specific prices and returns belong in asset rows/details, not fabricated source metrics.

## Implementation sequence

1. Inspect existing frontend styles/rendering, valuation fields, history response/selection, route validation, and visual fixture tooling. Respect AGENTS.md and never read .env.
2. Make the smallest backend history-window extension needed for a full-year baseline with tolerance; keep route validation and loader constants aligned. Reuse existing adjusted/native/base series fields.
3. Implement clearly named frontend helpers for prices and date/return selection; integrate metric cells into grouped holdings and mobile rendering. Reuse authentication/cache mechanisms and guard asynchronous updates.
4. Apply the unified visual design across the page and dialogs, simplifying obsolete inline chart rendering where safe without unrelated refactors.
5. Run no test suites and write no tests, per user instruction. Use the existing Playwright visual capture workflow with an isolated synthetic local app, inspect desktop/mobile screenshots, and manually exercise filters/details/dialogs. Do not read production secrets. If tooling needs setup or approval, use the prescribed permission mechanism.
6. Review the diff for scope, calculation semantics, availability states, accessibility, and responsive layout. Record actual visual validation and limitations in this plan and final report. Do not commit, push, or deploy unless requested.

## Acceptance criteria

- Whole dashboard has consistent surfaces, icons, borders, spacing, and typography on desktop and mobile.
- Inline little charts are gone; asset rows show snapshot price, 30-day change, and one-year change.
- Short histories correctly show unavailable annual returns; missing prices/FX never produce misleading numbers.
- Existing account, holdings, filters, source details, and dialogs remain usable.
- No tests written or run; screenshot artifacts are reviewed when tooling is available.

## Implementation notes

- Implemented the warm neutral/forest palette, more spacious portfolio overview, quieter allocation tracks, consistent SVG action icons, refined holdings table, mobile metric cards, and matching login/dialog styling.
- Holdings now show snapshot base-currency unit price plus 30-day/year price changes; the symbol opens the retained full history dialog. Missing prices, conflicting available prices, unlike instrument families, insufficient historical coverage, and failed requests remain unavailable with accessible explanations. Missing source prices do not suppress a consistent available unit price.
- Expanded the shared backend history limit from 180 to 400 days. Annual requests include leap-year coverage, seven-day baseline tolerance, inclusive endpoints, and snapshot-age padding within that cap. Requests use the existing authenticated cache, four concurrent workers, and account/session/render guards.
- Return comparisons conservatively require the selected historical endpoint's raw base price to match the displayed snapshot price. This guards against symbols shared by incompatible instruments; it can also suppress a return when snapshot/history pricing or FX differs. Such cases show an explanatory unavailable state rather than an uncertain percentage.
- Inline JavaScript was parsed with Node and the diff was checked for whitespace. No tests were written or test suites run.
- Visual tooling required approved socket binding and temporary downloaded Chromium libraries because the system dependency installer required a sudo password. Captured all eight fresh login/authenticated/details/editor screenshots under `artifacts/visual-check/` and reviewed desktop/mobile authenticated views, the desktop editor, and mobile source details. Fixed the mobile nested details layout and reviewed a fresh capture showing readable quantities, prices, values, and statuses.
- Temporary CSV histories were added only to the isolated preview artifacts to inspect populated returns: BTC displayed -4.76% over 30 days and +33.33% over one year; GGAL displayed -11.11% over 30 days with its annual change unavailable. Zero and unpriced assets remained unavailable for returns. No test suites were run.
