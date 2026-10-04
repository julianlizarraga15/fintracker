# Dashboard redesign

Date: 2026-10-04
Status: Implemented and reviewed; 2026-10-04

Review: 2026-10-04; clarified data semantics, editor scope, fixtures, and visual review milestones.

## Objective

Make Fintracker a comfortable daily portfolio tool: show portfolio value and composition immediately, make holdings easy to inspect, and put account administration and manual editing behind deliberate actions.

The current valuation, source grouping, price history, and manual crypto workflows remain the foundation. Success means a clearer working product, with observable improvements to navigation and data interpretation.

## Current problems and evidence

The supplied screenshots and `frontend/index.html` show:

- The header, login form, and account form occupy most of the first screen. Portfolio value receives less attention than account metadata.
- The login form stays visible after authentication.
- Manual crypto editing precedes the positions table, interrupting the main reading workflow.
- Gradients, button glows, translucent panels, large corner radii, and heavy shadows give routine controls too much visual weight.
- Holdings rows are tall, source lists consume substantial width, and numeric columns need stronger alignment.
- Sparklines lack a persistent period label and sit on contrasting black rectangles.
- The selected account is already saved in local storage. Login can already load a detected or entered account. Restoring a valid session does not automatically load its saved account snapshot.
- Grouped “Unit price” is calculated as total value divided by total quantity. Its meaning depends on whether the grouped rows have compatible instrument units.

## Design decision

Use a quiet light workspace as the proposed default: warm white background, charcoal text, fine dividers, and one restrained blue accent. Keep the information density of a practical financial table.

This follows the recommended direction from the design discussion; the user has requested a plan, not yet selected a visual implementation. A preference for flat charcoal dark mode can change the color tokens without changing the page structure. Do not build three competing themes or add a theme switcher in the first release.

### Visual rules

- Define named tokens for colors, spacing, typography, corner radii, and control heights.
- Use a system sans-serif stack and tabular numerals for financial values. Reserve monospace for technical identifiers.
- Make portfolio value the largest number. Section headings, table headers, and metadata should form a clear descending hierarchy.
- Use solid surfaces and restrained borders. Remove decorative gradients, backdrop blur, glowing buttons, and large panel shadows.
- Use a small radius for inputs and buttons. Avoid wrapping every section in a rounded card.
- Use color to communicate selection, focus, and meaningful status. Include text or icons so color is not the sole signal.
- Keep charts subdued and directly labeled. Use horizontal allocation bars with values and percentages.

## Page structure

```text
Fintracker                 Overview / Holdings             Account / Manage

Portfolio value                                            As of [date]
[value in snapshot currency]                               Refresh
[position count] · [valuation completeness]

Allocation by instrument type          Allocation by source
[labeled horizontal bars]              [labeled horizontal bars]

Holdings                   Search / Filters / Manage holdings
Position / Source

Asset | Price trend · [period] | Value | Weight | Sources
[expandable rows with quantity, price, and source breakdown]
```

Overview and Holdings can be section links on the same page. Keep the existing source view within Holdings. A separate Job health link belongs in the account or utility menu. Do not add a sidebar for this small navigation footprint.

### Authentication and account controls

- Show a compact sign-in screen when unauthenticated. Use “Sign in” rather than “Demo login.”
- After authentication, show the portfolio workspace and an account menu with sign out.
- Automatically load the saved account when restoring a valid session. Preserve the existing login account detection behavior.
- Provide an account editing control using the existing account ID workflow. Do not imply an account directory exists; a friendly selector requires a real list of accessible accounts.
- Put the raw account ID and computed timestamp in snapshot details. Keep the snapshot date visible beside the total.
- On expiration, clear protected dashboard data and return to sign-in with a useful message. Prevent an in-flight request from restoring data after sign out or account switching.
- Apply request cancellation or session/account generation checks to snapshots, manual holdings, and price-history dialogs. A late response or 401 from an obsolete session must not overwrite or invalidate the current session.
- On a failed same-account refresh, retain the last successful snapshot with its date and a visible refresh error. On account switching, clear the previous snapshot before loading; a failed switch must show an empty error state for the requested account.

### Overview and data quality

- Display portfolio total in the snapshot base currency; use a currency label rather than a conversion selector until conversion is supported.
- Distinguish unique displayed assets from underlying position rows. `totals.positions` counts snapshot rows; `totals.ok_positions` counts rows with status `ok`. Show this as a row-status count, not a percentage of portfolio value covered.
- Label the first allocation “By instrument type.” Existing categories such as `cedear`, `fci`, and `other` do not establish economic exposure classes. Use an Unknown bucket for missing types; defer inferred equity/bond/geographic classifications.
- Prefer the API's `source_allocations` for source totals. Derive instrument-type allocations from finite `rows[].value_base` values. If source allocations are absent, derive them from those same rows and label missing sources Unknown. Never add summary values to row values.
- Use `totals.total_value_base` as the denominator across allocations and Holdings. It sums known row values; a chart totaling 100% does not establish that every holding is valued. Show a separate missing-value/status warning, with counts, without guessing missing monetary value.
- Preserve missing versus zero values: an all-unvalued asset displays unavailable value and weight; a partially valued asset displays its known subtotal with an incomplete marker. Zero or nonpositive portfolio totals have unavailable percentage weights; use text values rather than misleading allocation bars for unsupported totals.
- Show snapshot date and refresh state. Refresh means fetching the latest available snapshot, not running ingestion or requesting live market prices.
- Keep freshness wording factual in the first release. Automated stale warnings need a defined ingestion schedule, timezone, and market-calendar policy.

### Holdings

- Default to descending portfolio value. Add search and filters for asset type and source, with an obvious reset action and empty-result state.
- Make Asset, Value, and Weight sortable. Use accessible header buttons and `aria-sort`.
- Filtering must not change the denominator of portfolio weights. Show matching position count and subtotal separately when useful.
- In Position view, search/filter selects complete asset groups: a matching source or type includes the asset's full portfolio value and source breakdown. State this behavior in filter help. In Source view, filters select source groups based on their underlying rows and preserve each group's full total. Combine filters with AND and search symbol/source text case-insensitively. Reset clears filters and search; switching views preserves them.
- Sort missing values last in both directions and break ties by asset/source name. Apply sorting to the selected view; Source view uses Source, Value, and Weight headers.
- Right-align financial columns and use consistent precision per field. Preserve meaningful fractional quantities.
- Show a source count for multiple sources and a name for a single source. Expand a row to show full source-level quantities, prices, and values.
- Move quantity and unit price into expanded details to reduce default width. Identify asset names when already available; do not invent missing names.
- Label the actual price-history window. Preserve chart expansion and native/adjusted series controls. A missing chart must not hide a holding or block the table.
- Use explicit expansion and chart controls, with clear accessible names. Avoid making one ambiguous row click trigger several actions.
- Do not expose aggregate quantity or implied aggregate price in this release. The response has no explicit instrument-unit identity sufficient to verify compatibility. Show quantity and supplied unit prices per source row in details, including details for single-source assets. Retain existing symbol grouping for portfolio values and defer changes to instrument identity/grouping to a separate correctness change.

### Manual holdings

- Open the existing manual crypto editor through “Manage holdings” in a dedicated dialog or section.
- Preserve load, add, remove, validation, save, and persistence behavior.
- The manual API reads and replaces one configured holdings file; it is not scoped by the selected dashboard account. Label the editor “Manual crypto holdings,” preserve every loaded entry and its optional account ID, and never save only a filtered subset as the full file. Account-specific editing requires a separately scoped API change.
- Saving writes the holdings file and does not recompute valuations. Show “Saved. Portfolio values update after the next valuation run.” Refreshing the latest snapshot must not suggest that a valuation run has been triggered.
- Warn before discarding unsaved changes. Restore focus to the opener on close and support keyboard navigation.
- Keep edits on validation/save failures. Handle editor close, reload, and account navigation consistently; require an explicit discard action before replacing dirty edits. Disable duplicate saves while a request is pending.

### Mobile and accessibility

- At narrow widths, show asset, value, and weight first. Reveal sources, quantity, and price through expansion.
- Stack allocation sections and wrap toolbar controls without page-level horizontal scrolling.
- Support keyboard access, visible focus, labeled fields, live error messages, dialog focus handling, and adequate contrast.
- Respect reduced motion. Loading placeholders should not resemble actual financial values.

## Implementation phases

### 1. Establish the baseline and data contracts

- Review `frontend/index.html`, `backend/app/valuations.py`, history responses, and manual holdings save semantics.
- Capture authenticated desktop and mobile screenshots using reproducible fixture data. Include enough holdings and sources to expose density problems.
- Document missing valuations, mixed instrument units, and response fields needed for allocation.
- Confirm the proposed palette before visual implementation; the remaining structure does not depend on it.
- Prepare a static desktop/mobile layout using representative fixture data and the proposed tokens. Review actual screenshots for hierarchy, density, and personality before adding interactive behavior. Keep this as a local review artifact rather than a second production dashboard.

Deliverable: baseline screenshots, reviewed static layouts, confirmed data mappings, and the selected visual tokens.

### 2. Replace the shell and fix session navigation

- Apply the visual tokens, compact header, sign-in state, portfolio summary, and snapshot details.
- Restore and load a valid saved account automatically; add sign out and safe account switching.
- Move manual editing behind its entry point while retaining existing API calls.

Deliverable: a usable workspace with portfolio information visible on entry and existing workflows accessible.

### 3. Improve holdings and composition

- Implement compact rows, numeric alignment, sorting, search, filters, source expansion, and labeled price trends.
- Add allocation bars from existing snapshot values and explicit incomplete-data states.
- Complete responsive layouts and keyboard interactions.

Deliverable: an inspectable portfolio on desktop and mobile, with transparent aggregation semantics.

### 4. Verify behavior and visual quality

Testing criteria for every implementation phase:

- Prefer integration and E2E tests that verify observable behavior. For complex features, cover realistic scenarios and failure paths with reproducible, inspectable artifacts.
- Avoid tests that duplicate implementation logic, test their own mocks, or fail merely because implementation details changed.
- Add tests only to close meaningful behavior coverage gaps, including for bug fixes. Review existing coverage before adding cases; the scenarios below are coverage requirements, not a requirement for a separate test per scenario.
- Extend existing tests and combine redundant cases when it preserves clarity.
- Before testing a component in isolation, identify its failure modes and design tests around them.

Verification work:

- Extend `e2e/dashboard.spec.js` and `scripts/e2e_backend.py` for meaningful behavior gaps. The latter creates the synthetic snapshot and runs the real API; `scripts/e2e_server.js` serves/proxies it and needs changes only if asset serving requires them. Preserve real API persistence checks.
- Expand fixtures beyond the current single BTC row: multiple sources for one symbol, differing instrument types, zero and missing values, partial valuation, fractional quantities, long labels, a second account, and deterministic price history. Verify a missing snapshot through the API's 404 behavior; test a successful empty payload separately if retained as a defensive UI state.
- Cover successful and failed login, automatic restoration, expiration, sign out, account switching, failed refresh, and empty snapshots.
- Verify sorting and filtering against fixture values, unchanged portfolio weights, source expansion, and unavailable history.
- Verify manual editing survives save/reload and unsaved edits receive appropriate handling.
- Verify delayed snapshot/history/editor responses and obsolete 401s after sign out, new login, or account switching. Verify that saving preserves entries belonging to other account IDs in the shared manual file.
- Run `npm run test:e2e` for implementation changes and inspect failures through retained traces/screenshots.
- Run `npm run visual:check:local` against the local app and review desktop/mobile artifacts under `artifacts/visual-check/`.
- The current visual script captures only the initial page. Extend it or provide an additional fixture-backed capture path so the authenticated dashboard, expanded details, and editor are actually reviewed. Use test credentials; never read `.env`.
- If backend behavior changes become necessary, add a focused failing pytest first and run `scripts/run_tests.sh` when practical.

Deliverable: reviewed screenshots, passing relevant behavior checks, and a short record of any remaining limitations. No deployment is included in this plan.

## Acceptance criteria

- At 1440 × 900, the authenticated page shows portfolio total, snapshot date, composition, and the start of Holdings without scrolling through forms or the manual editor.
- At 390 × 844, portfolio value and date are visible immediately; asset/value/weight remain readable without page-level horizontal scrolling.
- A valid restored session loads its saved account automatically. Expiration and sign out clear protected content.
- Account changes cannot leave the previous account's total, rows, or pending responses presented as the new account.
- Existing source grouping, price-history inspection, and manual holdings persistence remain available.
- Search, sorting, and filters produce correct results and preserve portfolio-weight semantics.
- Incomplete valuations, failed refreshes, and unavailable histories are visibly distinct from zero values and successful loads.
- Numerical aggregates reconcile with fixture totals or explain their coverage differences.
- All-unvalued assets are never displayed as zero-value assets; missing valuations remain visible even when allocations sum to 100% of known value.
- Position/source filters preserve full group values and portfolio weights. Counts explicitly distinguish unique assets from snapshot rows.
- Failed refresh preserves a clearly dated same-account snapshot; failed account switching never reveals the previous account's data under the requested account.
- Manual editing preserves the full shared holdings file and reports that saving does not update the existing valuation snapshot.
- Desktop/mobile screenshots show restrained surfaces, consistent alignment, legible type, and no decorative glow or gradient treatments.

## Scope and later work

Primary changes belong in `frontend/index.html`, with focused updates to existing E2E fixtures and visual capture tooling. Extract small styles/scripts only when that directly improves maintainability of the touched code. A framework migration or broad frontend rewrite is unnecessary for this scope.

Defer portfolio value history, investment returns, benchmark comparison, cost-basis P&L, live currency conversion, target allocation/rebalancing, and automatic freshness policy. These require additional data or agreed semantics. Portfolio value changes must not be labeled investment returns without accounting for cash flows.

## Progress checklist

- [x] Use the recommended light direction and confirm snapshot/editor data contracts.
- [x] Capture authenticated fixture-backed desktop/mobile screenshots and review the implemented layout.
- [x] Implement shell, session states, and summary.
- [x] Move manual editing behind Manage holdings.
- [x] Implement holdings interactions and allocation.
- [x] Complete mobile and accessibility behavior.
- [x] Run relevant behavior checks and review visual artifacts.
- [x] Record final outcomes and limitations.

## Implementation review

The light workspace is the default. Final authenticated captures, expanded source details, and the manual editor are saved under `artifacts/visual-check/` for desktop and mobile. A separate static-only prototype and pre-change screenshots were not retained; the final fixture-backed screenshots were used for visual review.

The dashboard E2E suite passes all 10 desktop and mobile cases. The valuation response regression tests pass (4 tests), including missing text fields. The complete backend suite was attempted but did not finish after reaching `test_http_workflows.py`; that file passes independently (5 tests). No deployment was performed.
