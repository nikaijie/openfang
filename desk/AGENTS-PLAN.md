# Desk Agent Plan (MVP)

## Goal
Build a practical multi-agent workflow for Desk ad operations:
- daily data ingestion
- daily analysis report
- creative operation support
- campaign operation support

## Agent List (4 Total)

### 1) `data-fetch-agent` (Done)
Purpose:
- Pull yesterday's ad data from internal portal APIs
- Save raw responses, normalized data, and ingestion logs

Input:
- `/Users/algorix/Documents/project/openfang/desk/config/ad-sources.json`

Output:
- `/Users/algorix/Documents/project/openfang/desk/data/data-fetch/<YYYY-MM-DD_HH-MM>/raw/...`
- `/Users/algorix/Documents/project/openfang/desk/data/data-fetch/<YYYY-MM-DD_HH-MM>/normalized/<YYYY-MM-DD>.json`
- `/Users/algorix/Documents/project/openfang/desk/data/data-fetch/<YYYY-MM-DD_HH-MM>/logs/ingestion-<YYYY-MM-DD>.md`

---

### 2) `analysis-report-agent` (Next)
Purpose:
- Generate daily performance report and optimization suggestions

Input:
- latest normalized file from `data-fetch-agent`

Output:
- `/Users/algorix/Documents/project/openfang/desk/reports/daily/<YYYY-MM-DD>.md`
- `/Users/algorix/Documents/project/openfang/desk/reports/daily/<YYYY-MM-DD>-actions.json`

Core metrics:
- spend, impressions, clicks, CTR, CVR, CPC, CPA, ROAS

---

### 3) `creative-ops-agent`
Purpose:
- Manage creative-side operations (organization, naming, refresh suggestions)

Input:
- daily report + performance signals by creative/campaign

Output:
- `/Users/algorix/Documents/project/openfang/desk/reports/creative/<YYYY-MM-DD>.md`

Typical tasks:
- low-performing creative detection
- new creative direction suggestions
- creative test backlog

---

### 4) `campaign-ops-agent`
Purpose:
- Produce campaign operation checklist (execution-ready but manual-safe)

Input:
- daily report + action suggestions + campaign-level metrics

Output:
- `/Users/algorix/Documents/project/openfang/desk/reports/campaign/<YYYY-MM-DD>.md`

Typical tasks:
- budget shift recommendations
- audience/placement adjustment suggestions
- pause/scale candidate list

## Workflow (MVP)
1. `data-fetch-agent` runs first (daily).
2. `analysis-report-agent` reads newest normalized output and writes report.
3. `creative-ops-agent` generates creative actions.
4. `campaign-ops-agent` generates campaign actions.

Current workflow definition file:
- `/Users/algorix/Documents/project/openfang/desk/workflows/data-fetch-and-analysis.json`

## Safety Rules
- Agents can suggest actions but must not directly modify live campaign settings in MVP.
- If data is missing, produce a blocker report instead of guessing.
- Keep all outputs timestamped and traceable.

## Acceptance Criteria (MVP)
- Daily run creates one full output set across data + reports.
- Failures are logged with clear reasons.
- Suggestions are concrete enough for manual execution.

## Current Status
- `data-fetch-agent`: completed and runnable
- `analysis-report-agent`: completed and runnable
- `creative-ops-agent`: pending
- `campaign-ops-agent`: pending
