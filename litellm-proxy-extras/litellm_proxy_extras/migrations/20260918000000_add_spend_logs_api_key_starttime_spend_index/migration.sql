-- DropIndex
DROP INDEX IF EXISTS "LiteLLM_SpendLogs_api_key_startTime_idx";

-- CreateIndex
-- "spend" trails the key columns so the budget-window aggregate in
-- budget_window_spend_writer.py plans as an index-only scan; api_key correlates
-- at ~0 in the heap, so without it the planner reads a page per matched row.
CREATE INDEX IF NOT EXISTS "LiteLLM_SpendLogs_api_key_startTime_spend_idx" ON "LiteLLM_SpendLogs"("api_key", "startTime", "spend");
