/**
 * Node/Databricks port of POC_prism/metrics/guardrail_query.py's compute().
 *
 * Faithful port, including known-deferred quirks (not fixed here, per the
 * team's explicit decision to track them as open items, not silently patch
 * them during this port):
 *   - EXCLUDED_GUARDRAILS membership check in the early-return path is
 *     case-sensitive, while the SQL-level filter for a named guardrail uses
 *     LOWER() on both sides (same inconsistency as the Python original).
 *   - currentUsage/threshold are coerced with Math.trunc (int()-equivalent),
 *     which would silently truncate a fractional value if one ever occurs
 *     (flagged, unverified-if-real, in doc 08 Part 4 - not fixed here).
 *   - No sandbox-typo detection (never implemented in the Python original
 *     either - tracked as an open item there, not in scope for this port).
 *
 * Differences from the Python original, required because this targets the
 * LIVE Databricks table instead of the local DuckDB snapshot (confirmed via
 * doc 08 Part 4's live-connection spike):
 *   - No `source_table` filter - that column does not exist on the live
 *     table at all (DuckDB-local-only artifact from merging pre-refactor
 *     CSVs).
 *   - auditDate/lastRefresh are live STRING columns, not TIMESTAMP - every
 *     comparison/aggregate wraps auditDate in CAST(auditDate AS TIMESTAMP).
 *   - Databricks interval syntax is `INTERVAL N DAYS` (bare integer + bare
 *     unit), not DuckDB's `INTERVAL 'N days'` (quoted string).
 */

const { runQuery } = require('./databricksClient')

const TABLE = 'dpaas_uccatalog_prd.aoaitgot.aep_guardrail_usage'
const EXCLUDED_GUARDRAILS = ['Edge Segmentation', 'Live Inbound Actions']

function statusFromPct (pct) {
    if (pct === null || pct === undefined || Number.isNaN(pct)) return 'unknown'
    if (pct >= 100) return 'red'
    if (pct >= 80) return 'yellow'
    return 'green'
}

/** Validates a value is a safe non-negative integer before it's interpolated
 * as an INTERVAL literal (Databricks requires a bare integer there, no bind
 * parameter form) - mirrors the safety the Python original got for free via
 * `int(days)` raising before the f-string was ever built. */
function assertSafeIntervalDays (value, label) {
    const n = Number(value)
    if (!Number.isInteger(n) || n < 0) {
        throw new Error(`${label} must be a non-negative integer, got: ${value}`)
    }
    return n
}

/**
 * Builds the guardrailName filter clause + its named parameters.
 * null -> exclude EXCLUDED_GUARDRAILS (case-sensitive NOT IN, matching the
 *   Python original's un-lowered filter for this mode).
 * array -> case-insensitive IN list (comparison mode).
 * string -> case-insensitive single match.
 */
function buildGuardrailFilter (guardrailName) {
    if (guardrailName === null || guardrailName === undefined) {
        const params = {}
        const placeholders = EXCLUDED_GUARDRAILS.map((name, i) => {
            const key = `excl${i}`
            params[key] = name
            return `:${key}`
        }).join(', ')
        return { clause: `AND guardrailName NOT IN (${placeholders})`, params }
    }
    if (Array.isArray(guardrailName)) {
        const params = {}
        const placeholders = guardrailName.map((name, i) => {
            const key = `gr${i}`
            params[key] = name
            return `LOWER(:${key})`
        }).join(', ')
        return { clause: `AND LOWER(guardrailName) IN (${placeholders})`, params }
    }
    return { clause: 'AND LOWER(guardrailName) = LOWER(:gr0)', params: { gr0: guardrailName } }
}

async function queryLatestSnapshot (credentials, sandbox, guardrailName) {
    const { clause, params } = buildGuardrailFilter(guardrailName)
    const sql = `
        WITH latest_rows AS (
            SELECT
                guardrailName, category, guardrailType, currentUsage, threshold, auditDate,
                ROW_NUMBER() OVER (PARTITION BY guardrailName, sandbox ORDER BY CAST(auditDate AS TIMESTAMP) DESC) AS rn
            FROM ${TABLE}
            WHERE sandbox = :sandbox
              AND lastRefresh IS NOT NULL
              ${clause}
        )
        SELECT guardrailName, category, guardrailType, currentUsage, threshold, auditDate,
            CASE WHEN threshold IS NULL OR threshold = 0 THEN NULL
                 ELSE ROUND((currentUsage * 100.0) / threshold, 1) END AS pct_of_threshold
        FROM latest_rows WHERE rn = 1
        ORDER BY pct_of_threshold DESC NULLS LAST
    `
    return runQuery(credentials, sql, { sandbox, ...params })
}

async function queryWindowed (credentials, sandbox, days, guardrailName) {
    const safeDays = assertSafeIntervalDays(days, 'window_days')
    const { clause, params } = buildGuardrailFilter(guardrailName)
    const sql = `
        WITH windowed AS (
            SELECT guardrailName, currentUsage, threshold, auditDate,
                CASE WHEN threshold IS NULL OR threshold = 0 THEN NULL
                     ELSE ROUND((currentUsage * 100.0) / threshold, 1) END AS pct_of_threshold
            FROM ${TABLE}
            WHERE sandbox = :sandbox
              AND lastRefresh IS NOT NULL
              AND CAST(auditDate AS TIMESTAMP) >= (SELECT MAX(CAST(auditDate AS TIMESTAMP)) FROM ${TABLE} WHERE sandbox = :sandbox) - INTERVAL ${safeDays} DAYS
              ${clause}
        )
        SELECT guardrailName, COUNT(*) AS total_readings,
            SUM(CASE WHEN pct_of_threshold >= 100 THEN 1 ELSE 0 END) AS breach_readings,
            MAX(pct_of_threshold) AS max_pct,
            MIN(CASE WHEN pct_of_threshold >= 100 THEN auditDate END) AS first_breach_at,
            MAX(CASE WHEN pct_of_threshold >= 100 THEN auditDate END) AS last_breach_at
        FROM windowed GROUP BY guardrailName
        ORDER BY max_pct DESC NULLS LAST
    `
    return runQuery(credentials, sql, { sandbox, ...params })
}

async function queryNewlyBreaching (credentials, sandbox, daysAgo) {
    const safeDaysAgo = assertSafeIntervalDays(daysAgo, 'compare_to_days_ago')
    const plus1 = safeDaysAgo + 1
    const minus1 = Math.max(0, safeDaysAgo - 1)
    const { clause, params } = buildGuardrailFilter(null) // always excludes EXCLUDED_GUARDRAILS here, matching the Python original
    const sql = `
        WITH latest_current AS (
            SELECT
                guardrailName, category, guardrailType, currentUsage, threshold, auditDate,
                ROW_NUMBER() OVER (PARTITION BY guardrailName ORDER BY CAST(auditDate AS TIMESTAMP) DESC) AS rn
            FROM ${TABLE}
            WHERE sandbox = :sandbox
              AND lastRefresh IS NOT NULL
              AND CAST(auditDate AS TIMESTAMP) >= (SELECT MAX(CAST(auditDate AS TIMESTAMP)) FROM ${TABLE} WHERE sandbox = :sandbox) - INTERVAL 1 DAYS
              ${clause}
        ),
        latest_historical AS (
            SELECT
                guardrailName,
                currentUsage AS usage_days_ago,
                threshold AS threshold_days_ago,
                ROW_NUMBER() OVER (PARTITION BY guardrailName ORDER BY CAST(auditDate AS TIMESTAMP) DESC) AS rn
            FROM ${TABLE}
            WHERE sandbox = :sandbox
              AND lastRefresh IS NOT NULL
              AND CAST(auditDate AS TIMESTAMP) BETWEEN
                  (SELECT MAX(CAST(auditDate AS TIMESTAMP)) FROM ${TABLE} WHERE sandbox = :sandbox) - INTERVAL ${plus1} DAYS
                  AND (SELECT MAX(CAST(auditDate AS TIMESTAMP)) FROM ${TABLE} WHERE sandbox = :sandbox) - INTERVAL ${minus1} DAYS
        )
        SELECT
            lc.guardrailName, lc.category, lc.guardrailType, lc.currentUsage, lc.threshold, lc.auditDate,
            ROUND(((lc.currentUsage - lc.threshold) * 100.0 / lc.threshold), 1) AS pct_over_threshold,
            COALESCE(lh.usage_days_ago, 0) AS usage_days_ago,
            lc.currentUsage - COALESCE(lh.usage_days_ago, 0) AS usage_increase
        FROM latest_current lc
        LEFT JOIN latest_historical lh
            ON lc.guardrailName = lh.guardrailName AND lh.rn = 1
        WHERE lc.rn = 1
          AND lc.currentUsage > lc.threshold
          AND COALESCE(lh.usage_days_ago, 0) <= COALESCE(lh.threshold_days_ago, lc.threshold)
        ORDER BY pct_over_threshold DESC
    `
    return runQuery(credentials, sql, { sandbox, ...params })
}

/**
 * @param {{host:string, path:string, token:string}} credentials - Databricks connection info
 * @param {object} options
 * @param {string} options.sandbox
 * @param {string|string[]|null} [options.guardrailName]
 * @param {number|null} [options.windowDays]
 * @param {string} [options.sortBy] - 'pct' or 'breach_count'
 * @param {number|null} [options.topN]
 * @param {number|null} [options.compareToDaysAgo]
 */
async function compute (credentials, options) {
    const {
        sandbox, guardrailName = null, windowDays = null,
        sortBy = 'pct', topN = null, compareToDaysAgo = null
    } = options

    if (typeof guardrailName === 'string' && EXCLUDED_GUARDRAILS.includes(guardrailName)) {
        return {
            sandbox, guardrail_name: guardrailName, found: false,
            message: `${guardrailName} is not yet available in production - its guardrail metric hasn't been enabled.`
        }
    }

    if (compareToDaysAgo !== null && compareToDaysAgo !== undefined) {
        const rows = await queryNewlyBreaching(credentials, sandbox, compareToDaysAgo)
        const newlyBreaching = rows.map(r => ({
            guardrail_name: r.guardrailName,
            category: r.category,
            guardrail_type: r.guardrailType,
            current_usage: Math.trunc(Number(r.currentUsage)),
            threshold: Math.trunc(Number(r.threshold)),
            pct_over_threshold: Number(r.pct_over_threshold),
            usage_days_ago: Math.trunc(Number(r.usage_days_ago)),
            usage_increase: Math.trunc(Number(r.usage_increase))
        }))
        return {
            sandbox, compare_to_days_ago: Math.trunc(Number(compareToDaysAgo)),
            newly_breaching_count: newlyBreaching.length,
            newly_breaching: newlyBreaching
        }
    }

    const rows = windowDays === null
        ? await queryLatestSnapshot(credentials, sandbox, guardrailName)
        : await queryWindowed(credentials, sandbox, windowDays, guardrailName)

    if (rows.length === 0) {
        return { sandbox, guardrail_name: guardrailName, window_days: windowDays, found: false, guardrails: [] }
    }

    if (typeof guardrailName === 'string') {
        const row = rows[0]
        if (windowDays === null) {
            const pct = row.pct_of_threshold === null || row.pct_of_threshold === undefined
                ? null : Number(row.pct_of_threshold)
            return {
                sandbox, guardrail_name: row.guardrailName, window_days: null, found: true,
                current_usage: Math.trunc(Number(row.currentUsage)),
                threshold: Math.trunc(Number(row.threshold)),
                pct_of_threshold: pct, status: statusFromPct(pct), as_of: String(row.auditDate)
            }
        }
        const total = Math.trunc(Number(row.total_readings))
        const breachCount = Math.trunc(Number(row.breach_readings))
        return {
            sandbox, guardrail_name: row.guardrailName, window_days: windowDays, found: true,
            total_readings: total, breach_count: breachCount,
            breach_pct_of_window: total ? Math.round((breachCount / total) * 1000) / 10 : 0,
            max_pct_of_threshold: row.max_pct === null || row.max_pct === undefined ? null : Number(row.max_pct),
            first_breach_at: row.first_breach_at === null || row.first_breach_at === undefined ? null : String(row.first_breach_at),
            last_breach_at: row.last_breach_at === null || row.last_breach_at === undefined ? null : String(row.last_breach_at),
            ever_breached: breachCount > 0
        }
    }

    if (windowDays === null) {
        const asOf = String(rows[0].auditDate)
        const allG = rows.map(r => {
            const pct = r.pct_of_threshold === null || r.pct_of_threshold === undefined ? null : Number(r.pct_of_threshold)
            return { guardrail_name: r.guardrailName, pct_of_threshold: pct, status: statusFromPct(pct) }
        })
        const red = allG.filter(g => g.status === 'red')
        const yellow = allG.filter(g => g.status === 'yellow')
        const overallStatus = red.length ? 'red' : (yellow.length ? 'yellow' : 'green')
        return {
            sandbox, guardrail_name: guardrailName, window_days: null, as_of: asOf,
            overall_status: overallStatus, total_guardrails_checked: allG.length,
            red_count: red.length, yellow_count: yellow.length,
            guardrails: topN ? allG.slice(0, topN) : allG
        }
    }

    const allG = rows.map(r => {
        const total = Math.trunc(Number(r.total_readings))
        const breachCount = Math.trunc(Number(r.breach_readings))
        return {
            guardrail_name: r.guardrailName, total_readings: total, breach_count: breachCount,
            breach_pct_of_window: total ? Math.round((breachCount / total) * 1000) / 10 : 0,
            max_pct_of_threshold: r.max_pct === null || r.max_pct === undefined ? null : Number(r.max_pct),
            ever_breached: breachCount > 0
        }
    })
    allG.sort((a, b) => sortBy === 'breach_count'
        ? b.breach_count - a.breach_count
        : (b.max_pct_of_threshold ?? -Infinity) - (a.max_pct_of_threshold ?? -Infinity))

    return {
        sandbox, guardrail_name: guardrailName, window_days: windowDays,
        guardrails: topN ? allG.slice(0, topN) : allG
    }
}

module.exports = { compute }