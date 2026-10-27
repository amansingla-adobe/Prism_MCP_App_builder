/**
 * Node/Databricks port of POC_prism/metrics/segment_audience_query.py's compute().
 *
 * Live tables confirmed via direct DESCRIBE (2026-10-22), same catalog/schema
 * as aep_segment_job (dpaas_uccatalog_prd.aoaitgot):
 *   - aep_segment_job_audiences — jobId/audienceId/qualifiedProfiles/
 *     realizedCount/exitedCount/pctOfTotalProfiles all match the Python
 *     original 1:1. startTime/endTime/pctOfTotalTime are documented
 *     "always NULL / do not use" live — not read here, matching Python.
 *   - aep_segment — the live table covers BOTH segmentType values
 *     ("profile" and "account"), unlike the Python original's table name
 *     (aep_segment_b2b_last_30_days) which suggested B2B-only. The query
 *     itself was never type-filtered in Python, so this is a non-issue —
 *     ported as-is.
 *
 * Differences from the Python/DuckDB original, required for live Databricks:
 *   - Catalog-qualified table names.
 *   - Multi-value IN clauses use one named parameter per value (the
 *     Statement Execution API has no array-typed parameter), same pattern
 *     as guardrailQuery.js's buildGuardrailFilter for a name list.
 *   - DuckDB's LIST(DISTINCT col) has no Databricks equivalent; ported to
 *     collect_set(col), the standard Spark SQL aggregate for a distinct
 *     array. Order is non-deterministic in both, so no behavior change.
 *   - auditDate is compared via CAST(:param AS DATE) for clarity, even
 *     though Databricks would likely auto-coerce a 'YYYY-MM-DD' string.
 *   - Databricks interval syntax is `INTERVAL N DAYS` (bare integer + bare
 *     unit), not DuckDB's `INTERVAL 'N days'` — same fix as the other two
 *     query files in this project.
 *
 * Output JSON keys are kept snake_case to match POC_prism/llm/narrator.py's
 * prompt, which references these field names literally.
 */

const { runQuery } = require('./databricksClient')
const { queryJobsByDate, queryLatestAvailableDate } = require('./segmentJobQuery')

const JOB_TABLE = 'dpaas_uccatalog_prd.aoaitgot.aep_segment_job'
const AUDIENCE_TABLE = 'dpaas_uccatalog_prd.aoaitgot.aep_segment_job_audiences'
const SEGMENT_TABLE = 'dpaas_uccatalog_prd.aoaitgot.aep_segment'

const DEFAULT_THRESHOLD_PCT = 30.0

function assertSafeIntervalDays (value, label) {
    const n = Number(value)
    if (!Number.isInteger(n) || n < 0) {
        throw new Error(`${label} must be a non-negative integer, got: ${value}`)
    }
    return n
}

/** Builds a `col IN (:p0, :p1, ...)` clause for a list of values — the
 * Statement Execution API has no array-typed parameter, same pattern used
 * in guardrailQuery.js for a multi-name filter. */
function buildInClause (column, values, paramPrefix, params) {
    const placeholders = values.map((v, i) => {
        const key = `${paramPrefix}${i}`
        params[key] = v
        return `:${key}`
    }).join(', ')
    return `${column} IN (${placeholders})`
}

/** collect_set() returns an ARRAY live. Defensive against either a genuine
 * array coming back (expected, per Databricks docs for JSON_ARRAY/INLINE
 * complex-type handling) or a JSON-stringified array (if the Statement
 * Execution API's "everything is a string" behavior — confirmed true for
 * every scalar column in this codebase's own runQuery usage — turns out to
 * extend to complex types too). Never silently drops data either way. */
function parseTagIds (val) {
    if (val === null || val === undefined) return []
    if (Array.isArray(val)) return val
    if (typeof val === 'string') {
        try {
            const parsed = JSON.parse(val)
            return Array.isArray(parsed) ? parsed : [val]
        } catch (e) {
            return [val]
        }
    }
    return [val]
}

/** Pulls every audience row for the given job IDs, joined to the parent job
 * for totalProfiles/status/job_date context. */
async function queryAudiencesForJobs (credentials, sandbox, jobIds) {
    if (!jobIds || jobIds.length === 0) return []
    const params = { sandbox }
    const inClause = buildInClause('a.jobId', jobIds, 'job', params)
    const sql = `
        SELECT a.jobId, a.audienceId, a.qualifiedProfiles, a.realizedCount, a.exitedCount,
               a.pctOfTotalProfiles, j.totalProfiles, j.status AS job_status,
               CAST(j.creationTime AS DATE) AS job_date
        FROM ${AUDIENCE_TABLE} a
        JOIN ${JOB_TABLE} j ON a.jobId = j.jobId AND a.sandboxName = j.sandboxName
        WHERE a.sandboxName = :sandbox AND ${inClause}
        ORDER BY a.jobId, a.pctOfTotalProfiles DESC NULLS LAST
    `
    return runQuery(credentials, sql, params)
}

/** Segment metadata for the EXACT calendar day requested — never falls back
 * to "latest available" silently. Dedupes the tag fan-out (one row per tag)
 * into one row per segment with a tag_ids array. */
async function querySegmentInfo (credentials, sandbox, segmentIds, onDate) {
    if (!segmentIds || segmentIds.length === 0) return []
    const params = { sandbox, onDate: String(onDate) }
    const inClause = buildInClause('segmentId', segmentIds, 'seg', params)
    const sql = `
        SELECT segmentId,
               any_value(segmentName) AS segmentName,
               any_value(lifecycleState) AS lifecycleState,
               any_value(audienceDefinitionPqlText) AS audienceDefinitionPqlText,
               any_value(segmentType) AS segmentType,
               any_value(evaluationType) AS evaluationType,
               collect_set(tagID) AS tag_ids
        FROM ${SEGMENT_TABLE}
        WHERE sandboxName = :sandbox AND auditDate = CAST(:onDate AS DATE) AND ${inClause}
        GROUP BY segmentId
    `
    return runQuery(credentials, sql, params)
}

/** Case-insensitive name -> segmentId(s). Never silently collapses — if the
 * name matches more than one distinct ID, all are returned. */
async function resolveSegmentName (credentials, sandbox, segmentName) {
    const sql = `
        SELECT DISTINCT segmentId FROM ${SEGMENT_TABLE}
        WHERE sandboxName = :sandbox AND LOWER(segmentName) = LOWER(:segmentName)
    `
    const rows = await runQuery(credentials, sql, { sandbox, segmentName })
    return rows.map(r => r.segmentId)
}

/** One audience's qualification history across every job in the last N days. */
async function queryAudienceHistory (credentials, sandbox, audienceId, days) {
    const safeDays = assertSafeIntervalDays(days, 'window_days')
    const sql = `
        SELECT a.jobId, a.qualifiedProfiles, a.pctOfTotalProfiles,
               CAST(j.creationTime AS DATE) AS job_date
        FROM ${AUDIENCE_TABLE} a
        JOIN ${JOB_TABLE} j ON a.jobId = j.jobId AND a.sandboxName = j.sandboxName
        WHERE a.sandboxName = :sandbox AND a.audienceId = :audienceId
          AND j.creationTime >= (SELECT MAX(creationTime) FROM ${JOB_TABLE} WHERE sandboxName = :sandbox) - INTERVAL ${safeDays} DAYS
        ORDER BY j.creationTime
    `
    return runQuery(credentials, sql, { sandbox, audienceId })
}

/** Across ALL audiences in the window: avg/max pct and how often each was
 * empty — for "which audience is consistently highest" style questions. */
async function queryRankedAudiences (credentials, sandbox, days) {
    const safeDays = assertSafeIntervalDays(days, 'window_days')
    const sql = `
        SELECT a.audienceId,
               COUNT(*) AS appearances,
               AVG(a.pctOfTotalProfiles) AS avg_pct,
               MAX(a.pctOfTotalProfiles) AS max_pct,
               SUM(CASE WHEN a.qualifiedProfiles = 0 THEN 1 ELSE 0 END) AS empty_count
        FROM ${AUDIENCE_TABLE} a
        JOIN ${JOB_TABLE} j ON a.jobId = j.jobId AND a.sandboxName = j.sandboxName
        WHERE a.sandboxName = :sandbox
          AND j.creationTime >= (SELECT MAX(creationTime) FROM ${JOB_TABLE} WHERE sandboxName = :sandbox) - INTERVAL ${safeDays} DAYS
        GROUP BY a.audienceId
        ORDER BY avg_pct DESC NULLS LAST
    `
    return runQuery(credentials, sql, { sandbox })
}

/** Attaches segment metadata to each audience row, grouped by the row's own
 * job_date (point-in-time join — never "latest available"). Honestly flags
 * segment_info_found: false per-row when that day/sandbox has no aep_segment
 * coverage. */
async function enrichWithSegmentInfo (credentials, sandbox, audienceRows) {
    const byDate = new Map()
    for (const row of audienceRows) {
        const d = String(row.job_date)
        if (!byDate.has(d)) byDate.set(d, [])
        byDate.get(d).push(row)
    }

    const enriched = []
    for (const [jobDate, rows] of byDate.entries()) {
        const segIds = [...new Set(rows.map(r => r.audienceId))]
        const segInfoRows = await querySegmentInfo(credentials, sandbox, segIds, jobDate)
        const segInfoById = new Map(segInfoRows.map(r => [r.segmentId, r]))

        for (const row of rows) {
            const segInfo = segInfoById.get(row.audienceId)
            const qualifiedProfiles = Math.trunc(Number(row.qualifiedProfiles))
            const entry = {
                audience_id: row.audienceId,
                qualified_profiles: qualifiedProfiles,
                pct_of_total_profiles: row.pctOfTotalProfiles === null || row.pctOfTotalProfiles === undefined ? null : Number(row.pctOfTotalProfiles),
                is_empty: qualifiedProfiles === 0
            }
            if (segInfo) {
                entry.segment_info_found = true
                entry.segment_name = segInfo.segmentName
                entry.lifecycle_state = segInfo.lifecycleState
                entry.definition = segInfo.audienceDefinitionPqlText
                entry.segment_type = segInfo.segmentType
                entry.evaluation_type = segInfo.evaluationType
                entry.tag_ids = parseTagIds(segInfo.tag_ids)
            } else {
                entry.segment_info_found = false
                entry.segment_info_message = `No segment definition available for ${jobDate} in '${sandbox}' - ` +
                    'either this sandbox isn\'t loaded into the segment table yet, or this date is outside the segment table\'s history.'
            }
            enriched.push(entry)
        }
    }
    return enriched
}

/**
 * job_id: exact job -> every audience in it, enriched with segment info and
 *     flagged above/below threshold_pct.
 * job_date: a specific date -> resolves job(s) that ran that day (reuses
 *     segmentJobQuery's own date logic, including its "hasn't synced yet"
 *     fallback), then does the same per-job breakdown.
 * audience_id / segment_name: pivot on ONE audience's identity across a
 *     window of jobs (window_days required). segment_name is matched
 *     case-insensitively and resolved to its ID(s) first; if it matches more
 *     than one distinct audience_id, ALL are returned separately.
 * window_days + no audience_id/segment_name/job_id/job_date: ranks ALL
 *     audiences in that sandbox over the window.
 */
async function compute (credentials, {
    sandbox, jobId = null, jobDate = null, audienceId = null, segmentName = null,
    windowDays = null, thresholdPct = DEFAULT_THRESHOLD_PCT, topN = null
}) {
    // --- Path 1: one specific job ---
    if (jobId) {
        const audRows = await queryAudiencesForJobs(credentials, sandbox, [jobId])
        if (audRows.length === 0) {
            return {
                sandbox, job_id: jobId, found: false,
                message: `No job with ID '${jobId}' found in sandbox '${sandbox}', or it has no audiences recorded.`
            }
        }
        const audiences = await enrichWithSegmentInfo(credentials, sandbox, audRows)
        audiences.forEach(a => { a.above_threshold = a.pct_of_total_profiles !== null && a.pct_of_total_profiles > thresholdPct })
        const above = audiences.filter(a => a.above_threshold)
        const empty = audiences.filter(a => a.is_empty)
        return {
            sandbox, job_id: jobId, job_date: String(audRows[0].job_date),
            job_status: audRows[0].job_status,
            total_profiles: audRows[0].totalProfiles === null || audRows[0].totalProfiles === undefined ? null : Math.trunc(Number(audRows[0].totalProfiles)),
            found: true, threshold_pct: thresholdPct,
            audience_count: audiences.length, above_threshold_count: above.length, empty_count: empty.length,
            audiences: topN ? audiences.slice(0, topN) : audiences
        }
    }

    // --- Path 2: a specific calendar date (possibly multiple jobs that day), or nothing given at all ---
    if (jobDate !== null || (windowDays === null && audienceId === null && segmentName === null && jobId === null)) {
        const dataAsOf = await queryLatestAvailableDate(credentials, sandbox)
        if (!dataAsOf) {
            return { sandbox, found: false, message: `No job data found at all for sandbox '${sandbox}'.` }
        }

        let fallbackUsed = false
        let targetDate = jobDate || String(dataAsOf)
        if (jobDate !== null && String(jobDate) > String(dataAsOf)) {
            fallbackUsed = true
            targetDate = String(dataAsOf)
        }

        const jobsRows = await queryJobsByDate(credentials, sandbox, targetDate)
        if (jobsRows.length === 0) {
            return {
                sandbox, requested_date: jobDate, data_as_of: String(dataAsOf),
                target_date: targetDate, fallback_used: fallbackUsed, found: false, jobs: [],
                message: fallbackUsed
                    ? `No job data has synced yet for ${jobDate} - most recent available is ${dataAsOf}.`
                    : `No job ran in ${sandbox} on ${targetDate}.`
            }
        }

        const jobIds = jobsRows.map(r => r.jobId)
        const audRows = await queryAudiencesForJobs(credentials, sandbox, jobIds)
        const audiences = audRows.length ? await enrichWithSegmentInfo(credentials, sandbox, audRows) : []
        audiences.forEach(a => { a.above_threshold = a.pct_of_total_profiles !== null && a.pct_of_total_profiles > thresholdPct })

        const jobsOut = jobIds.map(jid => {
            const idsForJob = new Set(audRows.filter(r => r.jobId === jid).map(r => r.audienceId))
            const jobAudiences = audiences.filter(a => idsForJob.has(a.audience_id))
            return {
                job_id: jid,
                audience_count: jobAudiences.length,
                above_threshold_count: jobAudiences.filter(a => a.above_threshold).length,
                empty_count: jobAudiences.filter(a => a.is_empty).length,
                audiences: topN ? jobAudiences.slice(0, topN) : jobAudiences
            }
        })

        return {
            sandbox, requested_date: jobDate, data_as_of: String(dataAsOf),
            target_date: targetDate, fallback_used: fallbackUsed, found: true,
            threshold_pct: thresholdPct, job_count: jobIds.length, jobs: jobsOut,
            message: fallbackUsed ? `No job data has synced yet for ${jobDate} - showing ${dataAsOf} instead.` : null
        }
    }

    // --- Path 3: one specific audience's history across a window ---
    if (audienceId || segmentName) {
        const resolvedIds = audienceId ? [audienceId] : await resolveSegmentName(credentials, sandbox, segmentName)
        if (resolvedIds.length === 0) {
            return {
                sandbox, segment_name: segmentName, audience_id: audienceId, found: false,
                message: `No segment named '${segmentName}' found in sandbox '${sandbox}'.`
            }
        }
        if (windowDays === null) {
            return { sandbox, found: false, message: 'window_days is required to check an audience\'s history over time.' }
        }

        const results = []
        for (const aid of resolvedIds) {
            const rows = await queryAudienceHistory(credentials, sandbox, aid, windowDays)
            if (rows.length === 0) {
                results.push({
                    audience_id: aid, found: false,
                    message: `No job history found for audience '${aid}' in the last ${windowDays} days.`
                })
                continue
            }
            const pctValues = rows.map(r => r.pctOfTotalProfiles).filter(v => v !== null && v !== undefined).map(Number)
            const crossings = rows.filter(r => r.pctOfTotalProfiles !== null && r.pctOfTotalProfiles !== undefined && Number(r.pctOfTotalProfiles) > thresholdPct)
            results.push({
                audience_id: aid, found: true, threshold_pct: thresholdPct,
                window_days: windowDays, appearances: rows.length,
                ever_crossed_threshold: crossings.length > 0, times_crossed: crossings.length,
                avg_pct_of_total_profiles: pctValues.length ? Math.round((pctValues.reduce((a, b) => a + b, 0) / pctValues.length) * 100) / 100 : null,
                max_pct_of_total_profiles: pctValues.length ? Math.round(Math.max(...pctValues) * 100) / 100 : null,
                history: rows.map(r => ({
                    job_id: r.jobId, job_date: String(r.job_date),
                    qualified_profiles: Math.trunc(Number(r.qualifiedProfiles)),
                    pct_of_total_profiles: r.pctOfTotalProfiles === null || r.pctOfTotalProfiles === undefined ? null : Number(r.pctOfTotalProfiles)
                }))
            })
        }

        if (results.length === 1) {
            return { ...results[0], sandbox, segment_name: segmentName }
        }
        return { sandbox, segment_name: segmentName, found: true, multiple_matches: true, match_count: results.length, results }
    }

    // --- Path 4: rank all audiences in a sandbox over a window ---
    const rows = await queryRankedAudiences(credentials, sandbox, windowDays)
    if (rows.length === 0) {
        return { sandbox, window_days: windowDays, found: false, message: 'No audience data found in that time period.' }
    }
    const ranked = rows.map(r => ({
        audience_id: r.audienceId,
        appearances: Math.trunc(Number(r.appearances)),
        avg_pct_of_total_profiles: r.avg_pct === null || r.avg_pct === undefined ? null : Math.round(Number(r.avg_pct) * 100) / 100,
        max_pct_of_total_profiles: r.max_pct === null || r.max_pct === undefined ? null : Math.round(Number(r.max_pct) * 100) / 100,
        empty_count: Math.trunc(Number(r.empty_count))
    }))
    return {
        sandbox, window_days: windowDays, threshold_pct: thresholdPct,
        found: true, audience_count: ranked.length,
        audiences: topN ? ranked.slice(0, topN) : ranked
    }
}

module.exports = { compute }
