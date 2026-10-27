/**
 * Node/Databricks port of POC_prism/metrics/segment_job_query.py's compute().
 *
 * Live table confirmed via a direct DESCRIBE (2026-10-21): catalog
 * dpaas_uccatalog_prd (NOT dpaas_uccatalog_stg — different from the
 * guardrail table), schema aoaitgot, table aep_segment_job. Columns match
 * the Python original 1:1, and — unlike guardrails' auditDate — jobStartTime/
 * jobEndTime/creationTime/updateTime are genuine `timestamp` columns live,
 * not STRING. No CAST(x AS TIMESTAMP) workaround needed for this table.
 *
 * Differences from the Python/DuckDB original, required for live Databricks:
 *   - Catalog-qualified table name (dpaas_uccatalog_prd.aoaitgot.aep_segment_job).
 *   - Databricks interval syntax is `INTERVAL N DAYS` (bare integer + bare
 *     unit), not DuckDB's `INTERVAL 'N days'` (quoted string) — same fix
 *     applied in guardrailQuery.js.
 *
 * Output JSON keys are kept snake_case (job_id, fallback_used, grouped_by,
 * failure_rate_pct, etc.) to exactly match POC_prism/llm/narrator.py's
 * prompt, which references these field names literally.
 */

const { runQuery } = require('./databricksClient')

const TABLE = 'dpaas_uccatalog_prd.aoaitgot.aep_segment_job'

const IN_FLIGHT_STATUSES = ['NEW', 'QUEUED', 'PROCESSING']
const CANCELLED_STATUSES = ['CANCELLING', 'CANCELLED']
const GROUP_BY_OPTIONS = new Set(['day', 'source', 'sandbox', 'schedule'])
const STATUS_GROUP_OPTIONS = new Set(['in_flight', 'cancelled'])

function statusCategory (status) {
    if (status === null || status === undefined) return 'unknown'
    const s = String(status).toUpperCase()
    if (s === 'SUCCEEDED') return 'succeeded'
    if (s === 'FAILED') return 'failed'
    if (CANCELLED_STATUSES.includes(s)) return 'cancelled'
    if (IN_FLIGHT_STATUSES.includes(s)) return 'in_flight'
    return 'unknown'
}

/** Treats a real SQL NULL and the literal string "null" the same way —
 * defensive against either, since the Python original confirmed the literal
 * string appears in the local CSV export (possibly an export-tool artifact,
 * unconfirmed live now that this column is a genuine timestamp type). */
function cleanTs (val) {
    if (val === null || val === undefined) return null
    const s = String(val).trim()
    if (s === '' || s.toLowerCase() === 'null') return null
    return s
}

function msToHuman (ms) {
    if (ms === null || ms === undefined || Number.isNaN(Number(ms))) return null
    const seconds = Number(ms) / 1000.0
    if (seconds < 60) return `${seconds.toFixed(1)}s`
    const minutes = seconds / 60.0
    if (minutes < 60) return `${minutes.toFixed(1)} min`
    return `${(minutes / 60.0).toFixed(1)} hours`
}

function assertSafeIntervalDays (value, label) {
    const n = Number(value)
    if (!Number.isInteger(n) || n < 0) {
        throw new Error(`${label} must be a non-negative integer, got: ${value}`)
    }
    return n
}

/** status/source matched case-insensitively (router/user may not match exact
 * casing). statusGroup expands to the full IN_FLIGHT/CANCELLED list. */
function buildFilterClause (status, source, statusGroup, paramPrefix) {
    let clause = ''
    const params = {}
    if (status) {
        clause += ` AND UPPER(status) = UPPER(:${paramPrefix}status)`
        params[`${paramPrefix}status`] = status
    }
    if (source) {
        clause += ` AND LOWER(source) = LOWER(:${paramPrefix}source)`
        params[`${paramPrefix}source`] = source
    }
    if (statusGroup === 'in_flight') {
        const placeholders = IN_FLIGHT_STATUSES.map((_, i) => `:${paramPrefix}ifs${i}`).join(', ')
        clause += ` AND UPPER(status) IN (${placeholders})`
        IN_FLIGHT_STATUSES.forEach((s, i) => { params[`${paramPrefix}ifs${i}`] = s })
    } else if (statusGroup === 'cancelled') {
        const placeholders = CANCELLED_STATUSES.map((_, i) => `:${paramPrefix}cs${i}`).join(', ')
        clause += ` AND UPPER(status) IN (${placeholders})`
        CANCELLED_STATUSES.forEach((s, i) => { params[`${paramPrefix}cs${i}`] = s })
    }
    return { clause, params }
}

const JOB_DETAIL_COLUMNS = `jobId, sandboxName, status, source, totalTimeMs, segmentationTimeMs,
            jobStartTime, jobEndTime, totalProfiles, audienceCount, scheduleId, creationTime`

async function queryJobById (credentials, sandbox, jobId) {
    const sql = `
        SELECT jobId, sandboxName, status, source, batchId,
            totalTimeMs, segmentationTimeMs, jobStartTime, jobEndTime,
            totalProfiles, audienceCount, scheduleId, runId, mergePolicyId,
            creationTime, updateTime
        FROM ${TABLE}
        WHERE sandboxName = :sandbox AND jobId = :jobId
    `
    return runQuery(credentials, sql, { sandbox, jobId })
}

async function queryLatestAvailableDate (credentials, sandbox) {
    const sql = `SELECT MAX(CAST(creationTime AS DATE)) AS maxDate FROM ${TABLE} WHERE sandboxName = :sandbox`
    const rows = await runQuery(credentials, sql, { sandbox })
    return rows[0] ? rows[0].maxDate : null
}

async function queryEarliestAvailableDate (credentials, sandbox) {
    const sql = `SELECT MIN(CAST(creationTime AS DATE)) AS minDate FROM ${TABLE} WHERE sandboxName = :sandbox`
    const rows = await runQuery(credentials, sql, { sandbox })
    return rows[0] ? rows[0].minDate : null
}

async function queryJobsByDate (credentials, sandbox, targetDate, status, source, statusGroup) {
    const { clause, params } = buildFilterClause(status, source, statusGroup, 'f')
    const sql = `
        SELECT ${JOB_DETAIL_COLUMNS}
        FROM ${TABLE}
        WHERE sandboxName = :sandbox
          AND CAST(creationTime AS DATE) = :targetDate
          ${clause}
        ORDER BY creationTime DESC
    `
    return runQuery(credentials, sql, { sandbox, targetDate, ...params })
}

async function queryWindowed (credentials, sandbox, days, status, source, statusGroup) {
    const safeDays = assertSafeIntervalDays(days, 'window_days')
    const { clause, params } = buildFilterClause(status, source, statusGroup, 'f')
    const sql = `
        SELECT ${JOB_DETAIL_COLUMNS}
        FROM ${TABLE}
        WHERE sandboxName = :sandbox
          AND creationTime >= (SELECT MAX(creationTime) FROM ${TABLE} WHERE sandboxName = :sandbox) - INTERVAL ${safeDays} DAYS
          ${clause}
        ORDER BY creationTime DESC
    `
    return runQuery(credentials, sql, { sandbox, ...params })
}

async function queryWindowedPreviousPeriod (credentials, sandbox, days, status, source, statusGroup) {
    const safeDays = assertSafeIntervalDays(days, 'window_days')
    const doubleDays = safeDays * 2
    const { clause, params } = buildFilterClause(status, source, statusGroup, 'f')
    const sql = `
        SELECT ${JOB_DETAIL_COLUMNS}
        FROM ${TABLE}
        WHERE sandboxName = :sandbox
          AND creationTime >= (SELECT MAX(creationTime) FROM ${TABLE} WHERE sandboxName = :sandbox) - INTERVAL ${doubleDays} DAYS
          AND creationTime <  (SELECT MAX(creationTime) FROM ${TABLE} WHERE sandboxName = :sandbox) - INTERVAL ${safeDays} DAYS
          ${clause}
        ORDER BY creationTime DESC
    `
    return runQuery(credentials, sql, { sandbox, ...params })
}

const BUCKET_AGGREGATE_COLUMNS = `
               COUNT(*) AS job_count,
               SUM(CASE WHEN UPPER(status) = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded_count,
               SUM(CASE WHEN UPPER(status) = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
               AVG(totalTimeMs) AS avg_total_time_ms,
               SUM(totalProfiles) AS total_profiles_sum,
               AVG(audienceCount) AS avg_audience_count`

async function queryGroupedByDay (credentials, sandbox, days, status, source, statusGroup) {
    const safeDays = assertSafeIntervalDays(days, 'window_days')
    const { clause, params } = buildFilterClause(status, source, statusGroup, 'f')
    const sql = `
        SELECT CAST(creationTime AS DATE) AS group_key, ${BUCKET_AGGREGATE_COLUMNS}
        FROM ${TABLE}
        WHERE sandboxName = :sandbox
          AND creationTime >= (SELECT MAX(creationTime) FROM ${TABLE} WHERE sandboxName = :sandbox) - INTERVAL ${safeDays} DAYS
          ${clause}
        GROUP BY 1
        ORDER BY 1 ASC
    `
    return runQuery(credentials, sql, { sandbox, ...params })
}

async function queryGroupedBySource (credentials, sandbox, days, status, statusGroup) {
    const safeDays = assertSafeIntervalDays(days, 'window_days')
    const { clause, params } = buildFilterClause(status, null, statusGroup, 'f')
    const sql = `
        SELECT source AS group_key, ${BUCKET_AGGREGATE_COLUMNS}
        FROM ${TABLE}
        WHERE sandboxName = :sandbox
          AND creationTime >= (SELECT MAX(creationTime) FROM ${TABLE} WHERE sandboxName = :sandbox) - INTERVAL ${safeDays} DAYS
          ${clause}
        GROUP BY 1
        ORDER BY 1 ASC
    `
    return runQuery(credentials, sql, { sandbox, ...params })
}

async function queryGroupedBySchedule (credentials, sandbox, days, status, source, statusGroup) {
    const safeDays = assertSafeIntervalDays(days, 'window_days')
    const { clause, params } = buildFilterClause(status, source, statusGroup, 'f')
    const sql = `
        SELECT scheduleId AS group_key, ${BUCKET_AGGREGATE_COLUMNS}
        FROM ${TABLE}
        WHERE sandboxName = :sandbox
          AND creationTime >= (SELECT MAX(creationTime) FROM ${TABLE} WHERE sandboxName = :sandbox) - INTERVAL ${safeDays} DAYS
          ${clause}
        GROUP BY 1
        ORDER BY job_count DESC
    `
    return runQuery(credentials, sql, { sandbox, ...params })
}

/** Crosses ALL sandboxes — no sandbox filter, anchored on the global latest creationTime. */
async function queryGroupedBySandbox (credentials, days, status, source, statusGroup) {
    const safeDays = assertSafeIntervalDays(days, 'window_days')
    const { clause, params } = buildFilterClause(status, source, statusGroup, 'f')
    const sql = `
        SELECT sandboxName AS group_key, ${BUCKET_AGGREGATE_COLUMNS}
        FROM ${TABLE}
        WHERE creationTime >= (SELECT MAX(creationTime) FROM ${TABLE}) - INTERVAL ${safeDays} DAYS
          ${clause}
        GROUP BY 1
        ORDER BY 1 ASC
    `
    return runQuery(credentials, sql, params)
}

function bucketRows (rows) {
    return rows.map(r => {
        const total = Math.trunc(Number(r.job_count))
        const failed = Math.trunc(Number(r.failed_count))
        return {
            group_value: String(r.group_key),
            job_count: total,
            succeeded_count: Math.trunc(Number(r.succeeded_count)),
            failed_count: failed,
            failure_rate_pct: total ? Math.round((failed / total) * 1000) / 10 : 0,
            avg_total_time_ms: r.avg_total_time_ms === null || r.avg_total_time_ms === undefined ? null : Math.round(Number(r.avg_total_time_ms) * 10) / 10,
            total_profiles_sum: r.total_profiles_sum === null || r.total_profiles_sum === undefined ? null : Math.trunc(Number(r.total_profiles_sum)),
            avg_audience_count: r.avg_audience_count === null || r.avg_audience_count === undefined ? null : Math.round(Number(r.avg_audience_count) * 10) / 10
        }
    })
}

function jobDetailDict (row) {
    const totalTimeMs = row.totalTimeMs === null || row.totalTimeMs === undefined ? null : Number(row.totalTimeMs)
    return {
        job_id: row.jobId,
        status: row.status,
        status_category: statusCategory(row.status),
        source: row.source,
        schedule_id: row.scheduleId,
        creation_time: row.creationTime,
        job_start_time: row.jobStartTime,
        job_end_time: cleanTs(row.jobEndTime),
        total_time_ms: totalTimeMs === null ? null : Math.trunc(totalTimeMs),
        total_time_human: msToHuman(totalTimeMs),
        total_profiles: row.totalProfiles === null || row.totalProfiles === undefined ? null : Math.trunc(Number(row.totalProfiles)),
        audience_count: row.audienceCount === null || row.audienceCount === undefined ? null : Math.trunc(Number(row.audienceCount))
    }
}

/** Shared by the plain windowed view and compare_previous_period, so both
 * compute stats identically instead of drifting apart. */
function summarizeJobsRows (rows, sortBy, topN) {
    const total = rows.length
    const statusCounts = {}
    rows.forEach(r => { statusCounts[r.status] = (statusCounts[r.status] || 0) + 1 })
    const failed = statusCounts.FAILED || 0
    const succeeded = statusCounts.SUCCEEDED || 0
    const cancelled = CANCELLED_STATUSES.reduce((sum, s) => sum + (statusCounts[s] || 0), 0)
    const inFlight = IN_FLIGHT_STATUSES.reduce((sum, s) => sum + (statusCounts[s] || 0), 0)

    const sortKey = sortBy === 'profiles' ? 'totalProfiles' : 'totalTimeMs'
    const ranked = [...rows].sort((a, b) => {
        const av = a[sortKey] === null || a[sortKey] === undefined ? -Infinity : Number(a[sortKey])
        const bv = b[sortKey] === null || b[sortKey] === undefined ? -Infinity : Number(b[sortKey])
        return bv - av
    })
    const jobs = ranked.map(jobDetailDict)

    const timeValues = rows.map(r => r.totalTimeMs).filter(v => v !== null && v !== undefined).map(Number)
    const profileValues = rows.map(r => r.totalProfiles).filter(v => v !== null && v !== undefined).map(Number)
    const audienceValues = rows.map(r => r.audienceCount).filter(v => v !== null && v !== undefined).map(Number)

    return {
        job_count: total,
        succeeded_count: succeeded,
        failed_count: failed,
        cancelled_count: cancelled,
        in_flight_count: inFlight,
        failure_rate_pct: total ? Math.round((failed / total) * 1000) / 10 : 0,
        avg_total_time_ms: timeValues.length ? Math.round((timeValues.reduce((a, b) => a + b, 0) / timeValues.length) * 10) / 10 : null,
        max_total_time_ms: timeValues.length ? Math.trunc(Math.max(...timeValues)) : null,
        total_profiles_sum: profileValues.length ? Math.trunc(profileValues.reduce((a, b) => a + b, 0)) : null,
        avg_audience_count: audienceValues.length ? Math.round((audienceValues.reduce((a, b) => a + b, 0) / audienceValues.length) * 10) / 10 : null,
        jobs: topN ? jobs.slice(0, topN) : jobs
    }
}

/**
 * @param {{host:string, warehouseId:string, token:string}} credentials
 * @param {object} options
 */
async function compute (credentials, options) {
    const {
        sandbox, jobId = null, jobDate = null, status = null, statusGroup = null,
        windowDays: windowDaysIn = null, source = null, sortBy = 'duration', topN = null,
        groupBy = null, comparePreviousPeriod = false
    } = options
    let windowDays = windowDaysIn

    if (jobId) {
        const rows = await queryJobById(credentials, sandbox, jobId)
        if (rows.length === 0) {
            return {
                sandbox, job_id: jobId, found: false,
                message: `No job with ID '${jobId}' found in sandbox '${sandbox}'.`
            }
        }
        return { ...jobDetailDict(rows[0]), sandbox, found: true }
    }

    if (groupBy !== null && !GROUP_BY_OPTIONS.has(groupBy)) {
        return { sandbox, found: false, message: `Unknown group_by '${groupBy}'. Valid options: day, source, sandbox, schedule.` }
    }
    if (statusGroup !== null && !STATUS_GROUP_OPTIONS.has(statusGroup)) {
        return { sandbox, found: false, message: `Unknown status_group '${statusGroup}'. Valid options: in_flight, cancelled.` }
    }

    if (groupBy !== null) {
        if (windowDays === null) windowDays = 7
        let rows
        if (groupBy === 'day') rows = await queryGroupedByDay(credentials, sandbox, windowDays, status, source, statusGroup)
        else if (groupBy === 'source') rows = await queryGroupedBySource(credentials, sandbox, windowDays, status, statusGroup)
        else if (groupBy === 'schedule') rows = await queryGroupedBySchedule(credentials, sandbox, windowDays, status, source, statusGroup)
        else rows = await queryGroupedBySandbox(credentials, windowDays, status, source, statusGroup)

        if (rows.length === 0) {
            return {
                sandbox, window_days: windowDays, grouped_by: groupBy, found: false, buckets: [],
                message: 'No jobs matched those filters in that time period.'
            }
        }

        const buckets = bucketRows(rows)
        const result = {
            window_days: windowDays, grouped_by: groupBy, status, source, status_group: statusGroup,
            found: true, bucket_count: buckets.length, buckets: topN ? buckets.slice(0, topN) : buckets
        }
        if (groupBy === 'sandbox') {
            result.sandbox = null
            result.note = 'Comparing across all sandboxes - the single-sandbox selection is ignored for this view.'
        } else {
            result.sandbox = sandbox
        }
        return result
    }

    if (comparePreviousPeriod) {
        if (windowDays === null) windowDays = 7
        const currentRows = await queryWindowed(credentials, sandbox, windowDays, status, source, statusGroup)
        const previousRows = await queryWindowedPreviousPeriod(credentials, sandbox, windowDays, status, source, statusGroup)
        const dataAsOf = await queryLatestAvailableDate(credentials, sandbox)
        const earliest = await queryEarliestAvailableDate(credentials, sandbox)

        const currentSummary = currentRows.length ? summarizeJobsRows(currentRows, sortBy, topN) : null
        const previousSummary = previousRows.length ? summarizeJobsRows(previousRows, sortBy, topN) : null

        let previousIncomplete = false
        let previousPeriodMessage = null
        if (dataAsOf) {
            const dataAsOfDate = new Date(dataAsOf)
            const neededStart = new Date(dataAsOfDate)
            neededStart.setDate(neededStart.getDate() - (2 * windowDays))
            const earliestDate = earliest ? new Date(earliest) : null
            if (!earliestDate || earliestDate > neededStart) {
                previousIncomplete = true
                const totalDays = earliestDate ? Math.round((dataAsOfDate - earliestDate) / 86400000) + 1 : 0
                previousPeriodMessage = `Only ${totalDays} days of job history exist in total - the previous ` +
                    `${windowDays}-day period falls partially or fully outside that range, so this comparison may be incomplete.`
            }
        }

        let change = null
        if (currentSummary && previousSummary) {
            change = {
                job_count_change: currentSummary.job_count - previousSummary.job_count,
                failure_rate_pct_change: Math.round((currentSummary.failure_rate_pct - previousSummary.failure_rate_pct) * 10) / 10,
                avg_total_time_ms_change: (currentSummary.avg_total_time_ms === null || previousSummary.avg_total_time_ms === null)
                    ? null
                    : Math.round((currentSummary.avg_total_time_ms - previousSummary.avg_total_time_ms) * 10) / 10
            }
        }

        return {
            sandbox, window_days: windowDays, compare_previous_period: true,
            data_as_of: dataAsOf ? String(dataAsOf) : null,
            previous_period_incomplete: previousIncomplete,
            previous_period_message: previousPeriodMessage,
            current_period: currentSummary || { job_count: 0 },
            previous_period: previousSummary || { job_count: 0 },
            change
        }
    }

    // status/status_group filter alone (no date/window given) implies a time-bounded question.
    if (windowDays === null && jobDate === null && (status !== null || statusGroup !== null)) {
        windowDays = 7
    }

    if (windowDays !== null) {
        const rows = await queryWindowed(credentials, sandbox, windowDays, status, source, statusGroup)
        const dataAsOf = await queryLatestAvailableDate(credentials, sandbox)

        if (rows.length === 0) {
            return {
                sandbox, window_days: windowDays, status, source, status_group: statusGroup,
                found: false, job_count: 0, jobs: [],
                data_as_of: dataAsOf ? String(dataAsOf) : null,
                message: 'No jobs matched those filters in that time period.'
            }
        }

        const summary = summarizeJobsRows(rows, sortBy, topN)
        return {
            sandbox, window_days: windowDays, status, source, status_group: statusGroup, found: true,
            data_as_of: dataAsOf ? String(dataAsOf) : null,
            ...summary
        }
    }

    // job_date given, or nothing given at all (defaults to latest available date).
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

    const rows = await queryJobsByDate(credentials, sandbox, targetDate, status, source, statusGroup)
    const jobs = rows.map(jobDetailDict)

    let message = null
    if (fallbackUsed) {
        message = `No job data has synced yet for ${jobDate} - showing the most recently available day (${dataAsOf}) instead.`
    } else if (jobs.length === 0) {
        message = `No job ran in ${sandbox} on ${targetDate}.`
    }

    const totalProfilesSum = jobs.length ? jobs.reduce((sum, j) => sum + (j.total_profiles || 0), 0) : null
    const totalAudienceCountSum = jobs.length ? jobs.reduce((sum, j) => sum + (j.audience_count || 0), 0) : null

    return {
        sandbox, requested_date: jobDate, data_as_of: String(dataAsOf),
        target_date: targetDate, fallback_used: fallbackUsed,
        found: jobs.length > 0, job_count: jobs.length, jobs,
        total_profiles_sum: totalProfilesSum, total_audience_count_sum: totalAudienceCountSum,
        message
    }
}

module.exports = { compute, queryJobsByDate, queryLatestAvailableDate }