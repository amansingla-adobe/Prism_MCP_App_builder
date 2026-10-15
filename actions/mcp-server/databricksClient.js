/**
 * Databricks SQL Statement Execution REST API client — lightweight
 * alternative to the official @databricks/sql SDK. Needed because the SDK's
 * dependency tree doesn't fit Adobe I/O Runtime's hard, non-configurable 22MB
 * action size ceiling once combined with the MCP SDK itself. Zero extra npm
 * dependencies — uses the Node 20 runtime's built-in fetch.
 *
 * @see https://docs.databricks.com/api/workspace/statementexecution/executestatement
 */

const STATEMENTS_PATH = '/api/2.0/sql/statements'
const POLL_INTERVAL_MS = 1000
const MAX_POLL_ATTEMPTS = 25 // keeps total wall-clock comfortably under Adobe's 60s web-action cap

function toApiParameters (namedParameters) {
    return Object.entries(namedParameters).map(([name, value]) => ({
        name,
        value: value === null || value === undefined ? undefined : String(value)
    }))
}

function toRowObjects (manifest, result) {
    const columnNames = [...manifest.schema.columns]
        .sort((a, b) => a.position - b.position)
        .map(c => c.name)
    const rows = (result && result.data_array) || []
    return rows.map(row => {
        const obj = {}
        columnNames.forEach((name, i) => { obj[name] = row[i] })
        return obj
    })
}

function assertNotFailed (body) {
    const state = body.status && body.status.state
    if (state === 'FAILED' || state === 'CANCELED' || state === 'CLOSED') {
        const errMsg = (body.status.error && body.status.error.message) || state
        throw new Error(`Databricks statement ${state}: ${errMsg}`)
    }
    return state
}

async function pollUntilDone (credentials, statementId) {
    for (let attempt = 0; attempt < MAX_POLL_ATTEMPTS; attempt++) {
        await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS))
        const res = await fetch(`https://${credentials.host}${STATEMENTS_PATH}/${statementId}`, {
            headers: { Authorization: `Bearer ${credentials.token}` }
        })
        const body = await res.json()
        if (!res.ok) {
            throw new Error(`Databricks statement poll HTTP ${res.status}: ${JSON.stringify(body)}`)
        }
        const state = assertNotFailed(body)
        if (state === 'SUCCEEDED') return body
        // PENDING / RUNNING — keep polling
    }
    throw new Error('Databricks statement did not complete within the polling window')
}

/**
 * @param {{host:string, warehouseId:string, token:string}} credentials
 * @param {string} sql - SQL text using :namedParam markers
 * @param {object} [namedParameters]
 * @returns {Promise<object[]>} rows as plain objects
 */
async function runQuery (credentials, sql, namedParameters = {}) {
    const requestBody = {
        statement: sql,
        warehouse_id: credentials.warehouseId,
        wait_timeout: '30s',
        on_wait_timeout: 'CONTINUE',
        format: 'JSON_ARRAY',
        disposition: 'INLINE',
        parameters: toApiParameters(namedParameters)
    }

    const res = await fetch(`https://${credentials.host}${STATEMENTS_PATH}`, {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${credentials.token}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody)
    })
    let body = await res.json()
    if (!res.ok) {
        throw new Error(`Databricks statement execute HTTP ${res.status}: ${JSON.stringify(body)}`)
    }

    let state = assertNotFailed(body)
    if (state === 'PENDING' || state === 'RUNNING') {
        body = await pollUntilDone(credentials, body.statement_id)
    }

    return toRowObjects(body.manifest, body.result)
}

module.exports = { runQuery }