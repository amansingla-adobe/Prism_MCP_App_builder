const { compute } = require('../guardrailQuery')

function errorResult (statusCode, message) {
    return { error: { statusCode, body: { error: message } } }
}

async function main (params) {
    const credentials = {
        host: params.DATABRICKS_SERVER_HOSTNAME,
        warehouseId: params.DATABRICKS_WAREHOUSE_ID,
        token: params.DATABRICKS_TOKEN
    }
    try {
        const result = await compute(credentials, {
            sandbox: params.sandbox,
            guardrailName: params.guardrailName ?? null,
            windowDays: params.windowDays ?? null,
            sortBy: params.sortBy ?? 'pct',
            topN: params.topN ?? null,
            compareToDaysAgo: params.compareToDaysAgo ?? null
        })
        return {
            statusCode: 200,
            body: { ...result, callerContext: params.callerContext ?? null }
        }
    } catch (error) {
        return errorResult(500, error && error.message ? error.message : String(error))
    }
}

exports.main = main