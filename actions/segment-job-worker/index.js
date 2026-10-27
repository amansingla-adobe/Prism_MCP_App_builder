const { compute } = require('../segmentJobQuery')

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
            jobId: params.jobId ?? null,
            jobDate: params.jobDate ?? null,
            status: params.status ?? null,
            statusGroup: params.statusGroup ?? null,
            windowDays: params.windowDays ?? null,
            source: params.source ?? null,
            sortBy: params.sortBy ?? 'duration',
            topN: params.topN ?? null,
            groupBy: params.groupBy ?? null,
            comparePreviousPeriod: params.comparePreviousPeriod ?? false
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