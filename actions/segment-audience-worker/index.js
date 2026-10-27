const { compute } = require('../segmentAudienceQuery')

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
            audienceId: params.audienceId ?? null,
            segmentName: params.segmentName ?? null,
            windowDays: params.windowDays ?? null,
            thresholdPct: params.thresholdPct ?? 30.0,
            topN: params.topN ?? null
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
