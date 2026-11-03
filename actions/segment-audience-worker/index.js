const { compute } = require('../segmentAudienceQuery')
const openwhisk = require('openwhisk')

// Conservative margin under Adobe Runtime's real 1MB activation-result ceiling,
// leaving headroom for JSON overhead and multi-byte characters in text fields.
const MAX_CHUNK_BYTES = 350000
const SIZE_CHECK_THRESHOLD = 700000

function errorResult (statusCode, message) {
    return { error: { statusCode, body: { error: message } } }
}

/** Greedily packs array items into chunks, each chunk's JSON size staying under
 * maxBytes. Operates at whole-item granularity - never splits one record across
 * two chunks. */
function chunkBySize (items, maxBytes) {
    const chunks = []
    let current = []
    let currentSize = 2
    for (const item of items) {
        const itemSize = JSON.stringify(item).length + 1
        if (current.length > 0 && currentSize + itemSize > maxBytes) {
            chunks.push(current)
            current = []
            currentSize = 2
        }
        current.push(item)
        currentSize += itemSize
    }
    if (current.length > 0) chunks.push(current)
    return chunks
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
            aboveThresholdOnly: params.aboveThresholdOnly ?? false,
            topN: params.topN ?? null
        })
        const fullBody = { ...result, callerContext: params.callerContext ?? null }
        const serialized = JSON.stringify(fullBody)

        if (serialized.length < SIZE_CHECK_THRESHOLD) {
            return { statusCode: 200, body: fullBody }
        }

        const arrayFieldName = ['audiences', 'history', 'results'].find(k => Array.isArray(fullBody[k]))
        if (!arrayFieldName) {
            throw new Error(`Result exceeds safe size (${serialized.length} bytes) but no chunkable array field was found`)
        }

        const items = fullBody[arrayFieldName]
        const chunks = chunkBySize(items, MAX_CHUNK_BYTES)
        const ow = openwhisk()

        // Dispatch non-blocking - avoids the 60-second cap that applies only
        // to blocking invocations. This worker was itself started non-blocking,
        // so it has up to 3 hours of its own budget - plenty to verify
        // chunk-holder completion without racing that separate 60s ceiling.
        const chunkActivationIds = await Promise.all(
            chunks.map((chunk, i) =>
                ow.actions.invoke({
                    name: 'prism-mcp-server/chunk-holder',
                    blocking: false,
                    result: false,
                    params: { chunk, chunkIndex: i, totalChunks: chunks.length }
                }).then(r => r.activationId)
            )
        )

        // Poll until every chunk-holder finishes (success or failure) -
        // catches OOM/crash HERE, before returning, instead of silently
        // shipping a broken activation ID. Bounded well under the backend's
        // own ~3min overall patience so there's still time left to fetch
        // the real chunk data afterward.
        const CHUNK_VERIFY_POLL_MS = 1000
        const CHUNK_VERIFY_TIMEOUT_MS = 60000
        const verifyDeadline = Date.now() + CHUNK_VERIFY_TIMEOUT_MS
        const chunkStates = new Array(chunkActivationIds.length).fill(null)
        const pending = new Set(chunkActivationIds.map((_, i) => i))

        while (pending.size > 0) {
            if (Date.now() > verifyDeadline) {
                throw new Error(`Timed out waiting for ${pending.size}/${chunks.length} chunk-holder activations to finish`)
            }
            // eslint-disable-next-line no-await-in-loop
            await Promise.all([...pending].map(async (i) => {
                let activation
                try {
                    activation = await ow.activations.get({ name: chunkActivationIds[i] })
                } catch (e) {
                    if (e.statusCode === 404) return // not ready yet
                    throw e
                }
                if (!activation.response) return // exists but not finished yet
                chunkStates[i] = activation.response
                pending.delete(i)
            }))
            if (pending.size > 0) {
                // eslint-disable-next-line no-await-in-loop
                await new Promise((r) => setTimeout(r, CHUNK_VERIFY_POLL_MS))
            }
        }

        const failed = chunkStates
            .map((state, i) => ({ state, i }))
            .filter(({ state }) => state.success !== true)
        if (failed.length > 0) {
            const details = failed.map(({ state, i }) => {
                const err = state.result && state.result.error
                const msg = typeof err === 'string' ? err : (err && err.body && err.body.error) || 'unknown failure'
                return `chunk ${i}: ${msg}`
            }).join('; ')
            throw new Error(`${failed.length}/${chunks.length} chunk-holder invocations failed: ${details}`)
        }

        const chunkJobIds = chunkActivationIds

        const { [arrayFieldName]: _omitted, ...rest } = fullBody
        return {
            statusCode: 200,
            body: {
                ...rest,
                paginated: true,
                chunkedField: arrayFieldName,
                totalItems: items.length,
                chunkJobIds
            }
        }
    } catch (error) {
        return errorResult(500, error && error.message ? error.message : String(error))
    }
}

exports.main = main