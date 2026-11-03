/**
 * Trivial passthrough action - its entire job is to exist as one small,
 * independently-retrievable activation result. No Databricks, no secrets,
 * no business logic. Used by segment-audience-worker (and eventually other
 * workers) to split a too-large result across multiple small activations,
 * each safely under Adobe Runtime's 1MB activation-result ceiling.
 */
async function main (params) {
    return {
        statusCode: 200,
        body: { chunk: params.chunk, chunkIndex: params.chunkIndex, totalChunks: params.totalChunks }
    }
}

exports.main = main