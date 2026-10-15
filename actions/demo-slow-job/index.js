/**
 * Placeholder for a real slow backend call — just sleeps, then returns a canned result.
 * Proves the async job+poll pattern works before wiring real Prism business logic to it.
 */
async function main(params) {
  const seconds = Number(params.durationSeconds) || 15
  await new Promise((resolve) => setTimeout(resolve, seconds * 1000))
  return {
    statusCode: 200,
    body: { message: `Demo job finished after ${seconds}s`, startedWith: params }
  }
}

exports.main = main