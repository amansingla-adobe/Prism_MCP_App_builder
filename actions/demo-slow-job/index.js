async function main(params) {
  const seconds = Number(params.durationSeconds) || 15
  await new Promise((resolve) => setTimeout(resolve, seconds * 1000))
  return {
    statusCode: 200,
    body: {
      message: `Demo job finished after ${seconds}s`,
      startedWith: params,
      callerContext: params.callerContext ?? null
    }
  }
}

exports.main = main