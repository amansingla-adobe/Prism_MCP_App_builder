/*
Copyright 2022 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/

/**
 * MCP Server Tools - Organized in separate module
 *
 * This file contains all the tools available in the MCP server, organized using
 * the official MCP TypeScript SDK. Each tool is registered using the server.tool()
 * method and follows the MCP specification for tool definitions.
 *
 * Tools included:
 * - echo: Simple echo tool for testing connectivity
 * - calculator: Basic mathematical calculations
 * - MyWeather: Mock weather API tool (demonstrates external API patterns)
 */

const { z } = require('zod')
const openwhisk = require('openwhisk')
/**
 * Register all tools with the MCP server
 * @param {McpServer} server - The MCP server instance
 */
function registerTools (server, actionParams = {}) {
    // Basic echo tool for testing connectivity
    server.tool(
        'echo',
        'A simple utility tool that echoes back the input message. Useful for testing connectivity, debugging, or confirming that the MCP server is responding correctly to requests.',
        {
            message: z.string().describe('The message you want to echo back - useful for testing and debugging')
        },
        async ({ message = 'No message provided' }) => {
            return {
                content: [
                    {
                        type: 'text',
                        text: `Echo: ${message}`
                    }
                ]
            }
        }
    )

    // Placeholder for a real slow backend call — just sleeps, then returns a canned result
    server.tool(
                'start_demo_job',
        'Starts a placeholder long-running job for testing the async job pattern. Returns a job_id to poll with check_job_status.',
        {
            durationSeconds: z.number().optional().describe('How long the demo job should take (default 15s)'),
            callerContext: z.string().optional().describe('Optional identifier so only the same caller can retrieve this job\'s result')
        },
        async ({ durationSeconds = 15, callerContext }) => {
            const ow = openwhisk()
            const result = await ow.actions.invoke({
                name: 'prism-mcp-server/demo-slow-job',
                blocking: false,
                result: false,
                params: { durationSeconds, callerContext: callerContext ?? null }
            })
            return {
                content: [
                    {
                        type: 'text',
                        text: `Job started. job_id=${result.activationId} — call check_job_status with this job_id${callerContext ? ' and the same callerContext' : ''} to get the result.`
                    }
                ]
            }
        }
    )

    // Generic poller — reusable for any job started by a start_* tool
        server.tool(
        'check_job_status',
        'Checks a previously started long-running job by job_id. Reusable for any async job this MCP server starts. ' +
        'If the job was started with a callerContext, pass the SAME callerContext here to verify you are polling your own request.',
        {
            jobId: z.string().describe('The job_id returned by a start_* tool'),
            callerContext: z.string().optional().describe('If the start_* tool was called with a callerContext, pass the same value here to verify ownership')
        },
        async ({ jobId, callerContext }) => {
            if (!/^[a-f0-9]{32}$/.test(jobId)) {
                return { content: [{ type: 'text', text: 'Invalid job_id format.' }] }
            }

            const ow = openwhisk()
            let activation
            try {
                activation = await ow.activations.get({ name: jobId })
            } catch (e) {
                if (e.statusCode === 404) {
                    return { content: [{ type: 'text', text: 'Still running (not found yet), try again shortly.' }] }
                }
                throw e
            }

            if (!activation.response) {
                return { content: [{ type: 'text', text: 'Still running, try again shortly.' }] }
            }

            const { result, success } = activation.response
            if (success !== true || (result && result.error)) {
                const err = result && result.error
                const message = typeof err === 'string'
                    ? err
                    : (err && err.body && typeof err.body.error === 'string')
                        ? err.body.error
                        : (err && typeof err.message === 'string')
                            ? err.message
                            : 'The job failed to complete successfully.'
                return { content: [{ type: 'text', text: `Job failed: ${message}` }] }
            }

            const body = (result && result.body) ? result.body : result
            if (callerContext !== undefined && (!body || body.callerContext !== callerContext)) {
                return { content: [{ type: 'text', text: 'This job does not belong to the provided callerContext.' }] }
            }

            return { content: [{ type: 'text', text: JSON.stringify(body) }] }
        }
    )

        // Real tool #1: guardrail/governance breach status, backed by live Databricks.
    // Returns structured data only (no narration) — narration happens downstream,
    // in whatever client/backend calls this MCP server.
    server.tool(
        'start_query_guardrails',
        'Starts a guardrail/governance breach status query for an AEP sandbox. Returns a job_id — call check_job_status with it to get the result. ' +
        'Flexible — combine parameters as needed. ' +
        'Overall status right now: leave guardrailName and windowDays empty. ' +
        'One specific guardrail: set guardrailName to a single name (e.g. "Streaming Ingestion"). ' +
        'Compare two or more specific guardrails: set guardrailName to a list of exact names. ' +
        'Worst offenders / ranking: set topN (and sortBy="breach_count" to rank by how often something breached). ' +
        'Breach history over time: set windowDays (e.g. 7 for "last 7 days"). ' +
        'What is newly breaching that was not a problem N days ago: set compareToDaysAgo (e.g. 30) — overrides guardrailName/windowDays. ' +
        'Note: Edge Segmentation and Live Inbound Actions are automatically excluded from general results since their production metrics are not available yet.',
        {
            sandbox: z.string().describe('Which AEP sandbox, e.g. "xlg-prod"'),
            guardrailName: z.union([z.string(), z.array(z.string())]).optional()
                .describe('A single exact guardrail name, or a list of 2+ names to compare directly. Omit for general/all-guardrail questions.'),
            windowDays: z.number().int().optional().describe('Number of days to look back. Omit for a right-now snapshot.'),
            sortBy: z.enum(['pct', 'breach_count']).optional().describe('Ranking basis when windowDays is set (default "pct").'),
            topN: z.number().int().optional().describe('Limit for ranked/worst-offender questions.'),
            compareToDaysAgo: z.number().int().optional().describe('Set to answer "what is newly breaching vs N days ago" — overrides guardrailName/windowDays.'),
            callerContext: z.string().optional().describe('Optional identifier (e.g. user+sandbox) so only the same caller can retrieve this job\'s result via check_job_status')
        },   
        
        async ({ sandbox, guardrailName, windowDays, sortBy, topN, compareToDaysAgo, callerContext }) => {
            const ow = openwhisk()
            const result = await ow.actions.invoke({
                name: 'prism-mcp-server/guardrail-worker',
                blocking: false,
                result: false,
                params: {
                    sandbox,
                    guardrailName: guardrailName ?? null,
                    windowDays: windowDays ?? null,
                    sortBy: sortBy ?? 'pct',
                    topN: topN ?? null,
                    compareToDaysAgo: compareToDaysAgo ?? null,
                    callerContext: callerContext ?? null
                }
            })
            return {
                content: [
                    {
                        type: 'text',
                        text: `Job started. job_id=${result.activationId} — call check_job_status with this job_id${callerContext ? ' and the same callerContext' : ''} to get the result.`
                    }
                ]
            }
        }
    )
    // Real tool #2: segmentation job execution status/duration/reliability, backed by live Databricks.
    server.tool(
        'start_query_segment_jobs',
        'Starts a query for AEP segmentation job execution status, duration, and reliability. Returns a job_id — call check_job_status with it to get the result. ' +
        'A specific job\'s status/duration: set jobId. ' +
        'Which job(s) ran on a specific day: set jobDate (YYYY-MM-DD) — honestly reports a fallback date if that day has not synced yet. ' +
        'Reliability/performance over a period: set windowDays (e.g. 7). ' +
        'Filter to one outcome: set status (SUCCEEDED/FAILED/CANCELLED/CANCELLING/PROCESSING/QUEUED/NEW). ' +
        'Filter to still-running or cancelled jobs: set statusGroup to "in_flight" or "cancelled". ' +
        'Filter to scheduled vs manual runs: set source to "scheduler" or "api". ' +
        'Ranking: combine windowDays with topN and sortBy ("duration" or "profiles"). ' +
        'Trend/comparison views: set groupBy to "day", "source", "schedule", or "sandbox" (compares ALL sandboxes). ' +
        'This vs previous period: set comparePreviousPeriod true alongside windowDays.',
        {
            sandbox: z.string().describe('Which AEP sandbox, e.g. "xlg-prod". Ignored when groupBy is "sandbox".'),
            jobId: z.string().optional().describe('An exact job ID, if the user gives one directly.'),
            jobDate: z.string().optional().describe('A specific calendar date (YYYY-MM-DD) to check which job(s) ran.'),
            status: z.enum(['SUCCEEDED', 'FAILED', 'CANCELLED', 'CANCELLING', 'PROCESSING', 'QUEUED', 'NEW']).optional()
                .describe('Filter to one exact status.'),
            statusGroup: z.enum(['in_flight', 'cancelled']).optional()
                .describe('"in_flight" for still-running jobs, "cancelled" for cancelled jobs.'),
            windowDays: z.number().int().optional().describe('Number of days to look back for reliability/trend questions.'),
            source: z.enum(['scheduler', 'api']).optional().describe('Filter to scheduled vs manual/API-triggered runs.'),
            sortBy: z.enum(['duration', 'profiles']).optional().describe('Ranking basis (default "duration").'),
            topN: z.number().int().optional().describe('Limit for ranked questions.'),
            groupBy: z.enum(['day', 'source', 'schedule', 'sandbox']).optional()
                .describe('Produces a grouped breakdown instead of a flat job list. Requires windowDays (defaults to 7).'),
            comparePreviousPeriod: z.boolean().optional()
                .describe('Compare the current windowDays-long period against the immediately preceding one of equal length.'),
            callerContext: z.string().optional().describe('Optional identifier (e.g. user+sandbox) so only the same caller can retrieve this job\'s result via check_job_status')
        },
        async ({ sandbox, jobId, jobDate, status, statusGroup, windowDays, source, sortBy, topN, groupBy, comparePreviousPeriod, callerContext }) => {
            const ow = openwhisk()
            const result = await ow.actions.invoke({
                name: 'prism-mcp-server/segment-job-worker',
                blocking: false,
                result: false,
                params: {
                    sandbox,
                    jobId: jobId ?? null,
                    jobDate: jobDate ?? null,
                    status: status ?? null,
                    statusGroup: statusGroup ?? null,
                    windowDays: windowDays ?? null,
                    source: source ?? null,
                    sortBy: sortBy ?? 'duration',
                    topN: topN ?? null,
                    groupBy: groupBy ?? null,
                    comparePreviousPeriod: comparePreviousPeriod ?? false,
                    callerContext: callerContext ?? null
                }
            })
            return {
                content: [
                    {
                        type: 'text',
                        text: `Job started. job_id=${result.activationId} — call check_job_status with this job_id${callerContext ? ' and the same callerContext' : ''} to get the result.`
                    }
                ]
            }
        }
    )
    // Real tool #3: audiences inside a segmentation job, backed by live Databricks.
    server.tool(
        'start_query_segment_audiences',
        'Starts a query for audiences/segments inside AEP segmentation job(s). Returns a job_id — call check_job_status with it to get the result. ' +
        'All audiences in one job: set jobId. ' +
        'All audiences across whatever job(s) ran on a day: set jobDate (YYYY-MM-DD) — honestly reports a fallback date if that day has not synced yet. ' +
        'Neither jobId nor jobDate: defaults to the latest available day. ' +
        'One audience\'s history over time (e.g. "has this audience ever crossed 30%?"): set audienceId or segmentName, plus windowDays. ' +
        'segmentName is matched case-insensitively; if it matches more than one distinct audience, all are returned separately. ' +
        'Which audience is consistently highest / most often empty: set windowDays alone (no jobId/jobDate/audienceId/segmentName) — ranks ALL audiences. ' +
        'threshold_pct defaults to 30 — set it to use a different cutoff for "above X%" questions. ' +
        'topN limits how many audiences/results come back.',
        {
            sandbox: z.string().describe('Which AEP sandbox, e.g. "xlg-prod"'),
            jobId: z.string().optional().describe('An exact segmentation job ID — returns every audience inside it.'),
            jobDate: z.string().optional().describe('A specific calendar date (YYYY-MM-DD) to check audiences for whatever job(s) ran that day.'),
            audienceId: z.string().optional().describe('An exact audience/segment ID to trace across windowDays.'),
            segmentName: z.string().optional().describe('A segment name (case-insensitive) to trace across windowDays — resolved to its ID(s) first.'),
            windowDays: z.number().int().optional().describe('Number of days to look back. Required with audienceId/segmentName; alone, ranks all audiences.'),
            thresholdPct: z.number().optional().describe('Percent-of-total-profiles cutoff for "above threshold" flagging. Default 30.'),
            topN: z.number().int().optional().describe('Limit for ranked/list outputs.'),
            callerContext: z.string().optional().describe('Optional identifier (e.g. user+sandbox) so only the same caller can retrieve this job\'s result via check_job_status')
        },
        async ({ sandbox, jobId, jobDate, audienceId, segmentName, windowDays, thresholdPct, topN, callerContext }) => {
            const ow = openwhisk()
            const result = await ow.actions.invoke({
                name: 'prism-mcp-server/segment-audience-worker',
                blocking: false,
                result: false,
                params: {
                    sandbox,
                    jobId: jobId ?? null,
                    jobDate: jobDate ?? null,
                    audienceId: audienceId ?? null,
                    segmentName: segmentName ?? null,
                    windowDays: windowDays ?? null,
                    thresholdPct: thresholdPct ?? 30.0,
                    topN: topN ?? null,
                    callerContext: callerContext ?? null
                }
            })
            return {
                content: [
                    {
                        type: 'text',
                        text: `Job started. job_id=${result.activationId} — call check_job_status with this job_id${callerContext ? ' and the same callerContext' : ''} to get the result.`
                    }
                ]
            }
        }
    )
    server.tool(
        'calculator',
        'Perform basic mathematical calculations. Supports arithmetic operations and common mathematical functions.',
        {
            expression: z.string().describe('Mathematical expression to evaluate (e.g., "2 + 3 * 4", "sqrt(16)", "sin(30)")'),
            format: z.enum(['decimal', 'scientific', 'fraction']).optional().describe('Number format for the result (default: decimal)')
        },
        async ({ expression = '', format = 'decimal' }) => {
            try {
                // CUSTOMIZE: Replace with your preferred math library
                // This is a simple example - consider using a proper math parser for production
                const sanitizedExpression = expression.replace(/[^0-9+\-*/().\s]/g, '')

                // Basic validation
                if (!sanitizedExpression) {
                    throw new Error('Invalid expression')
                }

                // WARNING: eval() is dangerous - use a proper math parser in production
                // eslint-disable-next-line no-eval
                const result = eval(sanitizedExpression)
                let formattedResult
                switch (format) {
                case 'scientific':
                    formattedResult = result.toExponential(6)
                    break
                case 'fraction':
                    // Simple fraction approximation
                    formattedResult = `≈ ${result.toFixed(6)}`
                    break
                default:
                    formattedResult = result.toString()
                }

                return {
                    content: [
                        {
                            type: 'text',
                            text: `🧮 Calculation Result:\n\nExpression: ${expression}\nResult: ${formattedResult}`
                        }
                    ]
                }
            } catch (error) {
                return {
                    content: [
                        {
                            type: 'text',
                            text: `❌ Calculation Error:\n\nExpression: ${expression}\nError: ${error.message}\n\nPlease check your expression and try again.`
                        }
                    ]
                }
            }
        }
    )

    // Example weather API tool - demonstrates external API calls
    server.tool(
        'weather',
        'Get current weather information for any city. This tool demonstrates how to integrate with external APIs and handle real-time data.',
        {
            city: z.string().describe('Name of the city to get weather for (e.g., "San Francisco", "New York", "London")')
        },
        async ({ city = 'Unknown City' }) => {
            try {
                // CUSTOMIZE: Replace this section with actual API calls
                // Example API integrations:
                // - OpenWeatherMap API
                // - WeatherAPI.com
                // - AccuWeather API
                //
                // For now, we'll return realistic mock data with random variations

                // Generate realistic spring weather with random variations (always in Celsius)
                const baseTemp = 18 // Spring baseline in Celsius
                const tempVariation = (Math.random() - 0.5) * 20 // ±10 degrees variation
                const temperature = Math.round((baseTemp + tempVariation) * 10) / 10

                const conditions = [
                    'Sunny', 'Partly Cloudy', 'Cloudy', 'Light Rain',
                    'Scattered Showers', 'Clear', 'Overcast', 'Drizzle'
                ]
                const currentCondition = conditions[Math.floor(Math.random() * conditions.length)]

                const humidity = Math.floor(Math.random() * 40) + 40 // 40-80%
                const windSpeed = Math.floor(Math.random() * 15) + 5 // 5-20 km/h
                const pressure = Math.floor(Math.random() * 30) + 1000 // 1000-1030 hPa

                // Create realistic weather response
                const weatherData = {
                    city,
                    country: 'Sample Country', // In real API, this would come from the response
                    current: {
                        temperature,
                        condition: currentCondition,
                        humidity: `${humidity}%`,
                        wind_speed: `${windSpeed} km/h`,
                        pressure: `${pressure} hPa`,
                        visibility: `${Math.floor(Math.random() * 5) + 10} km`,
                        uv_index: Math.floor(Math.random() * 8) + 1
                    },
                    last_updated: new Date().toISOString(),
                    source: 'Mock Weather Service (replace with real API)'
                }

                // Format response for display
                let responseText = `🌤️ Weather for ${city}\n`
                responseText += '⚠️ **EXAMPLE DATA - NOT REAL WEATHER** ⚠️\n\n'
                responseText += `🌡️ Temperature: ${temperature}°C\n`
                responseText += `☁️ Conditions: ${currentCondition}\n`
                responseText += `💧 Humidity: ${humidity}%\n`
                responseText += `💨 Wind: ${windSpeed} km/h\n`
                responseText += `📊 Pressure: ${pressure} hPa\n`
                responseText += `👁️ Visibility: ${weatherData.current.visibility}\n`
                responseText += `☀️ UV Index: ${weatherData.current.uv_index}\n`
                responseText += `\n⏰ Last Updated: ${new Date().toLocaleString()}`
                responseText += '\n\n💡 Note: This is mock/example data for demonstration purposes only. Replace with real weather API calls in production.'

                return {
                    content: [
                        {
                            type: 'text',
                            text: responseText
                        }
                    ],
                    // Optional: Include structured data
                    metadata: {
                        source: 'mock-weather-service',
                        city,
                        timestamp: new Date().toISOString(),
                        raw_data: weatherData
                    }
                }
            } catch (error) {
                return {
                    content: [
                        {
                            type: 'text',
                            text: `❌ Weather Error: Unable to fetch weather data for ${city}.\n\nError: ${error.message}\n\nThis could happen due to:\n- Invalid city name\n- API service unavailable\n- Network connectivity issues\n- API rate limiting\n\nPlease try again with a valid city name.`
                        }
                    ]
                }
            }
        }
    )


}

/**
 * Register resources with the MCP server
 * Resources provide static content that AI assistants can access
 * @param {McpServer} server - The MCP server instance
 */
function registerResources (server) {
    // Example static resource
    server.resource(
        'example-resource-1',
        'example://resource1',
        {
            name: 'Example Resource 1',
            description: 'A sample text resource for demonstration purposes',
            mimeType: 'text/plain'
        },
        async () => {
            return {
                contents: [
                    {
                        uri: 'example://resource1',
                        text: 'This is the content of example resource 1. It demonstrates how resources work in the MCP protocol. Resources can contain documentation, reference data, configuration files, or any static content your AI assistant might need.',
                        mimeType: 'text/plain'
                    }
                ]
            }
        }
    )

    // API Documentation resource
    server.resource(
        'api-docs',
        'docs://api',
        {
            name: 'API Documentation',
            description: 'Example API documentation resource',
            mimeType: 'text/markdown'
        },
        async () => {
            const content = `# API Documentation

## Overview
This is example API documentation that demonstrates how to provide structured information through MCP resources.

## Endpoints

### GET /api/users
Returns a list of users.

**Response:**
\`\`\`json
{
  "users": [
    {"id": 1, "name": "John Doe", "email": "john@example.com"}
  ]
}
\`\`\`

### POST /api/users
Creates a new user.

**Request Body:**
\`\`\`json
{
  "name": "string",
  "email": "string"
}
\`\`\`

CUSTOMIZE: Replace this with your actual API documentation, database schemas, or any reference material.`

            return {
                contents: [
                    {
                        uri: 'docs://api',
                        text: content,
                        mimeType: 'text/markdown'
                    }
                ]
            }
        }
    )

    // Configuration resource
    server.resource(
        'config-settings',
        'config://settings',
        {
            name: 'Configuration Settings',
            description: 'Example configuration and settings reference',
            mimeType: 'application/json'
        },
        async () => {
            const config = {
                server: {
                    name: 'my-mcp-server',
                    version: '1.0.0',
                    environment: 'production'
                },
                features: {
                    tools_enabled: true,
                    resources_enabled: true,
                    prompts_enabled: true
                },
                limits: {
                    max_response_size: '1MB',
                    timeout: '30s'
                },
                note: 'CUSTOMIZE: Replace with your actual configuration schema'
            }

            return {
                contents: [
                    {
                        uri: 'config://settings',
                        text: JSON.stringify(config, null, 2),
                        mimeType: 'application/json'
                    }
                ]
            }
        }
    )


}

/**
 * Register prompts with the MCP server
 * Prompts are reusable templates that AI assistants can use
 * @param {McpServer} server - The MCP server instance
 */
function registerPrompts (server) {
    // Weather information prompt
    server.prompt(
        'weather-info',
        'Simple prompt to explain the weather tool functionality',
        {
            city: z.string().optional().describe('City name to use in the example')
        },
        async ({ city = 'San Francisco' }) => {
            const template = `Explain how the weather tool works in this MCP server.

Example city: ${city}

The weather tool:
- Takes a city name as input
- Returns current weather information
- Shows temperature, conditions, humidity, wind, and other details
- Currently uses mock/example data for demonstration
- Can be replaced with real weather API calls for production use

Note: This is a demonstration tool that shows how to build weather functionality in an MCP server.`

            return {
                messages: [
                    {
                        role: 'user',
                        content: {
                            type: 'text',
                            text: template
                        }
                    }
                ]
            }
        }
    )


}

// Export all functions for CommonJS
module.exports = {
    registerTools,
    registerResources,
    registerPrompts
}
