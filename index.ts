const {BigQuery} = require('@google-cloud/bigquery');
const { v4: uuidv4 } = require('uuid');
const bigquery = new BigQuery();
const { Pool } = require('pg')
const pool = new Pool(
    {user: 'postgres',
        host: process.env.POSTGRES_HOST,
        database: process.env.DATABASE_NAME,
        password: process.env.POSTGRES_PASS,
        port: 5432,
        max:process.env.POSTGRES_POOL_MAX_SIZE,
    }
)
// the pool will emit an error on behalf of any idle clients
// it contains if a backend error or network partition happens
pool.on('error', (err:any, client:any) => {
    console.error('Unexpected error on idle client', err)
    process.exit(-1)
})


const queryGlobalStats = (accountId: number, dateFrom: Date, dateTo:Date) =>
    `SELECT DATETIME_TRUNC(DATETIME(\`platform_workflow\`.start_time, 'UTC'), 
DAY) \`platform_workflow__time_day\`, 
count(distinct \`platform_workflow\`.workflow_id) \`platform_workflow__total_executions\`,
 avg(\`platform_workflow\`.duration) \`platform_workflow__average_duration\`,
  count(distinct \`platform_workflow\`.initiator) \`platform_workflow__committers\`,
   count(distinct succeeded_id) / (case when count(distinct workflow_id) > 0 then count(distinct workflow_id) else 1 end) \`platform_workflow__success_rate\`
FROM
      (SELECT wf.*,  (case when wf.status = 'Succeeded' then workflow_id else null end) as succeeded_id, branch  
      FROM platform_workflow_benchmark.platform_workflow as wf  
      LEFT JOIN wf.branches as branch) AS \`platform_workflow\`  
      WHERE (\`platform_workflow\`.start_time >= TIMESTAMP('${dateFrom.toISOString()}')
       AND \`platform_workflow\`.start_time <= TIMESTAMP('${dateTo.toISOString()}')) 
       AND (\`platform_workflow\`.account_id = 'customer_${accountId}') 
       GROUP BY 1 ORDER BY 1 ASC LIMIT 100;`


const queryPipelineStats = (accountId: number, dateFrom: Date, dateTo:Date, pipelineId: number) =>
    `SELECT DATETIME_TRUNC(DATETIME(\`platform_workflow\`.start_time, 'UTC'), 
DAY) \`platform_workflow__time_day\`, 
count(distinct \`platform_workflow\`.workflow_id) \`platform_workflow__total_executions\`,
 avg(\`platform_workflow\`.duration) \`platform_workflow__average_duration\`,
  count(distinct \`platform_workflow\`.initiator) \`platform_workflow__committers\`,
   count(distinct succeeded_id) / (case when count(distinct workflow_id) > 0 then count(distinct workflow_id) else 1 end) \`platform_workflow__success_rate\`
FROM
      (SELECT wf.*,  (case when wf.status = 'Succeeded' then workflow_id else null end) as succeeded_id, branch  
      FROM platform_workflow_benchmark.platform_workflow as wf  
      LEFT JOIN wf.branches as branch) AS \`platform_workflow\`  
      WHERE (\`platform_workflow\`.start_time >= TIMESTAMP('${dateFrom.toISOString()}')
       AND \`platform_workflow\`.start_time <= TIMESTAMP('${dateTo.toISOString()}')) 
       AND (\`platform_workflow\`.account_id = 'customer_${accountId}') 
       and (\`platform_workflow\`.pipeline_name = 'pipeline_${pipelineId}')
       GROUP BY 1 ORDER BY 1 ASC LIMIT 100;`

const queryTop10Stats = (accountId: number, dateFrom: Date, dateTo:Date) =>
    `select platform_workflow.pipeline_name platform_workflow__pipeline_name,
                platform_workflow.pipeline_namespace platform_workflow__pipeline_namespace,
                platform_workflow.runtime platform_workflow__runtime,
                count(distinct platform_workflow.workflow_id) platform_workflow__total_executions,
                cast(avg(platform_workflow.duration) as int) platform_workflow__average_duration,
                count(distinct succeeded_id) /
                (case when count(distinct workflow_id) > 0 then count(distinct workflow_id) else 1 end) platform_workflow__success_rate
            from
                (select	wf.*,
                    (case when wf.status = \'SUCCESS\' then workflow_id
                        else null end) as succeeded_id
                from \`codefresh-dev-170600.platform_workflow_benchmark.platform_workflow\` as wf) as platform_workflow
            where (platform_workflow.start_time >= TIMESTAMP ('${dateFrom.toISOString()}')
                    and platform_workflow.start_time <= TIMESTAMP('${dateTo.toISOString()}')
                and (platform_workflow.account_id = 'customer_${accountId}'))
            group by	1,	2,	3
            order by	4 desc`

enum QueryTypeEnum {
    global=  'global analytics  ' ,
    pipeline='pipeline analytics',
    top10=   'top10             '
}

const insertStatement =
    'insert into analytics.benchmark_table (database_type, account_type, account_id, start_time_From, start_time_to, exec_start_time,elapsed_time, num_of_records,query_type,pipeline_id, run_id)' +
    'values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, $11)'

enum DatabaseTypeEnum {
    postgres = 'postgres',
    bigQuery = 'bigQuery'
}

async function query(accountOld: number, dateFrom: Date, dateTo:Date, queryType: QueryTypeEnum, runId: any,  pipelineID?: number ) {
    //async function query() {
    // Queries the U.S. given names dataset for the state of Texas.
    const min = 1
    const max = 1000
    const account = Math.floor(Math.random() * (max - min + 1)) + min
    const client = await pool.connect()
    let queryToRun;

    if (queryType === QueryTypeEnum.global) {
        queryToRun = queryGlobalStats(account, dateFrom, dateTo)
    }
    else if ((queryType === QueryTypeEnum.pipeline) && (pipelineID) ){
        queryToRun = queryPipelineStats(account, dateFrom, dateTo, pipelineID)
    }
    else if (queryType === QueryTypeEnum.top10) {
        queryToRun = queryTop10Stats(account,dateFrom,dateTo)
    }

    //console.log(queryToRun)
    // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    const options = {
        query: queryToRun,
        // Location must match that of the dataset(s) referenced in the query.
        location: 'US',

    };
    const execStartTime = new Date().getTime();

    // Run the query as a job
    const [job] = await bigquery.createQueryJob(options);
    const jobCreationDuration = new Date().getTime() - execStartTime

    // Wait for the query to finish
    const [rows] = await job.getQueryResults();
    const afterGetQueryResults = new Date().getTime() - execStartTime
    let numOfRows = 0

    rows.forEach((row: any) => {
        numOfRows++;
    });

    const execDuration = new Date().getTime() - job.metadata.statistics.startTime
    const execDuration1 = new Date().getTime() - job.metadata.statistics.creationTime
    const execDuration2 = new Date().getTime() - execStartTime
    // console.log(job.metadata)
    console.log(`${queryType} - Account:${ (account<600)?"Small ":(account<900)?"Medium":"Large "} - ${account}, `+
        `TotalTime:${execDuration2.toString().padStart(5,' ')} ms, ` +
        // sinceStatsStartTime:${execDuration} ms, sinceStatsCreationTime: ${execDuration1} ms, `+
        `JobCreationTime:${jobCreationDuration.toString().padStart(5,' ')} ms, QueryResultsTime:${(afterGetQueryResults - jobCreationDuration).toString().padStart(5, ' ')} ms ` //+
        //`Days:${(dateTo.getTime()-dateFrom.getTime())/1000 /24/60/60}, `+
        +`NumOfRows:${numOfRows.toString().padStart(3, ' ')}`
    )
    const accountType = (account<600)?"Small":(account<900)?"Medium":"Large";

    const insertValues = [DatabaseTypeEnum.bigQuery, accountType, account, new Date(dateFrom), new Date(dateTo), new Date(execStartTime), execDuration2, numOfRows, queryType, pipelineID, runId]
    await client.query(insertStatement, insertValues, )
    await client.query('commit')
    client.release()
}
async function runQueries(): Promise<number>  {
    const dateStart = new Date(2021,11,11)
    const dateTo7Days = new Date(2021,11,18)
    const dateTo90Days = new Date(2022,2,10)
    const runId = uuidv4();

    const queryArgs = [
        [101, dateStart,  dateTo7Days, QueryTypeEnum.global, runId],
        [102, dateStart,  dateTo90Days, QueryTypeEnum.global, runId],
        [103, dateStart, dateTo7Days, QueryTypeEnum.top10, runId],
        [104, dateStart, dateTo90Days,QueryTypeEnum.top10, runId],
        [105, dateStart, dateTo90Days,QueryTypeEnum.pipeline, runId, 101],
        [105, dateStart, dateTo90Days,QueryTypeEnum.pipeline, runId, 1],
        [105, dateStart, dateTo90Days,QueryTypeEnum.pipeline, runId, 81],
        [701, dateStart,  dateTo7Days, QueryTypeEnum.global, runId],
        [702, dateStart,  dateTo90Days, QueryTypeEnum.global, runId],
        [703, dateStart, dateTo7Days, QueryTypeEnum.top10, runId],
        [704, dateStart, dateTo90Days,QueryTypeEnum.top10, runId],
        [105, dateStart, dateTo90Days,QueryTypeEnum.pipeline, runId, 1],
        [105, dateStart, dateTo90Days,QueryTypeEnum.pipeline, runId, 81],
        [705, dateStart, dateTo90Days,QueryTypeEnum.pipeline, runId, 101]
    ]

    queryArgs.forEach((queryArgs: any, ind) => {
        setTimeout(query, ind*300,...queryArgs);
    })

    //await Promise.all(promises)
    return 1
}

function main() {
    //runQueries()
    const numOfIterations = process.env.LOOP_COUNT ? process.env.LOOP_COUNT : 100
    for (let i = 0; i < numOfIterations; i++) {
        setTimeout(runQueries, i*1000)
    }
}

main();

