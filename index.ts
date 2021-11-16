import {QueryOptions} from "@google-cloud/bigquery";
const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();

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
async function query(account: number, dateFrom: Date, dateTo:Date, queryType: QueryTypeEnum, pipelineID?: number ) {
    //async function query() {
    // Queries the U.S. given names dataset for the state of Texas.

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
        `TotalTime:${execDuration2} ms, ` +
        // sinceStatsStartTime:${execDuration} ms, sinceStatsCreationTime: ${execDuration1} ms, `+
        `JobCreationTime:${jobCreationDuration} ms, QueryResultsTime:${afterGetQueryResults - jobCreationDuration} ms ` //+
        //`Days:${(dateTo.getTime()-dateFrom.getTime())/1000 /24/60/60}, `+
        //`NumOfRows:${numOfRows}`
    )
}
async function runQueries(): Promise<number>  {
    const dateStart = new Date(2021,11,11)
    const dateTo7Days = new Date(2021,11,18)
    const dateTo90Days = new Date(2022,2,10)

    const promises = [
        query(101, dateStart,  dateTo7Days, QueryTypeEnum.global),
        query(102, dateStart,  dateTo90Days, QueryTypeEnum.global),
        query(103, dateStart, dateTo7Days, QueryTypeEnum.top10),
        query(104, dateStart, dateTo90Days,QueryTypeEnum.top10),
        query(105, dateStart, dateTo90Days,QueryTypeEnum.pipeline, 101),
        query(105, dateStart, dateTo90Days,QueryTypeEnum.pipeline, 1),
        query(105, dateStart, dateTo90Days,QueryTypeEnum.pipeline, 81),
        query(701, dateStart,  dateTo7Days, QueryTypeEnum.global),
        query(702, dateStart,  dateTo90Days, QueryTypeEnum.global),
        query(703, dateStart, dateTo7Days, QueryTypeEnum.top10),
        query(704, dateStart, dateTo90Days,QueryTypeEnum.top10),
        query(105, dateStart, dateTo90Days,QueryTypeEnum.pipeline, 1),
        query(105, dateStart, dateTo90Days,QueryTypeEnum.pipeline, 81),
        query(705, dateStart, dateTo90Days,QueryTypeEnum.pipeline, 101)
    ]
    await Promise.all(promises)
    return 1
}

function main() {
    //runQueries()
    for (let i = 0; i < 5; i++) {
        runQueries()
    }
}

main();

