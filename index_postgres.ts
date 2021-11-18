const { v4: uuidv4 } = require('uuid');

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


enum QueryTypeEnum {
    global=  'global analytics  ' ,
    pipeline='pipeline analytics',
    top10=   'top10             '
}

enum DatabaseTypeEnum {
    postgres = 'postgres',
    bigQuery = 'bitQuery'
}

const queryGlobalStats = (accountId: number, dateFrom: Date, dateTo:Date) =>
   `select
        DATE_TRUNC('day', platform_workflow.start_time) platform_workflow__time_day,
        count(distinct platform_workflow.workflow_id) platform_workflow__total_executions,
        avg(platform_workflow.duration) platform_workflow__average_duration,
        count(distinct platform_workflow.initiator) platform_workflow__committers,
        count(distinct succeeded_id)::decimal  /
        (case when count(distinct workflow_id) > 0 then count(distinct workflow_id)
            else 1 end) platform_workflow__success_rate
    from
        (select wf.*,
            (case when wf.status = 'SUCCESS' then workflow_id else null end) as succeeded_id
        from analytics.platform_workflow_partitioned_h as wf) as platform_workflow 
    where (platform_workflow.start_time >= TIMESTAMP '${dateFrom.toISOString()}'
           and platform_workflow.start_time <= TIMESTAMP '${dateTo.toISOString()}' )
    and	 platform_workflow.account_id = 'customer_${accountId}'
    group by 1
    order by 1 asc`

const queryPipelineStats = (accountId: number, dateFrom: Date, dateTo:Date, pipelineId: number) =>
    `select
	DATE_TRUNC('day', platform_workflow.start_time) platform_workflow__time_day,
	count(distinct platform_workflow.workflow_id) platform_workflow__total_executions,
	avg(platform_workflow.duration) platform_workflow__average_duration,
	count(distinct platform_workflow.initiator) platform_workflow__committers,
	count(distinct succeeded_id)::decimal  /
	(case
		when count(distinct workflow_id) > 0 then count(distinct workflow_id)
		else 1
	end) platform_workflow__success_rate
from
	(
	select wf.*,
		(case
			when wf.status = 'SUCCESS' then workflow_id
			else null
		end) as succeeded_id
	from
		analytics.platform_workflow_partitioned_h as wf) as platform_workflow 
where (platform_workflow.start_time >= TIMESTAMP '${dateFrom.toISOString()}'
           and platform_workflow.start_time <= TIMESTAMP '${dateTo.toISOString()}' )
    and	 platform_workflow.account_id = 'customer_${accountId}'
and  pipeline_name = 'pipeline_${pipelineId}'
group by 1
order by 1 asc`

const queryTop10Stats = (accountId: number, dateFrom: Date, dateTo:Date) =>
    `select
            platform_workflow.pipeline_name platform_workflow__pipeline_name,
            platform_workflow.pipeline_namespace platform_workflow__pipeline_namespace,
            platform_workflow.runtime platform_workflow__runtime,
            count(distinct platform_workflow.workflow_id) platform_workflow__total_executions,
            avg(platform_workflow.duration) platform_workflow__average_duration,
            count(distinct succeeded_id) /
            (case
                when count(distinct workflow_id) > 0 then count(distinct workflow_id)
                else 1
            end) platform_workflow__success_rate
    from
        (select	wf.*,
            (case when wf.status = 'SUCCESS' then workflow_id
                else null
            end) as succeeded_id
        from analytics.platform_workflow_partitioned_h as wf) as platform_workflow
    where (platform_workflow.start_time >= TIMESTAMP '${dateFrom.toISOString()}'
           and platform_workflow.start_time <= TIMESTAMP '${dateTo.toISOString()}' )
    and	 platform_workflow.account_id = 'customer_${accountId}'
    group by 1,	2,	3
    order by 5 desc`


function prepareQuery(account: number, dateFrom: Date, dateTo: Date, queryType: QueryTypeEnum, pipelineID: number | undefined) : string {
    let queryToRun: string = '';

    switch (queryType)
    {
        case QueryTypeEnum.global:
            queryToRun = queryGlobalStats(account, dateFrom, dateTo)
            break;
        case QueryTypeEnum.pipeline:
            if (pipelineID) {
                queryToRun = queryPipelineStats(account, dateFrom, dateTo, pipelineID)
            }
            break;
        case QueryTypeEnum.top10:
            queryToRun = queryTop10Stats(account,dateFrom,dateTo)
            break;
    }
    return queryToRun
}

async function query(dateFrom: Date, dateTo:Date, queryType: QueryTypeEnum, runId: any , pipelineID?: number) {
    const min = 1
    const max = 1000
    const account = Math.floor(Math.random() * (max - min + 1)) + min
    let totalTime, untilQueryTime;
    let queryToRun : string = prepareQuery(account, dateFrom, dateTo,queryType, pipelineID);
    const execStartTime = new Date().getTime();
    const execStartTimeDate = Date.now()
    const client = await pool.connect()
    const getClientFromPoolTime = new Date().getTime() - execStartTime
    let numOfRows = 0;
    try {
        const res = await client.query(queryToRun)
        untilQueryTime = new Date().getTime() - execStartTime
        res.rows.forEach((row:any) =>
        {
            numOfRows++
        })
        totalTime = new Date().getTime() - execStartTime ;
    } finally {
        // Make sure to release the client before any error handling,
        // just in case the error handling itself throws an error.
        client.release()
    }

    const accountType = (account<600)?"Small":(account<900)?"Medium":"Large";
    console.log(`${queryType} - Account:${accountType.padEnd(6,' ')} - ${account}, `+
        `TotalTime:${totalTime.toString().padStart(5,' ')} ms, getFromPoolTime:${getClientFromPoolTime.toString().padStart(5,' ')} ms, executeQueryTime:${(untilQueryTime - getClientFromPoolTime).toString().padStart(5,' ')}, `+
        `fetchTime:${(totalTime - untilQueryTime).toString().padStart(5,' ')}, numOfRows:${numOfRows.toString().padStart(3,' ')}`)


    const insertValues = [DatabaseTypeEnum.postgres, accountType, account, new Date(dateFrom), new Date(dateTo), new Date(execStartTimeDate), totalTime, numOfRows, queryType, pipelineID, runId]
    await client.query(insertStatement, insertValues, )
    await client.query('commit')
}
const insertStatement =
    'insert into analytics.benchmark_table (database_type, account_type, account_id, start_time_From, start_time_to, exec_start_time,elapsed_time, num_of_records,query_type,pipeline_id, run_id)' +
    'values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, $11)'

async function runQueries(): Promise<number>  {
    const dateStart = new Date(2021,11,11)
    const dateTo7Days = new Date(2021,11,18)
    const dateTo90Days = new Date(2022,2,10)
    const runId = uuidv4();
    const min = 1
    const max = 100
    const accountId = Math.floor(Math.random() * (max - min + 1)) + min
    const queryArgs = [
            [dateStart,  dateTo7Days, QueryTypeEnum.global, runId],
            [dateStart,  dateTo90Days, QueryTypeEnum.global, runId],
            [dateStart, dateTo7Days, QueryTypeEnum.top10, runId],
            [dateStart, dateTo90Days,QueryTypeEnum.top10, runId],
            [dateStart, dateTo90Days,QueryTypeEnum.pipeline, runId, 101],
            [dateStart, dateTo90Days,QueryTypeEnum.pipeline, runId, 1],
            [dateStart, dateTo90Days,QueryTypeEnum.pipeline, runId, 81],
            [dateStart,  dateTo7Days, QueryTypeEnum.global, runId],
            [dateStart,  dateTo90Days, QueryTypeEnum.global, runId],
            [dateStart, dateTo7Days, QueryTypeEnum.top10, runId],
            [dateStart, dateTo90Days,QueryTypeEnum.top10, runId],
            [dateStart, dateTo90Days,QueryTypeEnum.pipeline, runId, 1],
            [dateStart, dateTo90Days,QueryTypeEnum.pipeline, runId, 81],
            [dateStart, dateTo90Days,QueryTypeEnum.pipeline, runId, 101]
    ]

    queryArgs.forEach((queryArgs: any, ind) => {
        setTimeout(query, ind*300,...queryArgs);
    })


    return 1
}

function main() {
    //runQueries()

    const numOfIterations = process.env.LOOP_COUNT ? process.env.LOOP_COUNT : 100
    for (let i = 0; i < numOfIterations; i++) {
        setTimeout(runQueries, i * 1000) // runQueries()
    }
}

main();
