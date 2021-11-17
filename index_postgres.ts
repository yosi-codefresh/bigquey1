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

async function query(account: number, dateFrom: Date, dateTo:Date, queryType: QueryTypeEnum, pipelineID?: number ) {
    let totalTime, untilQueryTime;
    let queryToRun : string = prepareQuery(account, dateFrom, dateTo,queryType, pipelineID);
    const execStartTime = new Date().getTime();
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

    console.log(`${queryType} - Account:${ (account<600)?"Small ":(account<900)?"Medium":"Large "} - ${account}, `+
        `TotalTime:${totalTime.toString().padStart(5,' ')} ms, getFromPoolTime:${getClientFromPoolTime.toString().padStart(5,' ')} ms, executeQueryTime:${(untilQueryTime - getClientFromPoolTime).toString().padStart(5,' ')}, `+
        `fetchTime:${(totalTime - untilQueryTime).toString().padStart(5,' ')}, numOfRows:${numOfRows.toString().padStart(3,' ')}`)
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
    const numOfIterations = process.env.LOOP_COUNT ? process.env.LOOP_COUNT : 100
    for (let i = 0; i < numOfIterations; i++) {
        setTimeout(runQueries, i * 600) // runQueries()
    }
}

main();
