package io.mohs.store.jdbc;

import java.nio.file.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.sql.DataSource;
import com.zaxxer.hikari.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.*;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import io.github.robsonkades.uuidv7.UUIDv7;
import io.mohs.core.EngineState;
import io.mohs.core.definition.*;
import io.mohs.core.execution.*;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.*;
import io.mohs.engine.*;
import io.mohs.store.jdbc.delegate.*;
import static org.assertj.core.api.Assertions.assertThat;

/** Same source in all isolated candidates. Closed-loop storage workload, not a scheduler SLO. */
@EnabledIfSystemProperty(named="mohs.layout.measure", matches="true")
class WorkLayoutPerformanceScenario {
    static final int ROWS = Integer.getInteger("mohs.layout.rows", 1024);
    static final int RETAINED = Integer.getInteger("mohs.layout.retained", 4096);
    static final int BLOCKS = Integer.getInteger("mohs.layout.blocks", 3);
    static final int CALLERS = 4, CHUNK = 32;
    static final Path OUTPUT = Path.of(System.getProperty("mohs.layout.output", "target/work-layout"));
    static final Instant NOW = io.mohs.core.TimePrecision.observed(Instant.now().minusSeconds(5));
    static final com.sun.management.OperatingSystemMXBean CPU =
            (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();
    static final List<JobKey> JOBS = java.util.stream.IntStream.range(0,64).mapToObj(i -> JobKey.of("work-perf-"+i)).toList();

    @Test void compareEquivalentCompleteOperations() throws Exception {
        if(ROWS<128 || RETAINED<0 || BLOCKS<1) throw new IllegalArgumentException("invalid workload size");
        Files.createDirectories(OUTPUT);
        Files.writeString(OUTPUT.resolve("measurements.csv"),
                "cell,block,rows,retained_before,payload_bytes,jobs,callers,phase,elapsed_ms,executions_s,latency_samples,p50_ms,p95_ms,p99_ms,jdbc_exec,borrows,borrow_ms,cpu_ms,wait_samples,max_waiters,mean_waiters\n");
        var backend=ScenarioBackend.current();
        var config=new HikariConfig();config.setDataSource(backend.freshSchema());config.setMaximumPoolSize(16);
        config.setMinimumIdle(16);config.setConnectionTimeout(3000);config.setPoolName("layout-perf");
        try(var pool=new HikariDataSource(config)) {
            var connections=new ArrayList<Connection>();for(int i=0;i<16;i++)connections.add(pool.getConnection());
            for(var connection:connections)connection.close();
            var measured=new PerformanceDataSource(pool);
            var f=new Fixture(measured,backend.delegate());
            var tables=tables(pool);
            environment(pool,backend,tables);
            var seeded=accept(f,RETAINED,64,1024,false);
            drain(f,seeded,0,64,false);
            if(!Boolean.getBoolean("mohs.layout.skipPlans")) {
                var planRows=accept(f,256,64,1024,false);
                analyze(pool,backend,tables);
                plans(pool,backend,f,planRows);
                drain(f,planRows,0,64,false);
            }
            int retained=RETAINED+(Boolean.getBoolean("mohs.layout.skipPlans")?0:256);
            for(int bytes:new int[]{1024,65536}) for(int jobs:new int[]{1,64}) {
                String cell="payload-"+bytes+"-jobs-"+jobs;
                for(int block=0;block<=BLOCKS;block++) {
                    var before=measured.snapshot();long cpu=CPU.getProcessCpuTime(),start=System.nanoTime();
                    var births=accept(f,ROWS,jobs,bytes,true);long accepted=System.nanoTime();
                    var after=measured.snapshot();
                    if(block>0)report(cell,block,ROWS,retained,bytes,jobs,"accept",accepted-start,births.acceptNanos,
                            before,after,CPU.getProcessCpuTime()-cpu,new Waits(0,0,0));
                    Drain done;long elapsed,usedCpu;Waits waits;
                    try(var sampler=new LockSampler(pool,backend)) {
                        before=measured.snapshot();cpu=CPU.getProcessCpuTime();start=System.nanoTime();
                        done=drain(f,births,accepted,jobs,true);elapsed=System.nanoTime()-start;
                        usedCpu=CPU.getProcessCpuTime()-cpu;after=measured.snapshot();waits=sampler.snapshot();
                    }
                    if(block>0) {
                        report(cell,block,ROWS,retained,bytes,jobs,"drain",elapsed,done.completionNanos,before,after,usedCpu,waits);
                        Files.writeString(OUTPUT.resolve(cell+"-"+block+"-statements.txt"),delta(before,after));
                    }
                    for(String batch:births.batches) {
                        var counts=f.batches.find(batch).orElseThrow();
                        assertThat(counts.pending()).isZero();assertThat(counts.failed()).isZero();
                    }
                    retained+=ROWS;
                    assertThat(f.queue.countVisible(NOW)).isZero();
                    assertThat(f.history.countActiveByState(NOW).getOrDefault(ExecutionState.RUNNING,0L)).isZero();
                }
            }
            Files.writeString(OUTPUT.resolve("sizes.txt"),sizes(pool,backend));
        }
    }

    record Births(List<ExecutionId> ids, Set<String> retryIds, List<String> batches, long[] acceptNanos) {}
    record Drain(long[] completionNanos) {}

    static final class Fixture {
        final JdbcDelegate dialect;
        final JdbcSubmissionStore submit;
        final JdbcWorkQueue queue;
        final JdbcLeaseStore leases;
        final JdbcHistoryStore history;
        final JdbcBatchStore batches;
        final List<String> nodes=new ArrayList<>();
        Fixture(DataSource source,JdbcDelegate dialect) {
            this.dialect=dialect;
            var definitions=JOBS.stream().map(key -> JobDefinition.of(key.value(),new OnDemandSpec(),JobOptions.defaults().withRetries(1),Misfire.IGNORE,false)
                    .withInputContract("work-perf.v1")).toList();
            var contracts=new HashMap<JobKey,String>();JOBS.forEach(key->contracts.put(key,"work-perf.v1"));
            var catalog=new ExecutableCatalog(contracts);
            new JdbcCatalogStore(source,Clock.systemUTC(),dialect).reconcile(catalog,definitions,CatalogStore.Conflict.OVERRIDE);
            var registry=new JdbcNodeStore(source,dialect);
            for(int i=0;i<CALLERS;i++) {
                String id=UUIDv7.randomUUIDString();nodes.add(id);
                registry.register(id,EngineState.RUNNING,Duration.ofMinutes(30),catalog);
            }
            submit=new JdbcSubmissionStore(source,dialect,Duration.ofDays(7));
            queue=new JdbcWorkQueue(source,dialect);leases=new JdbcLeaseStore(source,dialect);
            history=new JdbcHistoryStore(source,dialect);batches=new JdbcBatchStore(source,dialect);
        }
    }

    static Births accept(Fixture f,int count,int jobs,int bytes,boolean retry) {
        var ids=new ArrayList<ExecutionId>(count);var retryIds=new HashSet<String>();var batches=new ArrayList<String>();
        var times=new ArrayList<Long>();
        // ASCII JSON has an exact byte count; payload is immutable for every attempt.
        var random=new SplittableRandom(42);String alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        var body=new StringBuilder(bytes-11);for(int i=0;i<bytes-11;i++)body.append(alphabet.charAt(random.nextInt(alphabet.length())));
        var payload=new JobPayload("{\"body\":\""+body+"\"}","work-perf.v1");
        for(int offset=0;offset<count;offset+=CHUNK) {
            int n=Math.min(CHUNK,count-offset);String batch=(offset/CHUNK)%2==0?UUIDv7.randomUUIDString():null;
            if(batch!=null)batches.add(batch);
            var items=new ArrayList<SubmissionStore.NewExecution>(n);
            for(int i=0;i<n;i++) {
                int index=offset+i;var id=ExecutionId.of(UUIDv7.randomUUIDString());ids.add(id);
                if(retry&&index%4==0)retryIds.add(id.value());
                items.add(new SubmissionStore.NewExecution(id,JOBS.get(index%jobs),0,20,NOW,NOW,"layout-measurement",batch,
                        batch==null?"request-"+id.value():null,payload,SubmissionStore.Origin.MANUAL,null));
            }
            long start=System.nanoTime();f.submit.accept(items,batch==null?null:new SubmissionStore.NewBatch(batch,"layout batch"),()->{});
            times.add(System.nanoTime()-start);
        }
        return new Births(List.copyOf(ids),Set.copyOf(retryIds),List.copyOf(batches),times.stream().mapToLong(Long::longValue).toArray());
    }

    static Drain drain(Fixture f,Births births,long accepted,int jobs,boolean measure) throws Exception {
        if(births.ids.isEmpty())return new Drain(new long[0]);
        var finished=new AtomicInteger();var durations=new long[births.ids.size()];
        var expected=new HashSet<>(births.ids);var completed=ConcurrentHashMap.<ExecutionId>newKeySet();
        var acquired=ConcurrentHashMap.<String>newKeySet();var start=new CountDownLatch(1);
        var failed=new AtomicBoolean();long deadline=System.nanoTime()+TimeUnit.MINUTES.toNanos(3);
        try(var workers=Executors.newFixedThreadPool(CALLERS)) {
            var tasks=new ArrayList<Future<?>>();
            for(String node:f.nodes)tasks.add(workers.submit(()->{
                try {
                    start.await();
                    while(finished.get()<births.ids.size()&&!failed.get()) {
                        if(System.nanoTime()>deadline)throw new IllegalStateException("drain made insufficient progress");
                        var work=f.queue.claim(0,node,CHUNK,List.of(),NOW);
                        if(work.isEmpty()){Thread.onSpinWait();continue;}
                        var payloads=f.history.findPayloads(work.stream().map(WorkQueue.ClaimedWork::executionId).toList());
                        var results=new ArrayList<LeaseStore.CompletionResult>();
                        for(var item:work) {
                            if(!expected.contains(item.executionId())||!acquired.add(item.executionId().value()+":"+item.attemptNumber()))
                                throw new IllegalStateException("unexpected or duplicated acquisition");
                            if(!payloads.rows().containsKey(item.executionId()))throw new IllegalStateException("accepted payload missing");
                            boolean again=item.attemptNumber()==1&&births.retryIds.contains(item.executionId().value());
                            results.add(new LeaseStore.CompletionResult(item.executionId(),item.jobKey(),node,item.claimToken(),item.attemptNumber(),
                                    NOW,NOW,again?ExecutionState.FAILED:ExecutionState.SUCCEEDED,again?"controlled.Retry":null,
                                    again?"retry 25 percent once":null,again?NOW:null));
                        }
                        var verdicts=f.leases.complete(results);
                        for(var result:results) {
                            if(!verdicts.get(result.executionId()).owned())throw new IllegalStateException("unconfirmed completion");
                            if(result.retryAt()==null) {
                                if(!completed.add(result.executionId()))throw new IllegalStateException("duplicate terminal result");
                                int slot=finished.getAndIncrement();durations[slot]=measure?System.nanoTime()-accepted:0;
                            }
                        }
                    }
                }catch(Exception e){failed.set(true);throw new CompletionException(e);}
            }));
            start.countDown();for(var task:tasks)task.get(4,TimeUnit.MINUTES);
        }
        assertThat(completed).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(acquired.size()).isEqualTo(births.ids.size()+births.retryIds.size());
        return new Drain(durations);
    }

    static void report(String cell,int block,int rows,int retained,int bytes,int jobs,String phase,long nanos,long[] times,
            PerformanceDataSource.Snapshot before,PerformanceDataSource.Snapshot after,long cpu,Waits waits) throws Exception {
        var sorted=times.clone();Arrays.sort(sorted);
        Files.writeString(OUTPUT.resolve("measurements.csv"),String.format(Locale.ROOT,
                "%s,%d,%d,%d,%d,%d,%d,%s,%.3f,%.3f,%d,%.3f,%.3f,%.3f,%d,%d,%.3f,%.3f,%d,%d,%.3f%n",
                cell,block,rows,retained,bytes,jobs,CALLERS,phase,nanos/1e6,rows*1e9/nanos,sorted.length,
                quantile(sorted,.5)/1e6,quantile(sorted,.95)/1e6,quantile(sorted,.99)/1e6,
                after.executions()-before.executions(),after.borrows()-before.borrows(),
                (after.borrowNanos()-before.borrowNanos())/1e6,cpu/1e6,waits.samples,waits.max,waits.samples==0?0:waits.sum/(double)waits.samples),StandardOpenOption.APPEND);
    }
    static long quantile(long[] sorted,double q){return sorted[Math.min(sorted.length-1,(int)Math.ceil(q*sorted.length)-1)];}
    static String delta(PerformanceDataSource.Snapshot before,PerformanceDataSource.Snapshot after) {
        var out=new StringBuilder();after.statements().forEach((sql,n)->{
            long change=n-before.statements().getOrDefault(sql,0L);if(change>0)out.append(change).append('\t').append(sql).append('\n');
        });return out.toString();
    }
    static List<String> tables(DataSource source)throws Exception {
        var names=new ArrayList<String>();try(var c=source.getConnection();var rows=c.getMetaData().getTables(c.getCatalog(),null,"%",new String[]{"TABLE"})) {
            while(rows.next()){String name=rows.getString("TABLE_NAME");if(name.startsWith("mohs_"))names.add(name);}
        }return names.stream().sorted().toList();
    }
    static void environment(DataSource source,ScenarioBackend backend,List<String> tables)throws Exception {
        try(var c=source.getConnection()) {
            var m=c.getMetaData();Files.writeString(OUTPUT.resolve("environment.txt"),
                    "captured="+Instant.now()+"\nbackend="+backend+"\ndatabase="+m.getDatabaseProductVersion()
                    +"\ndriver="+m.getDriverVersion()+"\njava="+System.getProperty("java.runtime.version")
                    +"\nos="+System.getProperty("os.name")+"\nprocessors="+Runtime.getRuntime().availableProcessors()
                    +"\nrows="+ROWS+"\nretained="+RETAINED+"\nblocks="+BLOCKS+"\ncallers="+CALLERS
                    +"\nshards=1 (deliberately contended)\nretry=25 percent, once\naccept_group_size="+CHUNK
                    +"\nHalf of groups are batches; half are independent executions with idempotency receipts."
                    +"\ninput_bytes=1024,65536; deterministic pseudorandom ASCII body, seed 42, to avoid a repeated-character compression artifact."
                    +"\ntables="+tables+"\nJDBC executes are API calls, not measured wire round trips."
                    +"\naccept quantiles measure 32-member transactions; drain quantiles measure completion after the full acceptance phase."
                    +"\nClosed loop, no handler effects, no open-arrival latency claim. Warmup block 0 excluded; retained rows grow identically across candidates."
                    +"\nDrain lock waiters sampled every 100 ms on an uninstrumented connection in the same pool. Sampling can miss short waits."
                    +"\nPostgres: pg_stat_activity Lock waiters in this database. SQL Server: blocked requests in this database. MySQL: global InnoDB current row-lock waiters in the isolated container."
                    +"\nWaiter values compare layouts within one engine, not lock semantics across engines. Zero samples on accept means not sampled.\n");
        }
    }
    static void analyze(HikariDataSource pool,ScenarioBackend backend,List<String> tables) {
        var jdbc=new JdbcTemplate(pool);for(String table:tables)jdbc.execute(switch(backend){
            case POSTGRES->"ANALYZE "+table;case MYSQL->"ANALYZE TABLE "+table;case SQLSERVER->"UPDATE STATISTICS "+table+" WITH FULLSCAN";
        });
    }
    record Waits(long samples,long max,long sum) {}
    static final class LockSampler implements AutoCloseable {
        final ScheduledExecutorService executor=Executors.newSingleThreadScheduledExecutor();
        final AtomicReference<Throwable> failure=new AtomicReference<>();
        final AtomicLong samples=new AtomicLong(),sum=new AtomicLong(),max=new AtomicLong();
        LockSampler(DataSource source,ScenarioBackend backend) {
            var jdbc=new JdbcTemplate(source);jdbc.setQueryTimeout(1);
            String query=switch(backend) {
                case POSTGRES->"SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'";
                case SQLSERVER->"SELECT COUNT_BIG(*) FROM sys.dm_exec_requests WHERE database_id=DB_ID() AND blocking_session_id<>0";
                case MYSQL->"SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_current_waits'";
            };
            executor.scheduleWithFixedDelay(()->{
                try {
                    long n=Objects.requireNonNull(jdbc.query(query,rs->{if(!rs.next())throw new SQLException("waiter counter missing");return rs.getLong(backend==ScenarioBackend.MYSQL?2:1);}));
                    samples.incrementAndGet();sum.addAndGet(n);max.accumulateAndGet(n,Math::max);
                }catch(Throwable problem){failure.compareAndSet(null,problem);}
            },0,100,TimeUnit.MILLISECONDS);
        }
        Waits snapshot(){return new Waits(samples.get(),max.get(),sum.get());}
        @Override public void close()throws Exception {
            executor.shutdown();if(!executor.awaitTermination(2,TimeUnit.SECONDS))throw new IllegalStateException("waiter sampler did not stop");
            if(failure.get()!=null)throw new IllegalStateException("waiter sampling failed",failure.get());
        }
    }
    static String sizes(HikariDataSource pool,ScenarioBackend backend) {
        var jdbc=new JdbcTemplate(pool);return jdbc.queryForList(switch(backend){
            case POSTGRES->"SELECT relname, pg_total_relation_size(oid) AS bytes FROM pg_class WHERE relname LIKE 'mohs_%' AND relkind = 'r' ORDER BY relname";
            case MYSQL->"SELECT table_name,data_length,index_length FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name LIKE 'mohs_%' ORDER BY table_name";
            case SQLSERVER->"SELECT OBJECT_NAME(p.object_id) AS table_name,SUM(p.reserved_page_count)*8192 AS bytes FROM sys.dm_db_partition_stats p JOIN sys.tables t ON t.object_id=p.object_id WHERE t.name LIKE 'mohs_%' GROUP BY p.object_id ORDER BY table_name";
        }).toString();
    }
    static void plans(HikariDataSource pool,ScenarioBackend backend,Fixture f,Births births)throws Exception {
        var ids=births.ids.stream().limit(CHUNK).map(ExecutionId::value).toList();
        var p=new MapSqlParameterSource().addValue("shard",0).addValue("now",f.dialect.splitTimestamp(NOW))
                .addValue("limit",(long)CHUNK,Types.BIGINT).addValue("ids",ids).addValue("inactive",false)
                .addValue("nodeId",f.nodes.getFirst()).addValue("claimToken",UUIDv7.randomUUIDString())
                .addValue("executionId",ids.getFirst()).addValue("revoked",true).addValue("grantedAt",f.dialect.splitTimestamp(NOW));
        capture(pool,backend,"admission",f.dialect.admissionCandidates(false),p);
        capture(pool,backend,"pick",f.dialect.readyCandidates(),p);
        if(backend==ScenarioBackend.POSTGRES)capture(pool,backend,"claim",PostgresJdbcDelegate.CLAIM_READY,p);
        if(backend==ScenarioBackend.SQLSERVER)capture(pool,backend,"claim",SqlServerJdbcDelegate.CLAIM_READY,p);
        capture(pool,backend,"detail",f.dialect.findExecutionById(),p);
        capture(pool,backend,"page",f.dialect.findExecutionPage(""),p);
        capture(pool,backend,"queue-count",f.dialect.countActiveInQueue(),p);
        capture(pool,backend,"recovery",f.dialect.findOrphanedLeases(),p);
    }
    static void capture(HikariDataSource pool,ScenarioBackend backend,String name,String query,MapSqlParameterSource params)throws Exception {
        var dir=OUTPUT.resolve("plans");Files.createDirectories(dir);Files.writeString(dir.resolve(name+".sql"),query+"\n-- "+params.getValues()+"\n");
        try(var c=pool.getConnection();var settings=c.createStatement()) {
            c.setAutoCommit(false);
            var jdbc=new NamedParameterJdbcTemplate(new SingleConnectionDataSource(c,true));
            try {
                if(backend==ScenarioBackend.POSTGRES||backend==ScenarioBackend.MYSQL) {
                    boolean dml=!query.stripLeading().startsWith("SELECT");
                    String prefix=backend==ScenarioBackend.POSTGRES?"EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT JSON) ":dml?"EXPLAIN FORMAT=JSON ":"EXPLAIN ANALYZE ";
                    var lines=jdbc.query(prefix+query,params,(rs,_)->rs.getString(1));
                    Files.writeString(dir.resolve(name+(backend==ScenarioBackend.POSTGRES?".json":".txt")),String.join("\n",lines));
                } else {
                    settings.execute("SET STATISTICS XML ON");settings.execute("SET STATISTICS IO ON");
                    var plan=new StringBuilder();var io=new StringBuilder();
                    jdbc.execute(query,params,(org.springframework.jdbc.core.PreparedStatementCallback<Void>)statement->{
                        boolean rows=statement.execute();while(true) {
                            if(rows)try(var result=statement.getResultSet()){
                                while(result.next())if(result.getMetaData().getColumnCount()==1) {
                                    String value=result.getString(1);if(value!=null&&value.contains("<ShowPlanXML"))plan.append(value).append('\n');
                                }
                            }else if(statement.getUpdateCount()==-1)break;
                            rows=statement.getMoreResults();
                        }
                        for(var warning=statement.getWarnings();warning!=null;warning=warning.getNextWarning())io.append(warning.getMessage()).append('\n');
                        return null;
                    });
                    assertThat(plan).isNotEmpty();Files.writeString(dir.resolve(name+".sqlplan"),plan);Files.writeString(dir.resolve(name+"-io.txt"),io);
                }
            } finally {
                c.rollback();
                if(backend==ScenarioBackend.SQLSERVER){settings.execute("SET STATISTICS XML OFF");settings.execute("SET STATISTICS IO OFF");}
                c.setAutoCommit(true);
            }
        }
    }
}
