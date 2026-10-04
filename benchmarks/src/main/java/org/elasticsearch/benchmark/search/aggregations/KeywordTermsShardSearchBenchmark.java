/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.aggregations;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.PreallocatedCircuitBreakerService;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Runs a complete shard query with the ordinary query-phase hit and aggregation collectors. The
 * reader and global-ordinal cache stay warm between requests; each request builds fresh aggregation
 * state and uses the default hit and terms bucket sizes.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 1, time = 5)
public class KeywordTermsShardSearchBenchmark {
    private static final int DOCUMENT_COUNT = 100_000;
    private static final int SPARSE_MATCH_STRIDE = 1_000;
    private static final int SEGMENT_COUNT = 10;
    private static final String AGGREGATION_FIELD = "key";
    private static final String QUERY_FIELD = "select";
    private static final String INDEX_NAME = "terms-benchmark";
    private static final String INDEX_UUID = "terms-benchmark-uuid";
    private static final String AGGREGATION_NAME = "terms";

    @Param({ "sparse", "dense" })
    private String matchShape;

    @Param({ "false", "true" })
    private boolean trackRequestMemory;

    private Path indexPath;
    private Directory directory;
    private DirectoryReader reader;
    private PageCacheRecycler recycler;
    private BigArrays bigArrays;
    private HierarchyCircuitBreakerService breakerService;
    private IndicesFieldDataCache indicesFieldDataCache;
    private IndexFieldDataCache fieldDataCache;
    private SearchModule searchModule;
    private ClusterSettings clusterSettings;
    private MappedFieldType keywordFieldType;
    private SearchExecutionContext searchExecutionContext;
    private IndexShard indexShard;
    private ThreadPool threadPool;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        BenchmarkLogging.configure();
        indexPath = Files.createTempDirectory("keyword-terms-shard-search-");
        directory = MMapDirectory.open(indexPath);
        buildIndex();
        reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId(INDEX_NAME, INDEX_UUID, 0));
        if (reader.leaves().size() != SEGMENT_COUNT) {
            throw new IllegalStateException("expected [" + SEGMENT_COUNT + "] leaves but got [" + reader.leaves().size() + "]");
        }

        clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        recycler = new PageCacheRecycler(Settings.EMPTY);
        breakerService = new HierarchyCircuitBreakerService(CircuitBreakerMetrics.NOOP, Settings.EMPTY, List.of(), clusterSettings);
        bigArrays = new BigArrays(recycler, breakerService, CircuitBreaker.REQUEST);
        indicesFieldDataCache = new IndicesFieldDataCache(Settings.EMPTY, new IndexFieldDataCache.Listener() {});
        fieldDataCache = indicesFieldDataCache.buildIndexFieldDataCache(
            new IndexFieldDataCache.Listener() {},
            new Index(INDEX_NAME, INDEX_UUID),
            AGGREGATION_FIELD
        );
        searchModule = new SearchModule(Settings.EMPTY, List.of());
        keywordFieldType = new KeywordFieldMapper.KeywordFieldType(AGGREGATION_FIELD);
        searchExecutionContext = mock(SearchExecutionContext.class);
        indexShard = mock(IndexShard.class);
        threadPool = mock(ThreadPool.class);
        when(indexShard.getThreadPool()).thenReturn(threadPool);
        when(threadPool.executor(ThreadPool.Names.SEARCH)).thenReturn(null);

        QuerySearchResult warmResult = runShardQuery(query(), trackRequestMemory ? new RequestMemoryTracker() : null);
        verifyResult(warmResult);
    }

    @Benchmark
    public QuerySearchResult shardQuery(RequestMemoryCounters counters) throws Exception {
        RequestMemoryTracker memoryTracker = trackRequestMemory ? new RequestMemoryTracker() : null;
        long startThreadCpuNanos = counters.currentThreadCpuTime();
        QuerySearchResult result = runShardQuery(query(), memoryTracker);
        counters.record(counters.currentThreadCpuTime() - startThreadCpuNanos, memoryTracker == null ? 0 : memoryTracker.peakBytes());
        return result;
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (reader != null) {
            reader.close();
        }
        if (indicesFieldDataCache != null) {
            indicesFieldDataCache.close();
        }
        if (directory != null) {
            directory.close();
        }
        if (indexPath != null) {
            try (var files = Files.walk(indexPath)) {
                files.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    private Query query() {
        return matchShape.equals("sparse") ? new TermQuery(new Term(QUERY_FIELD, "yes")) : new MatchAllDocsQuery();
    }

    private QuerySearchResult runShardQuery(Query query, RequestMemoryTracker memoryTracker) throws Exception {
        final ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            new BM25Similarity(),
            null,
            TrivialQueryCachingPolicy.NEVER,
            false
        );
        final AggregatorFactories.Builder factoriesBuilder = new AggregatorFactories.Builder().addAggregator(
            new TermsAggregationBuilder(AGGREGATION_NAME).field(AGGREGATION_FIELD)
        );
        try (
            searcher;
            BenchmarkAggregationContext aggregationContext = new BenchmarkAggregationContext(
                searcher,
                query,
                trackRequestMemory,
                memoryTracker,
                factoriesBuilder.bytesToPreallocate()
            )
        ) {
            AggregatorFactories factories = factoriesBuilder.build(aggregationContext, null);
            BenchmarkSearchContext searchContext = new BenchmarkSearchContext(searchExecutionContext, indexShard, searcher);
            searchContext.parsedQuery(new ParsedQuery(query));
            searchContext.setSize(SearchService.DEFAULT_SIZE);
            searchContext.aggregations(new SearchContextAggregations(factories, () -> {
                throw new AssertionError("unexpected parallel query-phase reduction");
            }));
            try (searchContext) {
                QueryPhase.execute(searchContext);
                if (searcher.timeExceeded()) {
                    throw new IllegalStateException("unexpected timeout");
                }
                return searchContext.queryResult();
            }
        }
    }

    private void verifyResult(QuerySearchResult result) {
        if (result.topDocs().topDocs.scoreDocs.length != SearchService.DEFAULT_SIZE) {
            throw new IllegalStateException("expected the default [" + SearchService.DEFAULT_SIZE + "] shard hits");
        }
        DelayableWriteable<org.elasticsearch.search.aggregations.InternalAggregations> delayedAggregations = result.aggregations();
        if (delayedAggregations == null) {
            throw new IllegalStateException("query phase did not build aggregations");
        }
        StringTerms terms = delayedAggregations.expand().get(AGGREGATION_NAME);
        int expectedBuckets = 25;
        long expectedOtherDocCount = matchShape.equals("sparse") ? 75 : DOCUMENT_COUNT - expectedBuckets;
        if (terms.getBuckets().size() != expectedBuckets || terms.getSumOfOtherDocCounts() != expectedOtherDocCount) {
            throw new IllegalStateException(
                "unexpected shard terms result: buckets [" + terms.getBuckets().size() + "], other [" + terms.getSumOfOtherDocCounts() + "]"
            );
        }
        for (int bucket = 0; bucket < expectedBuckets; bucket++) {
            String expectedKey = "term-" + String.format(java.util.Locale.ROOT, "%06d", bucket * (matchShape.equals("sparse") ? 1000 : 1));
            StringTerms.Bucket actual = terms.getBuckets().get(bucket);
            if (expectedKey.equals(actual.getKeyAsString()) == false || actual.getDocCount() != 1L) {
                throw new IllegalStateException(
                    "unexpected shard bucket at [" + bucket + "]: [" + actual.getKeyAsString() + "] count [" + actual.getDocCount() + "]"
                );
            }
        }
    }

    private void buildIndex() throws IOException {
        IndexWriterConfig writerConfig = new IndexWriterConfig(new KeywordAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            .setUseCompoundFile(false);
        try (IndexWriter writer = new IndexWriter(directory, writerConfig)) {
            int docsPerSegment = DOCUMENT_COUNT / SEGMENT_COUNT;
            for (int segment = 0; segment < SEGMENT_COUNT; segment++) {
                int end = (segment + 1) * docsPerSegment;
                for (int docId = segment * docsPerSegment; docId < end; docId++) {
                    String key = "term-" + String.format(java.util.Locale.ROOT, "%06d", docId);
                    Document document = new Document();
                    document.add(new StringField(AGGREGATION_FIELD, key, Field.Store.NO));
                    document.add(new SortedSetDocValuesField(AGGREGATION_FIELD, new BytesRef(key)));
                    document.add(new StringField(QUERY_FIELD, docId % SPARSE_MATCH_STRIDE == 0 ? "yes" : "no", Field.Store.NO));
                    writer.addDocument(document);
                }
                writer.commit();
            }
        }
    }

    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class RequestMemoryCounters {
        private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        public long query_thread_cpu_ns;
        public long request_peak_bytes;
        private long queryCount;

        @Setup(Level.Iteration)
        public void reset() {
            request_peak_bytes = 0;
            query_thread_cpu_ns = 0;
            queryCount = 0;
            if (threadMXBean.isThreadCpuTimeSupported() == false) {
                throw new IllegalStateException("thread CPU time is unavailable");
            }
            if (threadMXBean.isThreadCpuTimeEnabled() == false) {
                threadMXBean.setThreadCpuTimeEnabled(true);
            }
        }

        void record(long queryCpuNanos, long requestPeakBytes) {
            query_thread_cpu_ns += queryCpuNanos;
            request_peak_bytes += requestPeakBytes;
            queryCount++;
        }

        @TearDown(Level.Iteration)
        public void normalizeEventsPerQuery() {
            if (queryCount <= 0) {
                throw new IllegalStateException("no query operations were measured");
            }
            query_thread_cpu_ns /= queryCount;
            request_peak_bytes /= queryCount;
        }

        long currentThreadCpuTime() {
            long cpuNanos = threadMXBean.getCurrentThreadCpuTime();
            if (cpuNanos < 0) {
                throw new IllegalStateException("thread CPU time is unavailable");
            }
            return cpuNanos;
        }
    }

    private static final class RequestMemoryTracker {
        private long currentBytes;
        private long peakBytes;

        synchronized void add(long bytes) {
            currentBytes += bytes;
            if (currentBytes < 0) {
                throw new IllegalStateException("request memory accounting underflow [" + currentBytes + "]");
            }
            peakBytes = Math.max(peakBytes, currentBytes);
        }

        synchronized long peakBytes() {
            return peakBytes;
        }
    }

    private static final class TrackingPreallocatedCircuitBreakerService extends PreallocatedCircuitBreakerService {
        private final CircuitBreaker requestBreaker;

        TrackingPreallocatedCircuitBreakerService(
            CircuitBreakerService delegate,
            long bytesToPreallocate,
            RequestMemoryTracker memoryTracker
        ) {
            super(delegate, CircuitBreaker.REQUEST, bytesToPreallocate, "aggregations");
            requestBreaker = new TrackingCircuitBreaker(super.getBreaker(CircuitBreaker.REQUEST), memoryTracker);
        }

        @Override
        public CircuitBreaker getBreaker(String name) {
            return CircuitBreaker.REQUEST.equals(name) ? requestBreaker : super.getBreaker(name);
        }
    }

    private static final class TrackingCircuitBreaker implements CircuitBreaker {
        private final CircuitBreaker delegate;
        private final RequestMemoryTracker memoryTracker;

        TrackingCircuitBreaker(CircuitBreaker delegate, RequestMemoryTracker memoryTracker) {
            this.delegate = delegate;
            this.memoryTracker = memoryTracker;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            delegate.circuitBreak(fieldName, bytesNeeded);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            delegate.addEstimateBytesAndMaybeBreak(bytes, label);
            memoryTracker.add(bytes);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            delegate.addWithoutBreaking(bytes);
            memoryTracker.add(bytes);
        }

        @Override
        public void addWithoutBreaking(long bytes, String label) {
            delegate.addWithoutBreaking(bytes, label);
            memoryTracker.add(bytes);
        }

        @Override
        public long getUsed() {
            return delegate.getUsed();
        }

        @Override
        public long getLimit() {
            return delegate.getLimit();
        }

        @Override
        public double getOverhead() {
            return delegate.getOverhead();
        }

        @Override
        public long getTrippedCount() {
            return delegate.getTrippedCount();
        }

        @Override
        public String getName() {
            return delegate.getName();
        }

        @Override
        public Durability getDurability() {
            return delegate.getDurability();
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            delegate.setLimitAndOverhead(limit, overhead);
        }
    }

    private final class BenchmarkAggregationContext extends AggregationContext {
        private final Query query;
        private final ContextIndexSearcher searcher;
        private final List<Aggregator> releaseMe = new ArrayList<>();
        private final PreallocatedCircuitBreakerService preallocatedBreakerService;
        private final BigArrays requestBigArrays;
        private final CircuitBreaker requestBreaker;

        BenchmarkAggregationContext(
            ContextIndexSearcher searcher,
            Query query,
            boolean captureRequestMemory,
            RequestMemoryTracker memoryTracker,
            long bytesToPreallocate
        ) {
            this.searcher = searcher;
            this.query = query;
            if (bytesToPreallocate == 0) {
                preallocatedBreakerService = null;
                requestBigArrays = bigArrays.withCircuitBreaking();
            } else if (captureRequestMemory) {
                TrackingPreallocatedCircuitBreakerService tracked = new TrackingPreallocatedCircuitBreakerService(
                    breakerService,
                    bytesToPreallocate,
                    memoryTracker
                );
                preallocatedBreakerService = tracked;
                requestBigArrays = bigArrays.withBreakerService(tracked).withCircuitBreaking();
            } else {
                preallocatedBreakerService = new PreallocatedCircuitBreakerService(
                    breakerService,
                    CircuitBreaker.REQUEST,
                    bytesToPreallocate,
                    "aggregations"
                );
                requestBigArrays = bigArrays.withBreakerService(preallocatedBreakerService).withCircuitBreaking();
            }
            requestBreaker = requestBigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        }

        @Override
        public Query query() {
            return query;
        }

        @Override
        public Aggregator profileIfEnabled(Aggregator aggregator) {
            return aggregator;
        }

        @Override
        public boolean profiling() {
            return false;
        }

        @Override
        public long nowInMillis() {
            return 0;
        }

        @Override
        public org.apache.lucene.analysis.Analyzer getNamedAnalyzer(String analyzer) {
            return null;
        }

        @Override
        public org.apache.lucene.analysis.Analyzer buildCustomAnalyzer(
            org.elasticsearch.index.IndexSettings indexSettings,
            boolean normalizer,
            org.elasticsearch.index.analysis.NameOrDefinition tokenizer,
            List<org.elasticsearch.index.analysis.NameOrDefinition> charFilters,
            List<org.elasticsearch.index.analysis.NameOrDefinition> tokenFilters
        ) {
            return null;
        }

        @Override
        protected IndexFieldData<?> buildFieldData(MappedFieldType fieldType) {
            return fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields(INDEX_NAME, "terms shard benchmark"))
                .build(fieldDataCache, breakerService);
        }

        @Override
        public MappedFieldType getFieldType(String path) {
            if (AGGREGATION_FIELD.equals(path)) {
                return keywordFieldType;
            }
            throw new UnsupportedOperationException("unsupported field [" + path + "]");
        }

        @Override
        public Set<String> getMatchingFieldNames(String pattern) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.elasticsearch.search.lookup.SearchLookup lookup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ValuesSourceRegistry getValuesSourceRegistry() {
            return searchModule.getValuesSourceRegistry();
        }

        @Override
        public BigArrays bigArrays() {
            return requestBigArrays;
        }

        @Override
        public org.apache.lucene.search.IndexSearcher searcher() {
            return searcher;
        }

        @Override
        public Query buildQuery(org.elasticsearch.index.query.QueryBuilder builder) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Query filterQuery(Query query) {
            return query;
        }

        @Override
        public org.elasticsearch.index.IndexSettings getIndexSettings() {
            return null;
        }

        @Override
        public ClusterSettings getClusterSettings() {
            return clusterSettings;
        }

        @Override
        public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sortBuilders) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.elasticsearch.index.mapper.NestedLookup nestedLookup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.elasticsearch.index.query.support.NestedScope nestedScope() {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.elasticsearch.search.internal.SubSearchContext subSearchContext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addReleasable(Aggregator aggregator) {
            releaseMe.add(aggregator);
        }

        @Override
        public void removeReleasable(Aggregator aggregator) {
            releaseMe.remove(aggregator);
        }

        @Override
        public int maxBuckets() {
            return Integer.MAX_VALUE;
        }

        @Override
        public org.elasticsearch.index.cache.bitset.BitsetFilterCache bitsetFilterCache() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BucketedSort buildBucketedSort(SortBuilder<?> sort, int size, BucketedSort.ExtraData extra) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int shardRandomSeed() {
            return 0;
        }

        @Override
        public long getRelativeTimeInMillis() {
            return 0;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public CircuitBreaker breaker() {
            return requestBreaker;
        }

        @Override
        public org.apache.lucene.analysis.Analyzer getIndexAnalyzer(
            Function<String, org.elasticsearch.index.analysis.NamedAnalyzer> analyzer
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCacheable() {
            return false;
        }

        @Override
        public boolean enableRewriteToFilterByFilter() {
            return true;
        }

        @Override
        public boolean isInSortOrderExecutionRequired() {
            return false;
        }

        @Override
        public Set<String> sourcePath(String fullName) {
            return Set.of(fullName);
        }

        @Override
        public void close() {
            List<org.elasticsearch.core.Releasable> releasables = new ArrayList<>(releaseMe);
            if (preallocatedBreakerService != null) {
                releasables.add(preallocatedBreakerService);
            }
            Releasables.close(releasables);
        }
    }

    private static final class BenchmarkSearchContext extends TestSearchContext {
        private final IndexShard indexShard;

        BenchmarkSearchContext(SearchExecutionContext searchExecutionContext, IndexShard indexShard, ContextIndexSearcher searcher) {
            super(searchExecutionContext, null, searcher);
            this.indexShard = indexShard;
        }

        @Override
        public IndexShard indexShard() {
            return indexShard;
        }

        @Override
        public org.elasticsearch.core.TimeValue timeout() {
            return null;
        }
    }
}
