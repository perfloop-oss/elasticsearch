# Sparse keyword terms benchmark

Status: recorded proof with reproduction commands, October 4, 2026.

Found with [Perfloop](https://app.perfloop.ai/t/oss/case_vycxvq13jv).

This branch contains the measured repair and the exact JMH source used for
its accepted comparison. The benchmark uses a retained 100,000-term reader
with ten segments. Sparse searches match 100 documents. Dense searches match
all documents. Each operation runs QueryPhase collection and result
construction with production BigArrays and PageCacheRecycler. It checks the
initial buckets, counts and sum_other_doc_count. HTTP handling, coordinator
reduction and service latency are outside this measurement.

## Reproduce

Use full checkouts and the source-pinned JDKs. Recorded runs used Linux
x86_64 and JDK 27+35. The Gradle wrapper provisions the toolchains.

```sh
git clone https://github.com/perfloop-oss/elasticsearch.git elasticsearch-terms
cd elasticsearch-terms
git checkout bench/sparse-keyword-terms-repro
git diff 8b2333e50ce86cf171d62f74d675991663a7eabc HEAD -- \
  benchmarks/build.gradle \
  benchmarks/src/main/java/org/elasticsearch/benchmark/search/aggregations/KeywordTermsShardSearchBenchmark.java \
  > ../sparse-keyword-benchmark.patch
git worktree add ../elasticsearch-terms-base 8b2333e50ce86cf171d62f74d675991663a7eabc
git -C ../elasticsearch-terms-base apply ../sparse-keyword-benchmark.patch
```

Run these commands in each checkout. Each writes its JSON result in that
checkout's `benchmarks` directory. Alternate baseline and repair runs ten
times on the same quiet host. Keep each pair's JSON files.

```sh
./gradlew -p benchmarks -Druntime.java=27 run \
  --args='KeywordTermsShardSearchBenchmark.shardQuery -f 1 -wi 10 -w 2s -i 1 -r 5s -bm avgt -tu ns -t 1 -p matchShape=sparse -p trackRequestMemory=false -rf json -rff terms-sparse.json'
./gradlew -p benchmarks -Druntime.java=27 run \
  --args='KeywordTermsShardSearchBenchmark.shardQuery -f 1 -wi 10 -w 2s -i 1 -r 20s -bm avgt -tu ns -t 1 -p matchShape=dense -p trackRequestMemory=false -rf json -rff terms-dense.json'
./gradlew -p benchmarks -Druntime.java=27 run \
  --args='KeywordTermsShardSearchBenchmark.shardQuery -f 1 -wi 10 -w 2s -i 1 -r 5s -bm avgt -tu ns -t 1 -p matchShape=dense -p trackRequestMemory=true -prof gc -rf json -rff terms-memory.json'
```

Read `query_thread_cpu_ns` for query-thread CPU, `ns/op` for the complete
benchmark operation, `request_peak_bytes` for peak request-breaker accounting,
and `gc.alloc.rate.norm` for allocated bytes per operation. JMH reports the
two auxiliary counters with unit `#`; their values are ns/query and B/query.
The benchmark normalizes these counters by the number of query operations.

## Recorded result

These are the accepted runs on baseline `8b2333e5` and Candidate
`8b5499155f51ac2745a1e5045d8d01bebdaa336f`, with proof source
`968175a49f07b476d79c45ddeebe7d82c2b6e114`.

| Metric | Baseline median | Repair median | Paired change |
| --- | ---: | ---: | ---: |
| Sparse query-thread CPU | 483,855 ns | 201,339 ns | 283,196 ns saved, 58.5% |
| Sparse benchmark operation | 523,681 ns | 224,064 ns | 301,163 ns saved |
| Dense query-thread CPU | 3,376,051 ns | 3,420,616 ns | 38,093 ns added; interval includes zero |
| Dense benchmark operation | 3,573,880 ns | 3,623,037 ns | 52,438 ns added; interval includes zero |
| Dense request peak | 808,480 B | 812,288 B | 3,808 B added |
| Dense allocation | 657,628 B/op | 679,559 B/op | 21,807 B/op added, about 3.3% |

The sparse CPU saving interval is 278,577 to 292,109 ns across ten pairs.
The recorded controls passed their declared bounds. They still show added
memory cost. Production frequency and service-latency effects are unmeasured.

The contribution PR removes one always-true predicate from the measured
repair. This benchmark branch retains the measured code. No new Java test or
benchmark ran on the contribution revision on the preparation host. To measure
that revision, apply the same two-file benchmark overlay to its checkout and
repeat the commands above. Full upstream CI is pending.
