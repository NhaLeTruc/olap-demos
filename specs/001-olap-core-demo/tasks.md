# Tasks: OLAP Core Capabilities Tech Demo

**Input**: Design documents from `/specs/001-olap-core-demo/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Benchmarks are REQUIRED per Constitution Principle III (Benchmark-Driven Development). All benchmarks must be written BEFORE implementation and must FAIL initially.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3, US4)
- Include exact file paths in descriptions

## Path Conventions

- **Single project**: `src/`, `tests/` at repository root (this project)
- Parquet data: `data/parquet/`
- Benchmarks: `tests/benchmarks/`
- Integration tests: `tests/integration/`
- Unit tests: `tests/unit/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [X] T001 Create project directory structure per plan.md (src/, tests/, data/, docs/, benchmark-results/)
- [X] T002 Initialize Python project with pyproject.toml including dependencies (duckdb>=0.9.0, pyarrow>=14.0.0, pandas>=2.1.0, faker>=22.0.0, pytest>=7.4.0, pytest-benchmark>=4.0.0, pytest-xdist>=3.5.0, black>=23.0.0, ruff>=0.1.0)
- [X] T003 [P] Create .python-version file specifying Python 3.11+
- [X] T004 [P] Create pytest.ini with benchmark configuration (disable_gc=true, min_rounds=5)
- [X] T005 [P] Create .gitignore to exclude data/, __pycache__, *.pyc, .pytest_cache, .duckdb
- [X] T006 [P] Configure ruff linting rules in pyproject.toml
- [X] T007 [P] Configure black formatting in pyproject.toml
- [X] T008 [P] Create README.md with project overview and quickstart reference
- [X] T009 [P] Create benchmark-results/README.md documenting benchmark tracking approach

**Checkpoint**: Project structure created, dependencies configured, ready for implementation

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

### Data Model Foundation

- [X] T010 [P] Create src/models/__init__.py
- [X] T011 [P] Define dim_time schema in src/models/dimensions.py (time_key, date, year, quarter, month, day, is_weekend, is_holiday)
- [X] T012 [P] Define dim_geography schema in src/models/dimensions.py (geo_key, region, country, state, city, lat, lon)
- [X] T013 [P] Define dim_product schema in src/models/dimensions.py with SCD Type 2 (product_key, product_id, name, category, subcategory, brand, effective_date, expiration_date, is_current)
- [X] T014 [P] Define dim_customer schema in src/models/dimensions.py (customer_key, customer_id, segment, channel, ltv_tier)
- [X] T015 [P] Define dim_payment schema in src/models/dimensions.py (payment_key, payment_type, provider, fee_percent)
- [X] T016 Define sales_fact schema in src/models/facts.py (transaction_id, line_item_id, date, time_key, geo_key, product_key, customer_key, payment_key, quantity, unit_price, revenue, cost, discount_amount, profit)

### Data Generation Infrastructure

- [X] T017 [P] Create src/datagen/__init__.py
- [X] T018 [P] Implement schema validation utilities in src/datagen/schemas.py (validate_constraints, check_referential_integrity)
- [X] T019 Implement deterministic data generator base class in src/datagen/generator.py (set_seed, generate_range)
- [X] T020 [P] Implement dim_time generator in src/datagen/generator.py (generate 3 years: 2021-2023, ~1095 rows)
- [X] T021 [P] Implement dim_geography generator in src/datagen/generator.py (hierarchical: regions → countries → cities, ~5000 rows)
- [X] T022 [P] Implement dim_product generator in src/datagen/generator.py with SCD Type 2 support (~10K-15K rows with history)
- [X] T023 [P] Implement dim_customer generator in src/datagen/generator.py with realistic segments (~1M rows)
- [X] T024 [P] Implement dim_payment generator in src/datagen/generator.py (~20 rows)
- [X] T025 Implement sales_fact generator in src/datagen/generator.py with foreign key linkage, Pareto distribution for products, configurable row count

### Storage Infrastructure

- [X] T026 [P] Create src/storage/__init__.py
- [X] T027 [P] Implement Parquet writer in src/storage/parquet_handler.py (write_table, compression=snappy, row_group_size)
- [X] T028 [P] Implement Parquet reader in src/storage/parquet_handler.py (read_table, columns parameter for selective reads)
- [X] T029 [P] Implement CSV writer in src/storage/csv_handler.py (for comparison baseline)
- [X] T030 [P] Implement CSV reader in src/storage/csv_handler.py
- [X] T031 Implement Hive-style partitioning in src/storage/partitioning.py (partition_by_keys, write_partitioned, supported keys: year, quarter, month)
- [X] T032 Implement partition metadata tracking in src/storage/partitioning.py (list_partitions, get_partition_stats)

### Data Loading Infrastructure

- [X] T033 Create src/datagen/loaders.py
- [X] T034 Implement DuckDB table loader in src/datagen/loaders.py (load_parquet_to_duckdb, create_tables)
- [X] T035 Implement bulk loading with progress tracking in src/datagen/loaders.py (parallel_load, batch_size=1000000)

### Query Engine Infrastructure

- [X] T036 [P] Create src/query/__init__.py
- [X] T037 Implement DuckDB connection manager in src/query/engine.py (get_connection, set_threads, set_memory_limit)
- [X] T038 Implement query executor in src/query/engine.py (execute_query, execute_explain_analyze)
- [X] T039 [P] Implement query profiler in src/query/profiler.py (extract_metrics: execution_time, rows_scanned, bytes_scanned, partitions_accessed)
- [X] T040 [P] Implement structured logging for queries in src/query/profiler.py (log_query_execution: query_id, timestamp, duration, result_rows)

### CLI Infrastructure

- [X] T041 [P] Create src/cli/__init__.py
- [X] T042 Implement generate command skeleton in src/cli/generate.py (Click-based CLI with options: --rows, --seed, --format, --partition-by, --output-dir)
- [X] T043 [P] Implement benchmark command skeleton in src/cli/benchmark.py (options: --rounds, --baseline, --fail-on-regression, --output)
- [X] T044 [P] Implement analyze command skeleton in src/cli/analyze.py (options: --execute, --profile, --format)

### Testing Infrastructure

- [X] T045 [P] Create tests/benchmarks/__init__.py
- [X] T046 [P] Create tests/integration/__init__.py
- [X] T047 [P] Create tests/unit/__init__.py
- [X] T048 Create pytest fixtures in tests/conftest.py (duckdb_conn, sample_data_10k, benchmark_config)
- [X] T049 [P] Implement benchmark helper utilities in tests/conftest.py (assert_sla, compare_to_baseline)

**Checkpoint**: Foundation ready - dimension generators, storage handlers, query engine, CLI skeletons complete. User story implementation can now begin in parallel.

---

## Phase 3: User Story 1 - Multi-Dimensional Sales Analysis (Priority: P1) 🎯 MVP

**Goal**: Fast multi-dimensional aggregations across time, geography, product, customer dimensions with partition pruning

**Independent Test**: Run dimension-aware queries on 100M row dataset, measure response time (p95 <2s), verify partition pruning in EXPLAIN plans

### Benchmarks for User Story 1 (REQUIRED - Benchmark-Driven Development) ⚠️

> **Constitution Principle III**: Benchmarks MUST be written FIRST, FAIL initially, then implementation makes them pass

- [X] T050 [P] [US1] Write benchmark for revenue by region and year in tests/benchmarks/test_aggregations.py (SLA: p95 <2s on 100M rows)
- [X] T051 [P] [US1] Write benchmark for category performance by quarter in tests/benchmarks/test_aggregations.py (SLA: p95 <1s with year filter)
- [X] T052 [P] [US1] Write benchmark for drill-down year→quarter→month in tests/benchmarks/test_aggregations.py (SLA: p95 <1s for filtered queries)
- [X] T053 [P] [US1] Write benchmark for partition pruning validation in tests/benchmarks/test_aggregations.py (verify 80%+ partition skip with year filter)
- [X] T054 [P] [US1] Write integration test for deterministic results in tests/integration/test_query_execution.py (same query, identical results across runs)

**Verify benchmarks FAIL before proceeding to implementation**

### Implementation for User Story 1

- [X] T055 [P] [US1] Implement predefined query pattern for multi-dimensional aggregation in src/query/patterns.py (revenue_by_dimensions function)
- [X] T056 [P] [US1] Implement predefined query pattern for drill-down in src/query/patterns.py (drill_down_time_hierarchy function)
- [X] T057 [US1] Integrate partition pruning demonstration in src/query/patterns.py (partition_pruning_comparison function with/without filters)
- [X] T058 [US1] Wire up generate CLI command in src/cli/generate.py to create partitioned sales data (call generators, write Parquet with year/quarter partitions)
- [X] T059 [US1] Implement execute mode in analyze CLI command in src/cli/analyze.py (run query, show execution plan with partition stats)
- [X] T060 [US1] Add query execution metrics collection in src/query/profiler.py (capture from EXPLAIN ANALYZE output)
- [X] T061 [US1] Create example queries for US1 in docs/query-catalog.md (3 examples with expected execution plans)

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently. Can generate data, run multi-dimensional queries, observe partition pruning. Benchmarks should now PASS.

---

## Phase 4: User Story 2 - Time-Series Trend Analysis (Priority: P2)

**Goal**: Window functions for moving averages, year-over-year growth, rankings

**Independent Test**: Run window function queries (3-month moving average, YoY growth, product rankings) on time-series data, verify mathematical correctness and performance (p95 <3s)

### Benchmarks for User Story 2 (REQUIRED - Benchmark-Driven Development) ⚠️

- [X] T062 [P] [US2] Write benchmark for 3-month moving average in tests/benchmarks/test_window_functions.py (SLA: p95 <3s on 100M rows)
- [X] T063 [P] [US2] Write benchmark for year-over-year growth calculation in tests/benchmarks/test_window_functions.py (SLA: p95 <3s)
- [X] T064 [P] [US2] Write benchmark for product rankings by quarter in tests/benchmarks/test_window_functions.py (SLA: p95 <2s with year filter)
- [X] T065 [P] [US2] Write integration test for window function correctness in tests/integration/test_query_execution.py (validate moving average math on known dataset)

**Verify benchmarks FAIL before proceeding to implementation**

### Implementation for User Story 2

- [X] T066 [P] [US2] Implement moving average window function pattern in src/query/patterns.py (moving_average_revenue function with configurable window size)
- [X] T067 [P] [US2] Implement year-over-year growth pattern in src/query/patterns.py (yoy_growth function using LAG window function)
- [X] T068 [P] [US2] Implement product ranking pattern in src/query/patterns.py (product_rankings function with ROW_NUMBER and RANK)
- [X] T069 [US2] Add window function examples to docs/query-catalog.md (3 examples with expected outputs)
- [X] T070 [US2] Extend analyze CLI to highlight window function execution in src/cli/analyze.py (detect WINDOW clauses, show window stats)

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently. Window functions operational. Benchmarks should PASS.

---

## Phase 5: User Story 3 - Storage Efficiency Demonstration (Priority: P3)

**Goal**: Demonstrate 5:1+ compression ratio and 10-50x query performance improvement of columnar (Parquet) vs row-based (CSV) storage

**Independent Test**: Compare storage sizes and query execution times between Parquet and CSV on identical 100M row dataset

### Benchmarks for User Story 3 (REQUIRED - Benchmark-Driven Development) ⚠️

- [X] T071 [P] [US3] Write benchmark comparing Parquet vs CSV query performance in tests/benchmarks/test_storage.py (expect 10-50x speedup for Parquet)
- [X] T072 [P] [US3] Write benchmark for columnar I/O efficiency in tests/benchmarks/test_storage.py (selective column reads: Parquet <20% data scanned vs CSV 100%)
- [X] T073 [P] [US3] Write integration test for compression ratio validation in tests/integration/test_data_generation.py (Parquet >=5:1 vs CSV)

**Verify benchmarks FAIL before proceeding to implementation**

### Implementation for User Story 3

- [X] T074 [US3] Extend generate CLI to support dual format generation in src/cli/generate.py (--format both creates Parquet + CSV)
- [X] T075 [P] [US3] Implement storage comparison query pattern in src/query/patterns.py (same_query_both_formats function)
- [X] T076 [US3] Implement storage metrics collection in src/query/profiler.py (measure file sizes, compression ratios, bytes scanned)
- [X] T077 [US3] Add storage comparison reporting to benchmark CLI in src/cli/benchmark.py (show Parquet vs CSV side-by-side results)
- [X] T078 [US3] Create storage comparison documentation in docs/architecture.md (explain columnar benefits with metrics)

**Checkpoint**: All user stories 1, 2, AND 3 should now be independently functional. Storage comparison demonstrates columnar advantages. Benchmarks should PASS.

---

## Phase 6: User Story 4 - Scalability Validation (Priority: P4)

**Goal**: Prove sub-linear query scaling (2x data → <2.5x latency) and concurrent query handling (10 queries with <2x latency)

**Independent Test**: Benchmark identical queries on 50M, 100M, 200M row datasets; measure latency growth. Run 10 concurrent queries, measure throughput degradation.

### Benchmarks for User Story 4 (REQUIRED - Benchmark-Driven Development) ⚠️

- [X] T079 [P] [US4] Write benchmark for sub-linear scaling in tests/benchmarks/test_scalability.py (query on 50M vs 100M rows, validate <2.5x latency growth)
- [X] T080 [P] [US4] Write benchmark for concurrent query execution in tests/benchmarks/test_scalability.py (1 vs 10 concurrent queries using pytest-xdist, validate <2x latency)
- [X] T081 [P] [US4] Write benchmark for partition growth scalability in tests/benchmarks/test_scalability.py (2x partitions with filter, validate constant query time)
- [X] T082 [P] [US4] Write integration test for compression consistency at scale in tests/integration/test_data_generation.py (validate compression ratio holds at 200M rows)

**Verify benchmarks FAIL before proceeding to implementation**

### Implementation for User Story 4

- [X] T083 [US4] Extend generate CLI to support multiple dataset sizes in src/cli/generate.py (--rows accepts 10000000, 50000000, 100000000, 200000000)
- [X] T084 [P] [US4] Implement scaling test utilities in src/query/patterns.py (run_query_at_scale function with dataset size parameter)
- [X] T085 [US4] Implement concurrent query executor in src/query/engine.py (execute_concurrent using threading.Thread pool)
- [X] T086 [US4] Add scaling results visualization to benchmark CLI in src/cli/benchmark.py (generate scaling chart data)
- [X] T087 [US4] Document scalability characteristics in docs/architecture.md (show scaling curves, concurrency results)

**Checkpoint**: All user stories 1-4 should now be independently functional. Scalability proven. All benchmarks should PASS.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories, documentation, and production-readiness

### Documentation

- [X] T088 [P] Create docs/architecture.md documenting DuckDB + Parquet architecture decisions
- [X] T089 [P] Complete docs/query-catalog.md with all 6 query patterns and execution plans
- [X] T090 [P] Update README.md with comprehensive usage examples and quickstart link
- [X] T091 [P] Create benchmark-results/baseline/ with initial benchmark run results (baseline for future comparisons)

### Code Quality

- [X] T092 [P] Add comprehensive docstrings to all src/ modules following Google style
- [X] T093 [P] Run ruff linting and fix all issues across codebase
- [X] T094 [P] Run black formatting across entire codebase
- [X] T095 [P] Add type hints to all public functions in src/ (mypy-compatible)

### Testing Completeness

- [X] T096 [P] Create unit tests for data generators in tests/unit/test_models.py (validate schemas, constraints)
- [X] T097 [P] Create unit tests for storage handlers in tests/unit/test_storage.py (Parquet read/write, partitioning)
- [X] T098 [P] Create unit tests for query profiler in tests/unit/test_profiler.py (metric extraction from EXPLAIN output)
- [X] T099 Create integration test for end-to-end data generation pipeline in tests/integration/test_data_generation.py (generate → load → query → verify)
- [X] T100 Create integration test for partition pruning in tests/integration/test_partitioning.py (verify partition skip in DuckDB)

### Validation & Verification

- [X] T101 Run full quickstart.md walkthrough and verify all commands work
- [X] T102 Generate 100M row dataset and validate all benchmarks pass SLAs
- [X] T103 Run constitution compliance check: verify all 7 principles satisfied with evidence
- [X] T104 Create CHANGELOG.md documenting initial release (v1.0.0)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational completion - No dependencies on other stories
- **User Story 2 (Phase 4)**: Depends on Foundational completion - Can run in parallel with US1, US3, US4 (independent)
- **User Story 3 (Phase 5)**: Depends on Foundational completion - Can run in parallel with US1, US2, US4 (independent)
- **User Story 4 (Phase 6)**: Depends on Foundational completion - Can run in parallel with US1, US2, US3 (independent)
- **Polish (Phase 7)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1) - MVP**: Can start after Foundational (Phase 2) - NO dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) - NO dependencies on other stories
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) - NO dependencies on other stories
- **User Story 4 (P4)**: Can start after Foundational (Phase 2) - NO dependencies on other stories

**Key Insight**: All user stories are independent! After Foundational phase completes, all 4 user stories can be developed in parallel.

### Within Each User Story

- **Benchmark-Driven Development Workflow**:
  1. Write benchmarks (FIRST)
  2. Verify benchmarks FAIL (no implementation exists)
  3. Implement feature
  4. Verify benchmarks PASS
  5. Commit
- Models/patterns can be implemented in parallel (marked [P])
- CLI integration depends on patterns being complete
- Documentation comes after implementation

### Parallel Opportunities

**Setup Phase**:
- Tasks T003-T009 can all run in parallel (different files)

**Foundational Phase**:
- Data Model: T011-T016 can run in parallel (different schemas in same file, but independent definitions)
- Data Generation: T020-T024 can run in parallel (different generator functions)
- Storage: T027-T030 can run in parallel (different handler modules)
- Query: T039-T040 can run in parallel (different modules)
- CLI: T042-T044 can run in parallel (different command modules)
- Testing: T045-T047, T049 can run in parallel (different test directories)

**User Story 1**:
- Benchmarks T050-T054 can run in parallel (different test files/functions)
- Patterns T055-T057 can run in parallel (different query pattern functions)

**User Story 2**:
- Benchmarks T062-T065 can run in parallel
- Patterns T066-T068 can run in parallel

**User Story 3**:
- Benchmarks T071-T073 can run in parallel

**User Story 4**:
- Benchmarks T079-T082 can run in parallel

**Polish Phase**:
- Documentation T088-T091 can run in parallel
- Code Quality T092-T095 can run in parallel
- Unit Tests T096-T098 can run in parallel

**Maximum Parallelism**: After Foundational phase, ALL 4 user stories can be worked on simultaneously by different team members!

---

## Parallel Execution Examples

### Foundational Phase (Maximum Parallel Tasks)

```bash
# Launch all dimension schema definitions together:
Task T011: "Define dim_time schema"
Task T012: "Define dim_geography schema"
Task T013: "Define dim_product schema"
Task T014: "Define dim_customer schema"
Task T015: "Define dim_payment schema"

# Launch all dimension generators together:
Task T020: "Implement dim_time generator"
Task T021: "Implement dim_geography generator"
Task T022: "Implement dim_product generator"
Task T023: "Implement dim_customer generator"
Task T024: "Implement dim_payment generator"

# Launch all storage handlers together:
Task T027: "Implement Parquet writer"
Task T028: "Implement Parquet reader"
Task T029: "Implement CSV writer"
Task T030: "Implement CSV reader"
```

### User Story 1 (Parallel Benchmarks)

```bash
# Write all benchmarks for US1 in parallel:
Task T050: "Benchmark revenue by region and year"
Task T051: "Benchmark category by quarter"
Task T052: "Benchmark drill-down"
Task T053: "Benchmark partition pruning"
Task T054: "Integration test deterministic results"

# Implement all query patterns for US1 in parallel:
Task T055: "Multi-dimensional aggregation pattern"
Task T056: "Drill-down pattern"
Task T057: "Partition pruning pattern"
```

### All User Stories in Parallel

```bash
# After Foundational completes, launch all user stories simultaneously:
Developer A works on Phase 3 (US1 - Multi-Dimensional Analysis)
Developer B works on Phase 4 (US2 - Time-Series Analysis)
Developer C works on Phase 5 (US3 - Storage Comparison)
Developer D works on Phase 6 (US4 - Scalability)

# Each developer follows benchmark-driven workflow independently
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

**Goal**: Get working OLAP demo in minimum time

1. Complete Phase 1: Setup (~1-2 hours)
   - Initialize project, configure dependencies
2. Complete Phase 2: Foundational (~6-8 hours)
   - Core infrastructure: data models, generators, storage, query engine
   - CRITICAL: This phase blocks everything else
3. Complete Phase 3: User Story 1 (~4-6 hours)
   - Write benchmarks for multi-dimensional aggregation
   - Implement patterns and CLI integration
   - Verify benchmarks pass
4. **STOP and VALIDATE**: Test User Story 1 independently
   - Generate 10M row dataset
   - Run benchmarks, verify p95 <2s
   - Demonstrate partition pruning
5. Deploy/demo if ready - **Working MVP complete!**

**Total MVP Time**: ~12-16 hours of development

### Incremental Delivery (Add Features One at a Time)

**Goal**: Deliver value incrementally, validate each increment

1. Complete Setup + Foundational (~8-10 hours) → Foundation ready ✅
2. Add User Story 1 → Test independently → Deploy/Demo (MVP!) ✅
3. Add User Story 2 → Test independently → Deploy/Demo (Time-series capability added) ✅
4. Add User Story 3 → Test independently → Deploy/Demo (Storage comparison added) ✅
5. Add User Story 4 → Test independently → Deploy/Demo (Scalability proven) ✅
6. Add Polish → Production-ready documentation ✅

**Total Time**: ~30-40 hours for full demo

Each story adds value without breaking previous stories. Can stop after any story and have working demo.

### Parallel Team Strategy (Fastest Completion)

**Goal**: Complete full demo in minimum calendar time

**Team Setup**: 4 developers

**Timeline**:

**Week 1**:
- Entire team completes Setup + Foundational together (~2-3 days)
  - Pair programming on critical infrastructure
  - Code reviews for schemas and generators

**Week 2** (Parallel Development):
- Developer A: User Story 1 (P1 - Multi-Dimensional) → 2-3 days
- Developer B: User Story 2 (P2 - Time-Series) → 2-3 days
- Developer C: User Story 3 (P3 - Storage) → 2-3 days
- Developer D: User Story 4 (P4 - Scalability) → 2-3 days

**Week 3** (Integration & Polish):
- Entire team works on Polish phase together
- Run full benchmark suite on 100M rows
- Documentation and validation
- Constitution compliance check

**Total Calendar Time**: ~3 weeks with 4 developers (vs ~5-6 weeks solo)

### Solo Developer Strategy

**Recommended Order**: Follow priority sequence

1. Setup + Foundational (must complete first)
2. P1 (MVP) - Test and validate
3. P2 (next priority) - Test and validate
4. P3 (next priority) - Test and validate
5. P4 (final story) - Test and validate
6. Polish (when all stories complete)

**Time Commitment**: ~5-6 weeks part-time (10 hours/week)

---

## Notes

- **[P] tasks** = different files, no dependencies, can run in parallel
- **[Story] label** maps task to specific user story for traceability
- **Each user story** should be independently completable and testable
- **Benchmark-Driven Development**: Always write benchmarks FIRST, verify they FAIL, then implement
- **Constitution Compliance**: Principle III (Benchmark-Driven Development) is NON-NEGOTIABLE
- **Commit Strategy**: Commit after each task or logical group of parallel tasks
- **Checkpoints**: Stop at any checkpoint to validate story independently before proceeding
- **Data Generation**: Use fixed SEED=42 for reproducibility
- **Performance Targets**: All SLAs from spec.md must be met (enforced by benchmarks)

**Avoid**:
- Vague tasks without file paths
- Same file conflicts (sequential edits required)
- Cross-story dependencies that break independence
- Implementing before writing benchmarks (constitution violation)
- Skipping checkpoints (risk of accumulating bugs)
