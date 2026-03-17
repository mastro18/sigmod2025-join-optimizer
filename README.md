# SIGMOD 2025 Contest Submission

## Overview

This repository contains our final implementation for the SIGMOD 2025 Programming Contest, based on the official contest repository:

https://github.com/SIGMOD-25-Programming-Contest/contest-sigmod2025

The current version of the project documents only the implementation that is still active in the codebase. Older experimental variants such as Robin Hood, Hopscotch, and Cuckoo hashing are not part of the current build flow and are therefore omitted from this README.

Our work focuses on optimizing the contest execution engine for repeated join-query evaluation on the IMDB workload. The goal of the final version is not only correctness, but also reduced runtime through better data organization, efficient hash-based joins, parallel probing, and a cache-assisted execution flow for repeated experiments.

## Current Implementation

The final engine focuses on fast join execution over the contest plans and uses the components that are currently integrated into the executable targets:

- parallel hash join execution in [src/execute.cpp](src/execute.cpp)
- unchained hash table support in [src/Uhashtable.cpp](src/Uhashtable.cpp)
- column-oriented processing in [src/column_store.cpp](src/column_store.cpp)
- late materialization utilities in [src/late.cpp](src/late.cpp)
- cached execution support through the `build_cache` and `fast` targets

At runtime, the main execution path builds a hash table on integer join keys, probes it in parallel, and materializes the requested output columns from the intermediate column representation.

## What We Implemented

The final codebase is centered around a practical high-performance join engine tailored to the SIGMOD contest format.

- We implemented a hash-join execution path that evaluates the contest plans directly from the provided pipeline representation.
- We used an unchained hash table as the core lookup structure for integer join keys in order to reduce lookup overhead during probing.
- We parallelized the probe phase so that multiple worker threads can process page batches concurrently.
- We preserved a column-oriented intermediate format, which keeps execution closer to the structure of the input data and avoids unnecessary row materialization during join processing.
- We kept support for cached execution, allowing repeated runs to avoid rebuilding the full correctness-checking workflow every time.

In practical terms, the final implementation takes the plan generated from the benchmark workload, executes scans and joins over the selected columns, builds a compact hash structure on the build side of the join, probes it from the other side in parallel, and produces the requested output columns in contest-compatible format.

## Build

Run all commands from the project root.

1. Download the IMDB dataset.

```bash
./download_imdb.sh
```

2. Configure the project.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev
```

3. Build the binaries.

```bash
cmake --build build -- -j $(nproc)
```

## Running

### Prepare DuckDB for correctness checks

```bash
./build/build_database imdb.db
```

### Run the standard contest driver

```bash
./build/run plans.json
```

### Run the cached fast path

On Unix systems, the repository also provides cache-based execution targets for faster repeated runs.

1. Build cache files:

```bash
./build/build_cache plans.json
```

2. Execute using the cache:

```bash
./build/fast plans.json
```

## Repository Layout

```text
pro_sigmod/
├── include/        # headers for execution, storage, and helper structures
├── job/            # JOB benchmark SQL workload
├── src/            # implementation of the final engine
├── tests/          # driver programs and unit tests
├── CMakeLists.txt  # build configuration
├── download_imdb.sh
└── plans.json
```

## Notes

- the contest template and input format come from the official SIGMOD 2025 contest repository
- this README describes the current state of this repository, not earlier intermediate project milestones

## Contributors

https://github.com/sdi2200200