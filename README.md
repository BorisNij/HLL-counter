# HyperLogLog Implementation in Go

This repository contains a Go implementation of the HyperLogLog algorithm for calculating the cardinality (the number of
unique elements) of a multiset. This implementation is designed to process large files concurrently, leveraging multiple
CPU cores but limited memory footprint (the HyperLogLog data structure is designed to take up to 256 KB of memory with
precision of 16 bits and estimation error of up to 0.1%).

## Table of Contents

- [Main features](#main-features)
- [Requirements](#requirements)
- [Usage](#usage)
- [Example Run](#example-run)

## Main features

- **No external dependencies**: This implementation uses only the Go standard library packages.

- **Concurrent processing**: The implementation reads and processes large files in parallel using multiple goroutines,
  which significantly speeds up the processing time by utilizing all available CPU cores.

- **Memory efficiency and accuracy**: 32-bit registers are used. Their size is determined by the specified precision (
  usually ranging between 4 - 64 bytes of memory footprint but estimation error of up to 26%, and 16 bits - the default
  value, resulting in 256 KB and error of up to 0.1%), allowing for a trade-off between memory usage and accuracy.

- **Merging Capability**: The ability to merge multiple HyperLogLog instances allows for the aggregation of results from
  different data sources, making it suitable for distributed systems.

## Requirements

1. Go 1.23.4

## Usage

```bash
# Clone the repository
git clone https://github.com/BorisNij/HLL-counter.git
cd HLL-counter
# Cimpile and run
go run calc_uniq.go <input_file>
```

## Example run

- The following was executed in a Docker container allocated 6 CPUs and 8 GB of RAM under a MacBook Pro M2 2022 host:

```bash
~/go/gbIPc$ time go run calc_uniq.go ip_addr_5M_uniq_100M_total.txt 
Estimated number of unique rows: 5052035

real    0m3.425s
user    0m2.301s
sys     0m0.760s
~/go/gbIPc$ 
~/go/gbIPc$ wc -l ip_addr_5M_uniq_100M_total.txt 
100000000 ip_addr_5M_uniq_100M_total.txt
~/go/gbIPc$ 
~/go/gbIPc$ time sort ip_addr_5M_uniq_100M_total.txt | uniq | wc -l
5000000

real    0m26.475s
user    0m50.860s
sys     0m4.203s
~/go/gbIPc$ 
$ ls -lh ip_addr_5M_uniq_100M_total.txt 
-rw-r--r-- 1 501 dialout 1.4G Dec  9 01:07 ip_addr_5M_uniq_100M_total.txt
~/go/gbIPc$ 
```