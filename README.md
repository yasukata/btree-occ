# A Multi-core Scalable B+Tree adopting Optimistic Concurrency Control (OCC)

This implementation is slightly extended so that it can store pairs of key-value strings.

## What is Optimistic Concurrency Control (OCC)?

OCC is a cache-friendly concurrency coordination mechanism.

Technically, one of the primary benefits of OCC is that readers do not modify memory for readers-lock.

Commonly, the readers-lock modifies in-memory variables when a reader enters a critical section.
The problem is that the modified value on the cache has to be propagated to the other CPU cores, and that causes the cache coherence misses.

In OCC, when writers update in-memory objects, they increment version counters associated with them. The readers use the version counters to detect their obtained values are modified during their read.

The following are the typical steps for readers to perform a target value in OCC.

1. read a version counter
2. read a target value
3. read the version counter again

If the version counter numbers are different in step 1 and step 3,
it means that the target value is updated while the reader is reading.

In that case, the reader throws away the obtained target value and retry the entire steps.

## Compilation

The ```make``` command will generate a benchmark app named ```btbench``` that involves the B+Tree implementation.

```
$ make
```

## Run Benchmark

The following is an example command and output of ```btbench```.

```
$ ./btbench -c 1000000 -t 2 -k 8 -v 64
[main:315]: 2-thread, 1000000 entries (500000 per-thread), keylen 8, vallen 64
[main:320]: B+Tree
[main:332]: PUT entries...
[__bench:236]: thread[0]: 500000 operations
[__bench:236]: thread[1]: 500000 operations
total 1000000 entries: duration 945342202 ns, 945 ns/op, 1.057818 Mops/sec
[main:337]: GET entries...
[__bench:236]: thread[0]: 500000 operations
[__bench:236]: thread[1]: 500000 operations
total 1000000 entries: duration 468288840 ns, 468 ns/op, 2.135434 Mops/sec
```

- ```-c```: number of operations
- ```-t```: number of concurrent threads
- ```-k```: key length
- ```-v```: value length

## API

```kvs.hpp``` defines a class named ```BKVS``` that implements a series of interfaces for Get, Put, Delete operations, and ```btree.cpp``` implements the defined interfaces.

Those three member functions are the simplest ones. There are also variants that accept key and value length specification, and multiple fields using C++'s vector. For details, please refer to ```kvs.hpp```.

```c++
const char *Get(const char *key)
```

```c++
int Put(const char *key, const char *val)
```

```c++
int Del(const char *key)
```

For the instantiation of the ```BKVS``` class for B+Tree, please use the following.

```
BKVS *createBTKVS(void)
```
