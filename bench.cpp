/*
 *
 * Copyright 2020 Kenichi Yasukata
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <getopt.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>

#include "mt19937ar.h"

#include "kvs.hpp"

#ifndef _D
#define _D(fmt, ...) \
	printf("[%s:%d]: " fmt "\n", __func__, __LINE__, ##__VA_ARGS__)
#endif

enum {
	KVS_OP_PUT = 1,
	KVS_OP_GET,
	KVS_OP_DEL,
};

struct runner {
	int id;
	struct bench *b;
	void *db;
	void *udata;
};

struct kvs_ops {
	int (*insert)(void *, const char *, uint32_t, const char *, uint32_t);
	bool (*search)(void *, const char *, uint32_t);
	int (*remove)(void *, const char *, uint32_t);
	void (*init)(struct bench *);
	void (*exit)(struct bench *);
};

struct bench {
	int num_runners;
	long cnt;
	size_t keylen;
	size_t vallen;
	struct kvs_ops *kvs_op;
	struct runner **runners;
};

/* b+tree ops */

static int _bt_insert(void *data, const char *key, uint32_t kl, const char *val, uint32_t vl)
{
	BKVS *db = (BKVS *) data;
	return db->Put(key, kl, val, vl);
}

static bool _bt_search(void *data, const char *key, uint32_t kl)
{
	int err;
	const std::vector<std::string> *_ptr = NULL;
	const std::vector<std::string> &fields = *_ptr;
	std::vector<std::pair<std::string, std::string>> result;
	BKVS *db = (BKVS *) data;
	err = db->Get(NULL, key, kl, fields, result);
	if (err == 1) {
		const char *val = result[0].second.c_str();
		if (((uint32_t *) key)[0] != ((uint32_t *) val)[0]) {
			_D("wrong key and val %x %x", ((uint32_t *) key)[0], ((uint32_t *) val)[0]);
			exit(1);
		}
	}
	return err;
}

static int _bt_delete(void *data, const char *key, uint32_t kl)
{
	BKVS *db = (BKVS *) data;
	return db->Del(key, kl);
}

static void _bt_init(struct bench *b)
{
	BKVS *db = createBTKVS();
	for (auto i = 0; i < b->num_runners; i++)
		b->runners[i]->db = db;
}

static void _bt_exit(struct bench *b)
{
	struct runner *runner = b->runners[0];
	delete (BKVS *) runner->db;
}

struct kvs_ops bt_kvs_op = {
	.insert = _bt_insert,
	.search = _bt_search,
	.remove = _bt_delete,
	.init = _bt_init,
	.exit = _bt_exit,
};

/* benchmark */

struct td {
	pthread_t tid;
	int op;
	int cnt;
	long done;
	size_t keylen;
	size_t vallen;
};

static void bench_init(struct bench *b)
{
	int i;

	b->runners = (struct runner **) malloc(sizeof(struct runner *) * b->num_runners);
	assert(b->runners);

	for (i = 0; i < b->num_runners; i++) {
		b->runners[i] = (struct runner *) malloc(sizeof(struct runner));
		assert(b->runners[i]);
		b->runners[i]->b = b;
		b->runners[i]->id = i;
	}

	if (b->kvs_op->init)
		b->kvs_op->init(b);

	for (i = 0; i < b->num_runners; i++) {
		struct td *td = (struct td *) malloc(sizeof(struct td));
		assert(td);
		td->cnt = b->cnt / b->num_runners;
		td->done = 0;
		td->keylen = b->keylen;
		td->vallen = b->vallen;
		b->runners[i]->udata = td;
	}
}

static inline void random_string(struct imt *_m, char *buf, uint32_t len)
{
	unsigned int i;
	for (i = 0; i < len; i++)
		buf[i] = genrand_int32(_m) % 93 + 33;
}

static void *bench_thread_fn(void *data)
{
	struct runner *runner = (struct runner *) data;
	struct td *td = (struct td *) runner->udata;
	struct imt _m;
	char buf[1024];
	char buf2[4096];
	long cnt = td->cnt;

	assert(td->keylen < sizeof(buf));
	assert(td->vallen < sizeof(buf2));

	memset(buf2, 'A', sizeof(buf2));

	init_genrand(&_m, runner->id);

	while (cnt-- > 0) {
		random_string(&_m, buf, td->keylen);
		buf[td->keylen] = '\0';
		switch (td->op) {
		case KVS_OP_PUT:
			memcpy(buf2, buf, td->keylen-1);
			buf2[td->vallen] = '\0';
			runner->b->kvs_op->insert(runner->db,
									  buf,
									  td->keylen,
									  buf2,
									  td->vallen);
			td->done++;
			break;
		case KVS_OP_GET:
			if (runner->b->kvs_op->search(runner->db, buf, td->keylen))
				td->done++;
			break;
		case KVS_OP_DEL:
			if (cnt % 2) {
				if (runner->b->kvs_op->remove(runner->db, buf, td->keylen))
					td->done++;
			}
			break;
		default:
			_D("unknown op");
			exit(1);
		}
	}

	pthread_exit(NULL);
}

static void __bench(struct bench *b, int op)
{
	int i;
	long done = 0;
	struct timespec ts1, ts2;

	clock_gettime(CLOCK_REALTIME, &ts1);

	for (i = 0; i < b->num_runners; i++) {
		struct td *td = (struct td *) b->runners[i]->udata;
		td->op = op;
		pthread_create(&td->tid, NULL, bench_thread_fn, b->runners[i]);
	}

	for (i = 0; i < b->num_runners; i++) {
		struct td *td = (struct td *) b->runners[i]->udata;
		pthread_join(td->tid, NULL);
		done += td->done;
		_D("thread[%d]: %lu operations", i, td->done);
		td->done = 0;
	}

	clock_gettime(CLOCK_REALTIME, &ts2);

	printf("total %lu entries: duration %lu ns, %lu ns/op, %lu.%06lu Mops/sec\n",
			done,
			(ts2.tv_sec - ts1.tv_sec) * 1000000000UL + (ts2.tv_nsec - ts1.tv_nsec),
			(done ? ((ts2.tv_sec - ts1.tv_sec) * 1000000000UL + (ts2.tv_nsec - ts1.tv_nsec)) / (done) : 0),
			(done * 1000000000UL) / ((ts2.tv_sec - ts1.tv_sec) * 1000000000UL + (ts2.tv_nsec - ts1.tv_nsec)) / 1000000UL,
			(done * 1000000000UL) / ((ts2.tv_sec - ts1.tv_sec) * 1000000000UL + (ts2.tv_nsec - ts1.tv_nsec)) % 1000000UL);
}

static void put_bench(struct bench *b)
{
	__bench(b, KVS_OP_PUT);
}

static void get_bench(struct bench *b)
{
	__bench(b, KVS_OP_GET);
}

static void del_bench(struct bench *b)
{
	__bench(b, KVS_OP_DEL);
}

struct kvs_test_set {
	const char *name;
	const char *shortname;
	struct kvs_ops *kvs_op;
} kvs_list[] = {
	{ "B+Tree", "bt", &bt_kvs_op },
};

#define OPTARGSTR "c:e:k:t:v:"

int main(int argc, char* const* argv)
{
	int ch, err = 0;
	unsigned int i;
	const char *engine = "bt";
	struct bench b;

	b.num_runners = 1;
	b.keylen = 8;
	b.vallen = 8;
	b.cnt = 1;

	while ((ch = getopt(argc, argv, OPTARGSTR)) != -1) {
		switch (ch) {
		case 'c':
			b.cnt = atoi(optarg);
			break;
		case 'e':
			engine = optarg;
			break;
		case 'k':
			b.keylen = atoi(optarg);
			break;
		case 't':
			b.num_runners = atoi(optarg);
			break;
		case 'v':
			b.vallen = atoi(optarg);
			break;
		default:
			_D("unknown op %c", ch);
			exit(1);
		}
	}

	_D("%d-thread, %ld entries (%ld per-thread), keylen %lu, vallen %lu",
			b.num_runners,
			b.cnt,
			b.cnt / b.num_runners,
			b.keylen,
			b.vallen);

	for (i = 0; i < (sizeof(kvs_list) / sizeof(struct kvs_test_set)); i++) {
		if (strlen(engine) == strlen(kvs_list[i].shortname)
				&& !strncmp(engine, kvs_list[i].shortname, strlen(engine))) {
			_D("%s", kvs_list[i].name);
			b.kvs_op = kvs_list[i].kvs_op;
		}
	}

	if (!b.kvs_op) {
		_D("unknown engine %s", engine);
		exit(1);
	}

	bench_init(&b);

	_D("PUT entries...");
	put_bench(&b);

	(void) del_bench;

	_D("GET entries...");
	get_bench(&b);

	return err;
}
