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

#include <atomic>
#include <vector>
#include <sstream>
#include <unordered_map>

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>

#include "kvs.hpp"

#include "jenkins_hash.h"

#ifndef _D
#define _D(fmt, ...) \
	printf("[%s:%d]: " fmt "\n", __func__, __LINE__, ##__VA_ARGS__)
#endif

#ifndef PAGE_SIZE
#define PAGE_SIZE (4096)
#endif

#define BT_MAX_ENT ((long)((PAGE_SIZE - sizeof(struct bt_latch) - sizeof(struct bt_pg_hdr)) / 128) - 1)
#define BT_MAX_DEPTH (128)

#define BT_LOCK(_pg) \
	do { \
		uint8_t expected = 0; \
		while (!(_pg)->latch.lock.compare_exchange_strong(expected, 1, std::memory_order_release)) { expected = 0; } \
	} while (0)

#define BT_TRYLOCK(_pg) \
	({ \
		bool ret; \
		uint8_t expected = 0; \
		if (!(_pg)->latch.lock.compare_exchange_strong(expected, 1, std::memory_order_release)) { \
			ret = false; \
		} else { \
			ret = true; \
		} \
		ret; \
	})

#define BT_RELEASE(_pg) \
	do { \
		(_pg)->latch.ver++; \
		(_pg)->latch.lock.store(0, std::memory_order_release); \
	} while (0)

#define BT_IS_LOCKED(_pg) \
	({ \
		bool ret; \
		if (!(_pg)->latch.lock.load(std::memory_order_acquire)) { \
			ret = false; \
		} else { \
			ret = true; \
		} \
		ret; \
	})

struct bt_latch {
	std::atomic<uint8_t> lock;
	std::atomic<uint8_t> ver;
};

struct bt_item {
	struct bt_latch latch;
	struct bt_item *chain;
	uint32_t kl;
	uint32_t vl;
	struct bt_item *ptr;
	char data[0];
};

struct bt_pg_hdr {
	uint64_t del_time;
	uint8_t leaf;
	uint16_t __used_ent16;
	struct bt_pg *__pre64;
	struct bt_pg *__nxt64;
};

struct bt_pg {
	struct bt_latch latch;
	struct bt_pg_hdr hdr;
	uint64_t __key64[BT_MAX_ENT];
	union {
		struct {
			struct bt_pg *__ptr64[BT_MAX_ENT+1];
		};
		struct {
			struct bt_item *__val64[BT_MAX_ENT+1];
		};
	};
};

struct bt_cur;

struct bt {
	struct bt_pg *root;
	struct fm_info *fi;
	int num_cur;
	int max_cur;
	struct bt_cur **cur;
	pthread_mutex_t cur_mutex;
};

struct bt_path {
	uint8_t ver;
	uint8_t in_lock;
	uint8_t need_another_lock;
	uint8_t i;
	struct bt_pg *pg;
	struct bt_pg *_c_n_pg;
	struct bt_pg *_c_p_pg;
};

typedef int (fetch_cb_t)(struct bt_cur *cur, struct bt_pg *pg, uint16_t idx);

struct bt_cur {
	uint16_t id;
	struct bt *bt;
	void *ptr;
	uint16_t depth;
	uint64_t key;
	uint64_t val;
	uint64_t kl;
	uint64_t vl;
	const char *keystr;
	fetch_cb_t *fetch_cb;
	uint8_t need_del;
	std::vector<struct bt_pg *> del_pg_list;
	std::atomic<uint64_t> enter_time;
	std::atomic<uint64_t> exit_time;
	std::atomic<uint8_t> waitlock;
	const std::vector<std::string> *fields;
	std::vector<std::pair<std::string, std::string>> *results;
	struct bt_path path[BT_MAX_DEPTH];
};

struct bt_del_info {
	struct bt_pg *pg;
	uint16_t idx;
	struct bt_pg *del_pg;
	uint16_t del_ent;
	uint16_t del_depth;
};

static inline void __bt_get_result(struct bt_cur *cur, struct bt_item *item)
{
	const std::vector<std::string> &fields = *(cur->fields);
	std::vector<std::pair<std::string, std::string>> &results = *(cur->results);
	if (cur->fields) {
		for (const std::string &s : fields) {
			bool found = false;
			struct bt_item *field = item->ptr;
			while (field) {
				if (s.size() == field->kl
						&& !memcmp(s.c_str(), field->data, s.size())) {
					results.push_back(std::pair<std::string, std::string>(s, std::string(&field->data[field->kl+1])));
					found = true;
					break;
				}
				field = field->ptr;
			}
			if (!found)
				results.push_back(std::pair<std::string, std::string>(s, std::string("")));
		}
	} else {
		struct bt_item *field = item->ptr;
		while (field) {
			results.push_back(std::pair<std::string, std::string>(std::string(field->data), std::string(&field->data[field->kl+1])));
			field = field->ptr;
		}
	}
}

static inline void __bt_put_result(struct bt_cur *cur, struct bt_item *old_item, struct bt_item *new_item)
{
	struct bt_item *new_field = new_item->ptr;

	(void) cur;

	while (new_field) {
		bool replaced = false;
		struct bt_item *new_tmp = new_field->ptr, *old_field = old_item->ptr, *old_prev = old_item;
		new_field->ptr = NULL;
		while (old_field) {
			if (new_field->kl == old_field->kl
					&& !memcmp(new_field->data, old_field->data, new_field->kl)) {
				old_prev->ptr = new_field;
				/* readers should never read freeing field */
				std::atomic_thread_fence(std::memory_order_release);
				new_field->ptr = old_field->ptr;
				free(old_field);
				replaced = true;
				break;
			}
			old_prev = old_field;
			old_field = old_field->ptr;
		}
		if (!replaced)
			old_prev->ptr = new_field;
		new_field = new_tmp;
	}

	free(new_item);
}

static int bt_fetch_cb_delete(struct bt_cur *cur, struct bt_pg *_p, uint16_t _i);

static void *bt_alloc_obj(struct bt *bt, size_t size)
{
	(void) bt;
	return malloc(size);
}

static inline struct bt_pg *bt_pg_alloc(void)
{
	struct bt_pg *pg = (struct bt_pg *) malloc(sizeof(struct bt_pg));
	assert(pg);
	memset(pg, 0, sizeof(struct bt_pg));
	pg->latch.lock = 0;
	return pg;
}

static struct bt *bt_create(void)
{
	struct bt_pg *pg;
	struct bt *bt;

	bt = (struct bt *) malloc(sizeof(struct bt));
	assert(bt);
	bt->num_cur = 0;
	bt->max_cur = 1;
	pthread_mutex_init(&bt->cur_mutex, NULL);

	bt->cur = (struct bt_cur **) malloc(sizeof(struct bt_cur *) * bt->max_cur);
	assert(bt->cur);

	bt->root = bt_pg_alloc();
	pg = bt_pg_alloc();
	pg->hdr.leaf = 1;
	bt->root->__ptr64[0] = pg;

	return bt;
}

static struct bt_cur *bt_cur_alloc(struct bt *bt)
{
	struct bt_cur *cur;
	pthread_mutex_lock(&bt->cur_mutex);
	cur = (struct bt_cur *) malloc(sizeof(struct bt_cur));
	assert(cur);
	cur->bt = bt;
	cur->id = bt->num_cur;
	cur->waitlock = 0;
	cur->enter_time = 0;
	cur->exit_time = 1;
	cur->del_pg_list = {};
	bt->cur[cur->id] = cur;
	bt->num_cur++;
	if (bt->num_cur == bt->max_cur) {
		void *ptr;
		bt->max_cur *= 2;
		ptr = realloc(bt->cur, sizeof(struct bt_cur *) * bt->max_cur);
		assert(ptr);
		bt->cur = (struct bt_cur **) ptr;
	}
	pthread_mutex_unlock(&bt->cur_mutex);
	return cur;
}

static inline bool can_delete(struct bt_cur *cur, uint64_t time)
{
	int i;
	struct bt *bt = cur->bt;
	for (i = 0; i < bt->num_cur; i++) {
		struct bt_cur *_cur = bt->cur[i];
		if (_cur == cur)
			continue;
		if (_cur->enter_time.load(std::memory_order_acquire) <= time
				&& time <= _cur->exit_time.load(std::memory_order_acquire))
			return false;
	}
	return true;
}

static inline void bt_do_pg_free(struct bt_cur *cur)
{
	bool all_clear = true;
	if (cur->need_del) {
		for (auto it = cur->del_pg_list.begin(); it != cur->del_pg_list.end();) {
			if (can_delete(cur, (*it)->hdr.del_time)) {
				struct bt_pg *pg = *it;
				it = cur->del_pg_list.erase(it);
				free(pg);
			} else {
				++it;
				all_clear = false;
			}
		}
	}
	if (all_clear)
		cur->need_del = 0;
}

static inline void bt_pg_free(struct bt_cur *cur, struct bt_pg *pg)
{
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	cur->del_pg_list.push_back(pg);
	cur->need_del = 1;
	pg->hdr.del_time = ts.tv_sec * 1000000000UL + ts.tv_nsec;
}

static inline void __set_time(struct bt_cur *cur, bool enter)
{
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	if (enter)
		cur->enter_time.store(ts.tv_sec * 1000000000UL + ts.tv_nsec, std::memory_order_release);
	else
		cur->exit_time.store(ts.tv_sec * 1000000000UL + ts.tv_nsec, std::memory_order_release);
}

static inline void set_enter_time(struct bt_cur *cur)
{
	__set_time(cur, true);
}

static inline void set_exit_time(struct bt_cur *cur)
{
	__set_time(cur, false);
}

static int __fetch_item(struct bt_cur *cur, struct bt_pg *pg,
			bool backward, bool locking, bool collid,
			bool *mustcheck_backward)
{
	int err = 0, i, inc;
	uint16_t start, end;
	struct bt_pg *next;
	struct bt_item *item;
	uint8_t ver = pg->latch.ver.load(std::memory_order_acquire);
	end = pg->hdr.__used_ent16;
	start = (backward ? end - 1 : 0);
	inc = (backward ? -1 : 1);
	for (i = start; i >= 0 && i < end; i += inc) {
		if (cur->key != pg->__key64[i]) {
			if (collid)
				goto out1;
			else
				continue;
		}

		if (!collid && cur->fetch_cb == bt_fetch_cb_delete) {
			struct bt_del_info *di = (struct bt_del_info *) cur->ptr;
			if (di->pg == pg)
				di->idx = i;
		}

		if (!collid && i == 0 && !backward && mustcheck_backward)
			*mustcheck_backward = true;

		item = pg->__val64[i];
		if (!locking) {
			if (BT_IS_LOCKED(pg) || ver != pg->latch.ver.load(std::memory_order_acquire)) {
				err = -EAGAIN;
				goto out0;
			}
		}
		if (cur->kl == item->kl) {
			if (!memcmp(cur->keystr, item->data, cur->kl)) {
				err = cur->fetch_cb(cur, pg, i);
				goto out1;
			}
		}
		collid = true;
	}

	if (!collid)
		goto out1;

	next = (struct bt_pg *)(backward ? pg->hdr.__pre64 : pg->hdr.__nxt64);
	if (next && locking && (next == cur->path[cur->depth-1]._c_p_pg || next == cur->path[cur->depth-1]._c_n_pg)) {
		err = __fetch_item(cur, next, backward, locking, true, NULL);
		goto out0;
	}

	if (!locking) {
		if (BT_IS_LOCKED(pg) || ver != pg->latch.ver.load(std::memory_order_acquire)) {
			err = -EAGAIN;
			goto out0;
		}
	}

	if (next) {
		if (locking) {
			if (!backward)
				BT_LOCK(next);
			else {
				if (!BT_TRYLOCK(next)) {
					err = -EAGAIN;
					goto out0;
				}
			}
		}
		err = __fetch_item(cur, next, backward, locking, true, NULL);
		if (locking)
			BT_RELEASE(next);
		if (err == -EAGAIN)
			goto out0;
	}
out1:
	if (!locking) {
		if (err == -EAGAIN)
			goto out0;
		if (BT_IS_LOCKED(pg) || ver != pg->latch.ver.load(std::memory_order_acquire))
			err = -EAGAIN;
	}
out0:
	return err;
}

static int fetch_item(struct bt_cur *cur, struct bt_pg *pg, bool locking)
{
	int err = 0;
	uint8_t ver = 0;
	bool go_backward = false;

	/*
	 * this function offers two modes, locking and non-locking.
	 * looking is used for insert and delete. non-looking is for lookup.
	 * locking mode traverses leaf's next hops, and previous hops while taking the locks of them.
	 * so, we can trust the values of next and previous hop pointers at ever node.
	 *
	 * in non-locking mode, we do not have a lock, and if we detect changes while returning
	 * from the neighbor hops, we throw away the result and return to retry.
	 */

	/*
	 * because we accept a same key id with different key strings to
	 * cope with hash collision, potentially, a split produces the following situation.
	 *
	 *
	 * < before >
	 *
	 * node  |  9 | 10 | 12 |
	 *                  |
	 *                  |
	 * leaf             | 10 | 11 | 11 | 11 |
	 * keystr            "x"  "y"  "z"
	 *
	 *
	 * < after >
	 *
	 * node |  9 | 10 | 11 | 12 |
	 *                 |    |
	 *                 |    +------------+
	 *                 |                 |
	 * leaf            | 10 | 11 |       | 11 | 11 |
	 * keystr                "x"          "y"  "z"
	 *
	 *
	 * in the above case, traversal for a key 11 will reache the entry whose keystr
	 * is "y". however, if we look for "z", we need to go the next entry.
	 * if we want "x", we need to go to the previous leaf.
	 *
	 * in this implementation, we start from checking the current leaf and,
	 * if the current_leaf->entry[0] has the key that we want to have, we go to the previous leaf,
	 * because previous_leaf->entry[used_ent-1] may also have the same key.
	 *
	 */
	err = __fetch_item(cur, pg, false, locking, false, &go_backward);
	if (err == 0 && go_backward) {
		/*
		 * here, the forward path does not find an entry, and there
		 * is still a possibility that the entry we want locates previous leaves.
		 */
		struct bt_pg *pre;
		if (!locking)
			ver = pg->latch.ver.load(std::memory_order_acquire);
		pre = (struct bt_pg *) pg->hdr.__pre64;
		if (!pre)
			goto out;
		if (pre && locking && (pre == cur->path[cur->depth-1]._c_p_pg || pre == cur->path[cur->depth-1]._c_n_pg)) {
			err = __fetch_item(cur, pre, true, locking, true, NULL);
			goto out;
		}
		if (!locking) {
			if (BT_IS_LOCKED(pg) || ver != pg->latch.ver.load(std::memory_order_acquire)) {
				err = -EAGAIN;
				goto out;
			}
		} else
			BT_LOCK(pre);
		err = __fetch_item(cur, pre, true, locking, true, NULL);
		if (locking) {
			assert(err != -EAGAIN);
			BT_RELEASE(pre);
		} else {
			if (err == -EAGAIN)
				goto out;
			if (BT_IS_LOCKED(pg) || ver != pg->latch.ver.load(std::memory_order_acquire))
				err = -EAGAIN;
		}
	}
out:
	return err;
}

static int bt_fetch_cb_lookup(struct bt_cur *cur, struct bt_pg *pg, uint16_t idx)
{
	int err = 1;
	struct bt_item *item = pg->__val64[idx];
	uint8_t ver = pg->latch.ver.load(std::memory_order_acquire);
	std::vector<std::pair<std::string, std::string>> &results = *(cur->results);

	assert(item->ptr);

	results.clear();
	__bt_get_result(cur, item);

	if (BT_IS_LOCKED(pg) || ver != pg->latch.ver.load(std::memory_order_acquire))
		err = -EAGAIN;

	return err;
}

static int __bt_lookup(struct bt_cur *cur, struct bt_pg *pg)
{
	int i, err = 0;
	cur->depth++;
	if (pg->hdr.leaf) {
		uint8_t ver = pg->latch.ver.load(std::memory_order_acquire);
		err = fetch_item(cur, pg, false);
		if (err == -EAGAIN)
			goto out;
		if (BT_IS_LOCKED(pg) || ver != pg->latch.ver.load(std::memory_order_acquire))
			err = -EAGAIN;
	} else {
		struct bt_pg *cpg;
		uint8_t ver = pg->latch.ver.load(std::memory_order_acquire);
		for (i = 0; i < pg->hdr.__used_ent16; i++) {
			if (cur->key < pg->__key64[i])
				break;
		}
		cpg = (struct bt_pg *) pg->__ptr64[i];
		if (BT_IS_LOCKED(pg) || ver != pg->latch.ver.load(std::memory_order_acquire)) {
			err = -EAGAIN;
			goto out;
		}
		err = __bt_lookup(cur, cpg);
		if (err == -EAGAIN)
			goto out;
		else if (BT_IS_LOCKED(pg) || ver != pg->latch.ver.load(std::memory_order_acquire))
			err = -EAGAIN;
	}
out:
	return err;
}

static bool bt_lookup(struct bt_cur *cur, struct bt_pg *pg)
{
	int err = 0;
	uint8_t ver;
	struct bt_pg *cpg;
	bt_do_pg_free(cur);
	set_enter_time(cur);
	cur->fetch_cb = bt_fetch_cb_lookup;
retry:
	cur->depth = 0;
	ver = pg->latch.ver.load(std::memory_order_acquire);
	cpg = (struct bt_pg *) pg->__ptr64[0];
	if (BT_IS_LOCKED(pg) || ver != pg->latch.ver.load(std::memory_order_acquire))
		goto retry;
	err = __bt_lookup(cur, cpg);
	if (err == -EAGAIN)
		goto retry;
	set_exit_time(cur);
	return (err == 1);
}

static int bt_add_ent(struct bt_pg *pg, uint64_t key, uint64_t val)
{
	int i = 0;
	for (i = 0; i < pg->hdr.__used_ent16; i++) {
		if (key < pg->__key64[i])
			break;
	}
	memmove(&pg->__key64[i+1], &pg->__key64[i], sizeof(uint64_t) * (pg->hdr.__used_ent16 - i));
	memmove(&pg->__val64[i+1], &pg->__val64[i], sizeof(uint64_t) * (pg->hdr.__used_ent16 + 1 - i));
	pg->__key64[i] = key;
	if (pg->hdr.leaf)
		pg->__val64[i] = (struct bt_item *) val;
	else
		pg->__ptr64[i] = (struct bt_pg *) val;
	pg->hdr.__used_ent16++;
	return i;
}

static void bt_split(struct bt_cur *cur, struct bt_pg *ppg, struct bt_pg *cpg)
{
	int i;
	struct bt_pg *newpg = bt_pg_alloc();

	(void) cur;

	memcpy(&newpg->__key64[0], &cpg->__key64[BT_MAX_ENT - BT_MAX_ENT/2], sizeof(uint64_t) * (BT_MAX_ENT - BT_MAX_ENT/2));
	memcpy(&newpg->__val64[0], &cpg->__val64[BT_MAX_ENT - BT_MAX_ENT/2], sizeof(uint64_t) * (BT_MAX_ENT - BT_MAX_ENT/2 + 1));

	newpg->hdr.__used_ent16 = BT_MAX_ENT - BT_MAX_ENT/2;
	cpg->hdr.__used_ent16 = BT_MAX_ENT/2 - 1 + (cpg->hdr.leaf ? 1 : 0);
	newpg->hdr.leaf = cpg->hdr.leaf;

	if (!cpg->hdr.leaf)
		i = bt_add_ent(ppg, cpg->__key64[BT_MAX_ENT - BT_MAX_ENT/2 - 1], (uint64_t) cpg);
	else {
		if (cpg->hdr.__nxt64) {
			struct bt_pg *next = cpg->hdr.__nxt64;
			next->hdr.__pre64 = newpg;
			newpg->hdr.__nxt64 = cpg->hdr.__nxt64;
		}
		cpg->hdr.__nxt64 = newpg;
		newpg->hdr.__pre64 = cpg;
		i = bt_add_ent(ppg, newpg->__key64[0], (uint64_t) cpg);
	}
	assert(cpg == (struct bt_pg *) ppg->__ptr64[i+1]);
	ppg->__ptr64[i+1] = newpg;
}

static int bt_grow(struct bt_cur *cur, struct bt_pg *pg)
{
	struct bt_pg *newpg, *oldpg;
	oldpg = pg->__ptr64[0];
	newpg = bt_pg_alloc();
	newpg->hdr.__used_ent16 = 0;
	newpg->__val64[0] = pg->__val64[0];
	bt_split(cur, newpg, oldpg);
	pg->__ptr64[0] = newpg;
	return 0;
}

static int bt_path_lock_for_split(struct bt_cur *cur, uint16_t depth)
{
	int err = 0;
	struct bt_pg *pg = cur->path[depth].pg;
	if (!BT_TRYLOCK(pg)) {
		/*
		 * if we fail to take a lock, it means someone has modified the parent.
		 * now, our trial is not valid, so just go back to retry;
		 */
retry:
		err = -EAGAIN;
release:
		cur->path[depth].in_lock = 0;
		goto out;
	}

	if (cur->path[depth].ver != pg->latch.ver.load(std::memory_order_acquire)) {
		/*
		 * we could have a lock, but the node is changed.
		 * our trial is not valid anymore, so go back to retry.
		 */
		BT_RELEASE(pg);
		goto retry;
	}
	cur->path[depth].in_lock = 1;
	if (pg->hdr.__used_ent16 + 2 < BT_MAX_ENT) {
		/*
		 * this node has sufficient space to store the new entry.
		 * let's stop taking lock here, and go back to start insersion.
		 */
		goto out;
	}
	/*
	 * if the parent node of this node also have to be splitted,
	 * we try to take the lock of the parent.
	 */
	err = bt_path_lock_for_split(cur, depth - 1);
	if (err == -EAGAIN) {
		BT_RELEASE(pg);
		goto release;
	}
out:
	return err;
}

static int bt_fetch_cb_insert(struct bt_cur *cur, struct bt_pg *pg, uint16_t idx)
{
	struct bt_item *old_item, *new_item;

	old_item = pg->__val64[idx];
	new_item = (struct bt_item *) cur->val;

	if (!old_item) {
		pg->__val64[idx] = new_item;
		return 1;
	}

	BT_LOCK(old_item);

	__bt_put_result(cur, old_item, new_item);

	BT_RELEASE(old_item);

	return 1;
}

static int __bt_insert(struct bt_cur *cur, struct bt_pg *pg, uint16_t depth)
{
	int i, err = 0;
	assert(depth < BT_MAX_DEPTH);
	cur->depth = depth;
	cur->path[depth].pg = pg;
	if (pg->hdr.leaf) {
		BT_LOCK(pg);
		cur->path[depth].in_lock = 0;

		if (BT_IS_LOCKED(cur->path[depth-1].pg) || cur->path[depth-1].ver != cur->path[depth-1].pg->latch.ver.load(std::memory_order_acquire)) {
			/*
			 * now, we have a lock of the leaf.
			 * but we need to check whether our parent has been changed.
			 *
			 * this check is necessary to avoid the following case.
			 *
			 * 1. we reach this leaf.
			 * 2.                       somebody split this leaf
			 * 3. we take a lock
			 * 4. we modify this leaf
			 *
			 *
			 * < before >
			 *
			 * node | 3 | 18 |
			 *           |
			 * leaf      | 3 | 8 | 9 | 11 |
			 *           (addr: 0xfff000)
			 *     
			 * < after >
			 *
			 * node | 3 | 9 | 18 |
			 *           |   |
			 *           |   +-------------------+
			 *           |                       |
			 * leaf      | 3 | 8 |               | 9 | 11 |
			 *           (addr: 0xfff000)        (addr: 0xfff110)
			 *
			 *
			 * in the upper case, let's say we are trying to add a key of 12 in the < before > state.
			 * first, in the 1st step, we will reach the leaf at 0xfff000 in < before >.
			 * in the 2nd phase, someone has splitted the leaf, and the state goes to < after >.
			 * next, as the 3rd phase, we are still looking at the leaf at 0xfff000, and will hold a lock.
			 * afterwards, in the 4th step, we will add 12 to the leaf at 0xfff000.
			 *
			 * now, what we will see will be the following tree. 12 is inserted in the wrong place.
			 *
			 *
			 * node | 3 | 9 | 18 |
			 *           |   |
			 *           |   +-------------------+
			 *           |                       |
			 * leaf      | 3 | 8 | 12 |          | 9 | 11 |
			 *           (addr: 0xfff000)        (addr: 0xfff110)
			 *
			 *
			 * potentially, after we hold a lock of a leaf, the leaf may not be the right place to
			 * add a key that we are trying to insert anymore.
			 *
			 * in order to detect that we are in the wrong place, we check the parent's modification
			 * AFTER we get the lock of the leaf.
			 * because, any node and leaf won't be splitted while its lock is taken by someone else.
			 * we need to have a lock of this leaf first, for avoiding split on this leaf.
			 *
			 * now, we check the parent, IF the parent is changed, here may not be the right place.
			 * so, we give up this trial and go back to retry.
			 *
			 */
			err = -EAGAIN;
			BT_RELEASE(pg);
			goto out;
		}

		/*
		 * for just replacing an item, we do not need furhter locks,
		 * and it never fails because we already have a lock of this leaf.
		 */
		err = fetch_item(cur, pg, true);
		assert(err != -EAGAIN);
		if (err == 1) {
			/*
			 * we have replaced the entry, our work is done.
			 */
			BT_RELEASE(pg);
			goto out;
		}

		if (pg->hdr.__used_ent16 + 1 == BT_MAX_ENT) {
			/*
			 * our leaf is now full. we need to split after we add this entry.
			 * to do it, we hold locks for parents.
			 * the locks for parents are done recursively, and essensially
			 * up to a parent node that has sufficient empty spaces.
			 * this must be done BEFORE touching the leaf's entry because
			 * locking may fail.
			 */
			cur->path[depth].in_lock = 1;
			err = bt_path_lock_for_split(cur, depth - 1);
			if (err == -EAGAIN) {
				cur->path[depth].in_lock = 0;
				BT_RELEASE(pg);
				goto out;
			}
		}

		bt_add_ent(pg, cur->key, cur->val);
		/*
		 * now, we have modified an entry.
		 * from here, we will never ever retry.
		 */
		if (!cur->path[depth].in_lock) {
			/*
			 * when we need a split, we do not release the lock.
			 * otherwise, we come to this path and free the lock.
			 */
			BT_RELEASE(pg);
		}
	} else {
		struct bt_pg *cpg;
		cur->path[depth].ver = pg->latch.ver.load(std::memory_order_acquire);
		for (i = 0; i < pg->hdr.__used_ent16; i++) {
			if (cur->key < pg->__key64[i])
				break;
		}
		cpg = pg->__ptr64[i];
		if (BT_IS_LOCKED(pg) || cur->path[depth].ver != pg->latch.ver.load(std::memory_order_acquire)) {
			/*
			 * at this point, freshness of the pointer to a child node has to be guaranteed
			 * in order to be able to detect change on this node from the point of view
			 * of this child node.
			 *
			 * at least, up to here, we have chosen a correct path that is not modified by anybody.
			 * consequently, change of the version number tells us the modification on this node.
			 */
			err = -EAGAIN;
			goto out;
		}
		cur->path[depth].in_lock = 0;
		/*
		 * while we are going through here, someone may modify this node because we don't have the lock, and
		 * the child node that we are trying to looking into may not be the correct one anymore.
		 * but, we will perform detection in the leaf, so just keep going.
		 * if we find we came to a wrong leaf, we will retry.
		 */
		err = __bt_insert(cur, cpg, depth + 1);
		if (err == -EAGAIN)
			goto out;
		else if (cur->path[depth].in_lock) {
			if (cpg->hdr.__used_ent16 == BT_MAX_ENT)
				bt_split(cur, pg, cpg);
			BT_RELEASE(cpg);
			if (!cur->path[depth-1].in_lock)
				BT_RELEASE(pg);
		}
	}
out:
	return err;
}

static int bt_insert(struct bt_cur *cur, struct bt_pg *pg)
{
	int err;
	struct bt_pg *cpg;
recheck:
	if (cur->waitlock.load(std::memory_order_acquire))
		goto recheck;
	bt_do_pg_free(cur);
	set_enter_time(cur);
	cur->fetch_cb = bt_fetch_cb_insert;
retry:
	cur->depth = 0;
	cur->path[0].ver = pg->latch.ver.load(std::memory_order_acquire);
	cur->path[0].pg = pg;
	cpg = pg->__ptr64[0];
	if (BT_IS_LOCKED(pg) || (cur->path[0].ver != pg->latch.ver.load(std::memory_order_acquire)))
		goto retry;
	cur->path[0].in_lock = 0;
	err = __bt_insert(cur, cpg, 1);
	if (err == -EAGAIN)
		goto retry;
	else if (cur->path[0].in_lock) {
		if (cpg->hdr.__used_ent16 == BT_MAX_ENT)
			bt_grow(cur, pg);
		BT_RELEASE(cpg);
		BT_RELEASE(pg);
	}
	set_exit_time(cur);
	return 0;
}

static int bt_fetch_cb_delete(struct bt_cur *cur, struct bt_pg *_p, uint16_t _i)
{
	int err = 1;
	struct bt_del_info *di = (struct bt_del_info *) cur->ptr;
	struct bt_pg *pg = di->pg, *del_pg = di->del_pg;
	uint16_t i = di->idx, del_ent = di->del_ent;
	struct bt_item *item = _p->__val64[_i];

	if (del_pg && (del_pg->__key64[del_ent] == cur->key) && i == 0) {
		struct bt_pg *next = NULL;
		if (pg->hdr.__nxt64)
			next = (struct bt_pg *) pg->hdr.__nxt64;

		assert(cur->path[di->del_depth].pg == del_pg);
		/* have a lock of del_pg if we do not take it already */
		if (!cur->path[di->del_depth].in_lock && !BT_TRYLOCK(del_pg)) {
			err = -EAGAIN;
			BT_RELEASE(next);
			goto out;
		}

		if (pg->hdr.__used_ent16 > 1)
			del_pg->__key64[del_ent] = pg->__key64[1];
		else if (next) {
			/*
			 * in this path, the leaf node will disappear.
			 * in this case, we already have the lock for next.
			 * so, we can trust values on next.
			 */
			del_pg->__key64[del_ent] = next->__key64[0];
		}

		if (!cur->path[di->del_depth].in_lock)
			BT_RELEASE(del_pg);
	}
	free(item);
	if (_p != pg) {
		_p->__val64[_i] = pg->__val64[i];
		memmove(&pg->__key64[i], &pg->__key64[i+1],
				sizeof(uint64_t) * (pg->hdr.__used_ent16 - 1 - i));
		memmove(&pg->__val64[i], &pg->__val64[i+1],
				sizeof(uint64_t) * (pg->hdr.__used_ent16 - i));
	} else {
		memmove(&pg->__key64[_i], &pg->__key64[_i+1],
				sizeof(uint64_t) * (pg->hdr.__used_ent16 - 1 - _i));
		memmove(&pg->__val64[_i], &pg->__val64[_i+1],
				sizeof(uint64_t) * (pg->hdr.__used_ent16 - _i));
		i = _i;
	}
	pg->hdr.__used_ent16--;
out:
	return err;
}

static int bt_path_lock_for_delete(struct bt_cur *cur, uint16_t depth)
{
	int err = 0;
	struct bt_pg *pg = cur->path[depth].pg, *lpg = NULL;

	if (!BT_TRYLOCK(pg)) {
		/* 
		 * taking the lock from a child to its parent.
		 * if it fails to have a lock, it means that some modification happens
		 * on the parent. in other words, the trial for the modification on
		 * the child has to be retried. so, if we fail to have a lock even once,
		 * just give up and go back to retry.
		 *
		 * this is necessary to avoid deadlock.
		 */
		err = -EAGAIN;
		cur->path[depth].in_lock = 0;
		goto out;
	}
	if (cur->path[depth].ver != pg->latch.ver.load(std::memory_order_acquire)) {
		/*
		 * ok, now we have a lock, but somebody has modified this node.
		 * in this case, what we are trying to do may not be the correct behavior
		 * anymore as the same reason as insert. so, just give up and return to retry.
		 */
		err = -EAGAIN;
		cur->path[depth].in_lock = 0;
		BT_RELEASE(pg);
		goto out;
	}

	cur->path[depth].in_lock = 1;

	/*
	 * here, we have the lock of this node, and this won't be modified.
	 * thus, the pointer to the child is always valid.
	 * and we can spend plenty of time to wait until someone releases the child's lock.
	 */

	if (cur->path[depth+1].i == 0)
		lpg = cur->path[depth]._c_n_pg = pg->__ptr64[1];
	else
		lpg = cur->path[depth]._c_p_pg = pg->__ptr64[cur->path[depth+1].i-1];

	assert(cur->path[depth+1].pg != lpg);

	if (lpg->hdr.leaf && (lpg->hdr.__used_ent16 == 1) && (pg->hdr.__used_ent16 == 1))
		cur->path[depth+1].need_another_lock = 1;
	else
		cur->path[depth+1].need_another_lock = 0;

	/*
	 * taking the lock from a parent to a child.
	 * this case is different from the child to parent locking case.
	 * because, now we have a lock of the parent node, and the child to parent locking yields and
	 * go back to retry if the executor detects our lock. So, let's just wait until the
	 * child finds us and releases its lock.
	 */

	BT_LOCK(lpg);

	if (pg->hdr.__used_ent16 > 1) {
		/*
		 * this node has sufficient entries and won't be disappear immediately.
		 * so, let's go back to carry out acutual deletion.
		 */
		goto out;
	}
	/*
	 * here, this node also does not have enough entries and will disappear when
	 * we remove the leaf. so we need to take another lock in the parent of this node.
	 */
	err = bt_path_lock_for_delete(cur, depth - 1);
	if (err == -EAGAIN) {
		cur->path[depth].in_lock = 0;
		BT_RELEASE(lpg);
		BT_RELEASE(pg);
	}
out:
	return err;
}

static void bt_path_unlock_for_delete(struct bt_cur *cur, uint16_t depth)
{
	uint16_t i;
	for (i = 0; i <= depth; i++) {
		if (cur->path[i].in_lock) {
			cur->path[i].in_lock = 0;
			if (cur->path[i]._c_n_pg)
				BT_RELEASE(cur->path[i]._c_n_pg);
			if (cur->path[i]._c_p_pg)
				BT_RELEASE(cur->path[i]._c_p_pg);
			BT_RELEASE(cur->path[i].pg);
		}
	}
}

static int __bt_delete(struct bt_cur *cur, struct bt_pg *pg,  uint16_t depth)
{
	int err = 0;
	struct bt_pg *cpg;
	int i;

	assert(depth < BT_MAX_DEPTH);
	cur->depth = depth;
	cur->path[depth].pg = pg;

	if (pg->hdr.leaf) {
		struct bt_del_info *di = (struct bt_del_info *) cur->ptr;

		BT_LOCK(pg);

		cur->path[depth].in_lock = 1;
		cur->path[depth]._c_n_pg = NULL;
		cur->path[depth]._c_p_pg = NULL;

		if (BT_IS_LOCKED(cur->path[depth-1].pg))
			goto leaf_again;
		if (cur->path[depth-1].ver != cur->path[depth-1].pg->latch.ver.load(std::memory_order_acquire)) {
			/*
			 * this checks this leaf's parent has been changed.
			 * please refer to __bt_insert for the reason.
			 */
leaf_again:
			err = -EAGAIN;
			goto leaf_retry;
		}

		if (pg->hdr.__used_ent16 == 1) {
			err = bt_path_lock_for_delete(cur, depth - 1);
			if (err == -EAGAIN)
				goto leaf_retry;
			/*
			 * when this leaf disappears, we need to have other prev and next leaves' locks.
			 * while we have the lock of this leaf lock, neighbors won't disappear because
			 * they need the lock we have for removing themselves.
			 */
			if (cur->path[depth].need_another_lock) {
				if (cur->path[depth].i == 0) {
					if (pg->hdr.__pre64) {
						struct bt_pg *prev = (struct bt_pg *) pg->hdr.__pre64;
						if (!BT_TRYLOCK(prev)) {
							err = -EAGAIN;
							goto leaf_retry;
						}
						cur->path[depth-1]._c_p_pg = prev;
						assert(prev != pg);
					}
				} else {
					if (pg->hdr.__nxt64) {
						struct bt_pg *next = (struct bt_pg *) pg->hdr.__nxt64;
						BT_LOCK(next);
						cur->path[depth-1]._c_n_pg = next;
						assert(next != pg);
					}
				}
			}
		}
		di->pg = pg;
		err = fetch_item(cur, pg, true);
		if (err == -EAGAIN) {
leaf_retry:
			bt_path_unlock_for_delete(cur, depth);
			goto out;
		}
		if (!cur->path[depth-1].in_lock) {
			cur->path[depth].in_lock = 0;
			BT_RELEASE(pg);
		}
		goto out;
	}

	cur->path[depth].ver = pg->latch.ver.load(std::memory_order_acquire);

	for (i = 0; i < pg->hdr.__used_ent16; i++) {
		if (cur->key < pg->__key64[i])
			break;
		if (cur->key == pg->__key64[i]) {
			struct bt_del_info *di = (struct bt_del_info *) cur->ptr;
			di->del_pg = pg;
			di->del_ent = i;
			di->del_depth = depth;
			i++;
			break;
		}
	}

	cpg = pg->__ptr64[i];

	if (BT_IS_LOCKED(pg) || cur->path[depth].ver != pg->latch.ver.load(std::memory_order_acquire)) {
		err = -EAGAIN;
		goto out;
	}

	cur->path[depth + 1].in_lock = 0;
	cur->path[depth + 1].need_another_lock = 0;
	cur->path[depth + 1]._c_n_pg = NULL;
	cur->path[depth + 1]._c_p_pg = NULL;
	cur->path[depth + 1].i = i;
	err = __bt_delete(cur, cpg, depth + 1);
	assert(cur->path[depth+1].pg != cur->path[depth]._c_n_pg);
	assert(cur->path[depth+1].pg != cur->path[depth]._c_p_pg);
	if (err == -EAGAIN)
		goto out;
	else if (cur->path[depth].in_lock) {
		struct bt_pg *_l, *_r, *_t, *_p;
		int _idx;
		bool cpg_freed = false;

		if (cpg->hdr.__used_ent16 != 0)
			goto unlock;

		_p = pg;
		if (i == 0) {
			_idx = 0;
			_l = cpg;
			_r = pg->__ptr64[1];
			_t = _r;
		} else {
			_idx = i - 1;
			_l = pg->__ptr64[_idx];
			_r = cpg;
			_t = _l;
		}
		if (_r->hdr.__used_ent16 > 1) {
			if (!cpg->hdr.leaf) {
				_l->__key64[0] = _p->__key64[_idx];
				_p->__key64[_idx] = _r->__key64[0];
				_l->__val64[1] = _r->__val64[0];
				memmove(&(_r->__key64[0]), &(_r->__key64[1]), sizeof(uint64_t) * (_r->hdr.__used_ent16 - 1));
				memmove(&(_r->__val64[0]), &(_r->__val64[1]), sizeof(uint64_t) * _r->hdr.__used_ent16);
			} else {
				_l->__key64[0] = _r->__key64[0];
				_l->__val64[0] = _r->__val64[0];
				memmove(&(_r->__key64[0]), &(_r->__key64[1]), sizeof(uint64_t) * (_r->hdr.__used_ent16 - 1));
				memmove(&(_r->__val64[0]), &(_r->__val64[1]), sizeof(uint64_t) * _r->hdr.__used_ent16);
				_p->__key64[_idx] = _r->__key64[0];
			}
			_l->hdr.__used_ent16++;
			_r->hdr.__used_ent16--;
		} else if (_l->hdr.__used_ent16 > 1) {
			if (!cpg->hdr.leaf) {
				_r->__key64[0] = _p->__key64[_idx];
				_p->__key64[_idx] = _l->__key64[_l->hdr.__used_ent16-1];
				_r->__val64[1] = _r->__val64[0];
				_r->__val64[0] = _l->__val64[_l->hdr.__used_ent16];
			} else {
				_r->__key64[0] = _l->__key64[_l->hdr.__used_ent16-1];
				_r->__val64[0] = _l->__val64[_l->hdr.__used_ent16-1];
				_p->__key64[_idx] = _r->__key64[0];
			}
			_r->hdr.__used_ent16++;
			_l->hdr.__used_ent16--;
		} else {
			if (!cpg->hdr.leaf) {
				if (_r->hdr.__used_ent16) {

					memmove(&(_t->__key64[1]), &(_t->__key64[0]), sizeof(uint64_t) * 1);
					memmove(&(_t->__val64[1]), &(_t->__val64[0]), sizeof(uint64_t) * 2);
					_t->__key64[0] = _p->__key64[_idx];
					_t->__val64[0] = cpg->__val64[0];
				} else {
					_t->__key64[1] = _p->__key64[_idx];
					_t->__val64[2] = cpg->__val64[0];
				}
				memmove(&(_p->__key64[_idx]), &(_p->__key64[_idx+1]), sizeof(uint64_t) * (_p->hdr.__used_ent16 - _idx - 1));
				memmove(&(_p->__val64[_idx]), &(_p->__val64[_idx+1]), sizeof(uint64_t) * (_p->hdr.__used_ent16 - _idx));
				if (_r->hdr.__used_ent16)
					assert(_p->__ptr64[_idx] == _t);
				else
					_p->__ptr64[_idx] = _t;
				_p->hdr.__used_ent16--;
				_t->hdr.__used_ent16++;
			} else {
				if (_r->hdr.__used_ent16) {
					_t->hdr.__pre64 = cpg->hdr.__pre64;
					if (cpg->hdr.__pre64 != 0) {
						struct bt_pg *prev = (struct bt_pg *) cpg->hdr.__pre64;
						prev->hdr.__nxt64 = _t;
					}
				} else {
					_t->hdr.__nxt64 = cpg->hdr.__nxt64;
					if (cpg->hdr.__nxt64 != 0) {
						struct bt_pg *next = (struct bt_pg *) cpg->hdr.__nxt64;
						next->hdr.__pre64 = _t;
					}
				}
				memmove(&(_p->__key64[_idx]), &(_p->__key64[_idx+1]), sizeof(uint64_t) * (_p->hdr.__used_ent16 - _idx - 1));
				memmove(&(_p->__val64[_idx]), &(_p->__val64[_idx+1]), sizeof(uint64_t) * (_p->hdr.__used_ent16 - _idx));
				if (_r->hdr.__used_ent16)
					assert(_p->__ptr64[_idx] == _t);
				else
					_p->__ptr64[_idx] = _t;
				_p->hdr.__used_ent16--;
			}
			bt_pg_free(cur, cpg);
			cpg_freed = true;
		}
unlock:
		if (cur->path[depth]._c_n_pg)
			BT_RELEASE(cur->path[depth]._c_n_pg);
		if (cur->path[depth]._c_p_pg)
			BT_RELEASE(cur->path[depth]._c_p_pg);
		if (!cpg_freed)
			BT_RELEASE(cpg);
		if (!cur->path[depth-1].in_lock)
			BT_RELEASE(pg);
	}
out:
	return err;
}

static int bt_delete(struct bt_cur *cur, struct bt_pg *pg)
{
	int err;
	struct bt_pg *cpg;
	struct bt_del_info di;
recheck:
	if (cur->waitlock.load(std::memory_order_acquire))
		goto recheck;
	bt_do_pg_free(cur);
	set_enter_time(cur);
	cur->ptr = &di;
	cur->fetch_cb = bt_fetch_cb_delete;
retry:
	cur->depth = 0;
	di.del_pg = NULL;
	memset(&cur->path[0], 0, sizeof(struct bt_path) * 2);
	cur->path[0].pg = pg;
	cur->path[0].ver = pg->latch.ver.load(std::memory_order_acquire);
	cpg = pg->__ptr64[0];
	if (BT_IS_LOCKED(pg) || cur->path[0].ver != pg->latch.ver.load(std::memory_order_acquire))
		goto retry;
	err = __bt_delete(cur, cpg, 1);
	if (cur->path[1].in_lock)
		BT_RELEASE(cpg);
	if (cur->path[0].in_lock)
		BT_RELEASE(pg);
	if (err == -EAGAIN)
		goto retry;
	set_exit_time(cur);
	return err;
}

__thread std::unordered_map<std::string, struct bt_cur *> *__thread_bkvs_cur;

class BTKVSImpl : public BKVS {
private:
	std::unordered_map<std::string, struct bt *> trees;
	pthread_mutex_t hash_mtx;

	int doLookup(struct bt_cur *cur, struct bt_pg *pg) {
		return bt_lookup(cur, pg);
	}

	int doInsert(struct bt_cur *cur, struct bt_pg *pg) {
		return bt_insert(cur, pg);
	}

	int doDelete(struct bt_cur *cur, struct bt_pg *pg) {
		return bt_delete(cur, pg);
	}

	struct bt *doCreate(void)
	{
		return bt_create();
	}

	struct bt_cur *fetch_cur(const char *table)
	{
		struct bt_cur *cur;
		struct bt *tbl;
		if (!table)
			table = "default";
		if (!__thread_bkvs_cur)
			__thread_bkvs_cur = new std::unordered_map<std::string, struct bt_cur *>;
		auto search = trees.find(std::string(table));
		if (search == trees.end()) {
			pthread_mutex_lock(&hash_mtx);
			auto search= trees.find(std::string(table));
			if (search == trees.end()) {
				tbl = doCreate();
				assert(tbl);
				trees.insert({std::string(table), tbl});
			} else
				tbl = search->second;
			pthread_mutex_unlock(&hash_mtx);
		} else
			tbl = search->second;
		std::unordered_map<std::string, struct bt_cur *> &hash_cur = *__thread_bkvs_cur;;
		if (hash_cur.find(std::string(table)) == hash_cur.end())
			hash_cur[std::string(table)] = cur = bt_cur_alloc(tbl);
		else
			cur = hash_cur[std::string(table)];
		return cur;
	}

public:
	BTKVSImpl()
	{
		trees.clear();
		pthread_mutex_init(&hash_mtx, NULL);
	}

	~BTKVSImpl()
	{
		if (__thread_bkvs_cur)
			delete __thread_bkvs_cur;
	}

	int Get(const char *table, const char *key, uint32_t kl,
		const std::vector<std::string> &fields,
		std::vector<std::pair<std::string, std::string>> &results) override
	{
		struct bt_cur *cur = fetch_cur(table);
		if (!cur)
			return 0;
		cur->key = jenkins_one_at_a_time_hash(key, kl);
		cur->keystr = key;
		cur->kl = kl;
		cur->fields = &fields;
		cur->results = &results;
		return doLookup(cur, cur->bt->root);
	}

	int Get(const char *table, const char *key, uint32_t kl,
		std::vector<std::pair<std::string, std::string>> &results) override
	{
		const std::vector<std::string> *_ptr = NULL;
		const std::vector<std::string> &fields = *_ptr;
		return Get(table, key, kl, fields, results);
	}

	const char *Get(const char *table, const char *key, uint32_t kl) override
	{
		int ret;
		const std::vector<std::string> *_ptr = NULL;
		const std::vector<std::string> &fields = *_ptr;
		std::vector<std::pair<std::string, std::string>> result;
		ret = Get(table, key, kl, fields, result);
		if (ret) {
			std::stringstream ss;
			for (std::pair<std::string,std::string> &p : result) {
				if (p.first.size() == 0)
					ss << p.second;
				else
					ss << p.first << ":" << p.second;
			}
			return strdup(ss.str().c_str());
		} else
			return NULL;
	}

	int Put(const char *table, const char *key, uint32_t kl,
		std::vector<std::pair<std::string, std::string>> &values) override
	{
		struct bt_item *item, *field, *prev = NULL;
		struct bt_cur *cur = fetch_cur(table);
		item = (struct bt_item *) bt_alloc_obj(cur->bt, sizeof(struct bt_item) + kl + 1);
		assert(item);
		item->kl = kl;
		memcpy(item->data, key, kl);
		item->data[kl] = '\0';
		item->ptr = NULL;
		item->chain = NULL;
		item->latch.lock = 0;
		for (std::pair<std::string, std::string> &p : values) {
			field = (struct bt_item *) bt_alloc_obj(cur->bt, sizeof(struct bt_item) + p.first.size() + p.second.size() + 2);
			assert(field);
			field->ptr = NULL;
			field->kl = p.first.size();
			field->vl = p.second.size();
			memcpy(field->data, p.first.c_str(), field->kl);
			field->data[field->kl] = '\0';
			memcpy(&field->data[field->kl+1], p.second.c_str(), field->vl);
			field->data[field->kl+1+field->vl] = '\0';
			if (!item->ptr)
				item->ptr = field;
			else
				prev->ptr = field;
			prev = field;
		}
		cur->key = jenkins_one_at_a_time_hash(key, kl);
		cur->keystr = key;
		cur->val = (uint64_t) item;
		cur->kl = kl;
		return doInsert(cur, cur->bt->root);
	}

	int Put(const char *table, const char *key, uint32_t kl, const char *val, uint32_t vl) override
	{
		(void) vl;
		std::vector<std::pair<std::string, std::string>> v;
		std::pair<std::string, std::string> p = std::pair<std::string, std::string>(std::string(""), std::string(val));
		v.push_back(p);
		return Put(table, key, kl, v);
	}

	int Del(const char *table, const char *key, uint32_t kl) override
	{
		struct bt_cur *cur = fetch_cur(table);
		if (!cur)
			return 0;
		cur->key = jenkins_one_at_a_time_hash(key, kl);
		cur->keystr = key;
		cur->kl = kl;
		return doDelete(cur, cur->bt->root);
	}
};

BKVS *createBTKVS(void)
{
	BTKVSImpl *i = new BTKVSImpl();
	return i;
}
