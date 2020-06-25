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

#ifndef _KVS_H
#define _KVS_H

#include <vector>
#include <string>
#include <utility>

#include <stdint.h>
#include <string.h>

class BKVS {
public:
	BKVS() {}
	virtual ~BKVS() {}

	virtual int Get(const char *table, const char *key, uint32_t kl,
			const std::vector<std::string> &fields,
			std::vector<std::pair<std::string, std::string>> &results)
	{
		(void) table;
		(void) key;
		(void) kl;
		(void) fields;
		(void) results;
		return -ENOTSUP;
	}

	virtual int Get(const char *table, const char *key, uint32_t kl,
			std::vector<std::pair<std::string, std::string>> &results)
	{
		(void) table;
		(void) key;
		(void) kl;
		(void) results;
		return -ENOTSUP;
	}

	virtual const char *Get(const char *table, const char *key, uint32_t kl)
	{
		(void) table;
		(void) key;
		(void) kl;
		return NULL;
	}

	const char *Get(const char *table, const char *key)
	{
		return Get(table, key, strlen(key));
	}

	const char *Get(const char *key, uint32_t kl)
	{
		return Get(NULL, key, kl);
	}

	const char *Get(const char *key)
	{
		return Get(NULL, key);
	}

	virtual int Put(const char *table, const char *key, uint32_t kl,
			std::vector<std::pair<std::string, std::string>> &values)
	{
		(void) table;
		(void) key;
		(void) kl;
		(void) values;
		return -ENOTSUP;
	}

	virtual int Put(const char *table, const char *key, uint32_t kl, const char *val, uint32_t vl)
	{
		(void) table;
		(void) key;
		(void) kl;
		(void) val;
		(void) vl;
		return -ENOTSUP;
	}

	int Put(const char *table, const char *key, const char *val)
	{
		return Put(table, key, strlen(key), val, strlen(val));
	}

	int Put(const char *key, uint32_t kl, const char *val, uint32_t vl)
	{
		return Put(NULL, key, kl, val, vl);
	}

	int Put(const char *key, const char *val)
	{
		return Put(NULL, key, val);
	}

	virtual int Del(const char *table, const char *key, uint32_t kl)
	{
		(void) table;
		(void) key;
		(void) kl;
		return -ENOTSUP;
	}

	int Del(const char *table, const char *key)
	{
		return Del(table, key, strlen(key));
	}

	int Del(const char *key, uint32_t kl)
	{
		return Del(NULL, key, kl);
	}

	int Del(const char *key)
	{
		return Del(NULL, key);
	}

	int Update(const char *table, const char *key, uint32_t kl,
		   std::vector<std::pair<std::string, std::string>> &values)
	{
		return Put(table, key, kl, values);
	}

	int Update(const char *table, const char *key, uint32_t kl, const char *val, uint32_t vl)
	{
		return Put(table, key, kl, val, vl);
	}

	int Update(const char *table, const char *key, const char *val)
	{
		return Put(table, key, strlen(key), val, strlen(val));
	}

	int Update(const char *key, uint32_t kl, const char *val, uint32_t vl)
	{
		return Put(NULL, key, kl, val, vl);
	}

	int Update(const char *key, const char *val)
	{
		return Put(NULL, key, val);
	}
};

BKVS *createBTKVS(void);

#endif
