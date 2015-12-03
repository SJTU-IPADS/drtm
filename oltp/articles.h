/*  
 * Copyright (c) 2015 Shanghai Jiao Tong University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://ipads.se.sjtu.edu.cn/drtm.html
 *
 *
 */

#ifndef _NDB_BENCH_ARTI_H_
#define _NDB_BENCH_ARTI_H_

#include "encoder.h"
#include "inline_str.h"
#include "macros.h"
#include "bench.h"

#define USERS_KEY_FIELDS(x, y) \
	x(int64_t, u_id)
#define USERS_VALUE_FIELDS(x, y) \
	x(inline_str_8<100>, u_first) \
	y(inline_str_8<100>, u_last) \
	y(inline_str_8<100>, u_password) \
	y(inline_str_8<100>, u_email)
DO_STRUCT(users, USERS_KEY_FIELDS, USERS_VALUE_FIELDS)

#define ARTICLES_KEY_FIELDS(x, y) \
	x(int64_t, a_id) 
#define ARTICLES_VALUE_FIELDS(x, y) \
	x(inline_str_8<100>, a_title) \
	y(inline_str_8<100>, a_text) \
	y(uint64_t, a_num_comments) 
DO_STRUCT(articles, ARTICLES_KEY_FIELDS, ARTICLES_VALUE_FIELDS)

#define COMMENTS_KEY_FIELDS(x, y) \
	x(int64_t, c_id) 
#define COMMENTS_VALUE_FIELDS(x, y) \
	x(int64_t, c_a_id) \
	y(int64_t, c_u_id) \
	y(inline_str_8<100>, c_text) 
DO_STRUCT(comments, COMMENTS_KEY_FIELDS, COMMENTS_VALUE_FIELDS)

#define COMM_INDEX_KEY_FIELDS(x, y) \
	x(int64_t, c_index_id) 
#define COMM_INDEX_VALUE_FIELDS(x, y) \
	x(uint8_t,o_dummy)
DO_STRUCT(comm_index, COMM_INDEX_KEY_FIELDS, COMMENTS_VALUE_FIELDS)

#endif
