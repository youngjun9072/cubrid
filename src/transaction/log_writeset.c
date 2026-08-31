/*
 * Copyright 2008 Search Solution Corporation
 * Copyright 2016 CUBRID Corporation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

/*
 * log_writeset.c - writeset collection and global commit history (writeset PoC)
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#include "log_writeset.h"

#include "error_manager.h"
#include "log_impl.h"
#include "object_domain.h"
#include "object_primitive.h"
#include "object_representation.h"
#include "porting.h"

#include "memory_wrapper.hpp"	// XXX: SHOULD BE THE LAST INCLUDE HEADER

#if defined(SERVER_MODE) || defined(SA_MODE)

/*
 * log_writeset_clock_ns - monotonic wall clock in nanoseconds
 *
 * Note: TEST ONLY (writeset perf). Backs the collect/probe/flush/commit timing
 *       instrumentation; remove together with the perf er_log_debug lines.
 */
UINT64
log_writeset_clock_ns (void)
{
  struct timespec ts;

  clock_gettime (CLOCK_MONOTONIC, &ts);
  return (UINT64) ts.tv_sec * 1000000000ULL + (UINT64) ts.tv_nsec;
}

/* FNV-1a 64-bit constants */
#define LOG_WRITESET_FNV_OFFSET_BASIS ((UINT64) 0xcbf29ce484222325ULL)
#define LOG_WRITESET_FNV_PRIME        ((UINT64) 0x00000100000001b3ULL)

LOG_WRITESET_HISTORY log_Writeset_history;

/* commit-order baseline: the most recent commit LSA published to the history.
 * Guarded by log_Writeset_history.latch (advanced monotonically in commit_flush). */
static LOG_LSA log_Writeset_prev_commit_lsa;

static UINT64 log_writeset_fnv1a (const OID * class_oid, const char *packed, int len);
static int log_writeset_add_dbvalue_internal (THREAD_ENTRY * thread_p, LOG_TDES * tdes, const OID * class_oid,
					      DB_VALUE * pk);
static int log_writeset_add_ref_dbvalue_internal (THREAD_ENTRY * thread_p, LOG_TDES * tdes, const OID * ref_class_oid,
						  DB_VALUE * fk_value, struct tp_domain *parent_pk_domain);
static int log_writeset_push (THREAD_ENTRY * thread_p, LOG_TDES * tdes, const OID * class_oid, const char *packed,
			      int len, LOG_WRITESET_KIND kind);
#if !defined (NDEBUG)
static void log_writeset_selfcheck_packing (void);
#endif /* !NDEBUG */

/*
 * log_writeset_fnv1a - FNV-1a 64-bit hash over class_oid (8 bytes) + packed key
 */
static UINT64
log_writeset_fnv1a (const OID * class_oid, const char *packed, int len)
{
  UINT64 hash = LOG_WRITESET_FNV_OFFSET_BASIS;
  unsigned char oid_bytes[8];
  int i;

  /* class_oid: 8 bytes = pageid (4) + slotid (2) + volid (2) */
  memcpy (&oid_bytes[0], &class_oid->pageid, sizeof (class_oid->pageid));
  memcpy (&oid_bytes[4], &class_oid->slotid, sizeof (class_oid->slotid));
  memcpy (&oid_bytes[6], &class_oid->volid, sizeof (class_oid->volid));

  for (i = 0; i < 8; i++)
    {
      hash ^= (UINT64) oid_bytes[i];
      hash *= LOG_WRITESET_FNV_PRIME;
    }

  for (i = 0; i < len; i++)
    {
      hash ^= (UINT64) (unsigned char) packed[i];
      hash *= LOG_WRITESET_FNV_PRIME;
    }

  return hash;
}

/*
 * log_writeset_history_initialize - allocate and initialize the global commit history
 *
 * return: NO_ERROR, or ER_OUT_OF_VIRTUAL_MEMORY on allocation failure
 */
int
log_writeset_history_initialize (void)
{
  log_Writeset_history.map.clear ();
  LSA_SET_NULL (&log_Writeset_history.history_start);
  pthread_mutex_init (&log_Writeset_history.latch, NULL);

  LSA_SET_NULL (&log_Writeset_prev_commit_lsa);

  /* TEST ONLY (writeset PoC 검증): 서버 부팅에 의한 히스토리 초기화. 가득 차서 비우는
   * commit_flush 경로("CLEAR (full)")와 구분되도록 다른 태그로 남긴다. 검증 후 제거. */
  er_log_debug (ARG_FILE_LINE, "writeset history INIT (server boot): cap_limit=%d\n", LOG_WRITESET_HISTORY_CAP);

  return NO_ERROR;
}

/*
 * log_writeset_history_finalize - free the global commit history
 */
void
log_writeset_history_finalize (void)
{
  log_Writeset_history.map.clear ();
  LSA_SET_NULL (&log_Writeset_history.history_start);
  pthread_mutex_destroy (&log_Writeset_history.latch);

  LSA_SET_NULL (&log_Writeset_prev_commit_lsa);
}

/*
 * log_writeset_push - hash a packed key and append it to the transaction's writeset
 *
 *   tdes(in/out): transaction descriptor
 *   class_oid(in): class OID whose identity the key belongs to (the modified row's class for
 *                  WRITE keys, the referenced parent class for FK REF keys)
 *   packed(in): packed key image
 *   len(in): length of the packed image
 *   kind(in): WRITE (this transaction owns the key) or REF (foreign-key reference only)
 *
 * return: NO_ERROR, or ER_OUT_OF_VIRTUAL_MEMORY on allocation failure
 *
 * Note: On per-tx limit overflow the collected writeset is dropped and the
 *       transaction is marked to be committed in commit order (ws_overflow).
 */
static int
log_writeset_push (THREAD_ENTRY * thread_p, LOG_TDES * tdes, const OID * class_oid, const char *packed, int len,
		   LOG_WRITESET_KIND kind)
{
  LOG_WRITESET_ENTRY entry;

  if (tdes == NULL || class_oid == NULL)
    {
      return NO_ERROR;
    }

  if (tdes->ws_overflow)
    {
      /* already dropped this transaction's writeset */
      return NO_ERROR;
    }

  /* ws_overflow 는 메모리 캡이 아니라 "정합성" 안전장치다. 부분 수집이 왜 위험한가:
   *   1. writeset 이 불완전해지면 트랜잭션이 실제보다 "독립적"으로 보인다.
   *   2. 슬레이브 게이트가 그걸 믿고 잘못 병렬화 -> 같은 행 순서가 붕괴한다.
   *   3. 호출부가 반환값을 (void) 로 무시하므로 이 오류는 조용히 발생한다.
   * 그래서 per-tx 한도를 넘기면 부분 writeset 을 통째로 버리고 commit-order 로 격하한다
   * (= MySQL has_missing_keys). REF 해시도 tdes->ws_hashes 메모리를 쓰므로 함께 센다.
   * PoC 단순화 대상이 아니다 - 제거 금지. */
  if (tdes->ws_hashes.size () >= LOG_WRITESET_TX_LIMIT)
    {
      /* per-tx limit reached: drop writeset, degrade to commit order */
      tdes->ws_overflow = true;
      tdes->ws_hashes.clear ();
      tdes->ws_hashes.shrink_to_fit ();
      /* TEST ONLY (writeset PoC 검증): per-tx 한도 초과로 writeset 을 버리고 commit-order 로 격하.
       * 이 트랜잭션은 직전 커밋 전부를 기다리는 직렬화 배리어가 된다. 검증 후 제거. */
      er_log_debug (ARG_FILE_LINE, "writeset FALLBACK(overflow) trid=%d: ws_keys reached per-tx limit=%d, dropped\n",
		    tdes->trid, LOG_WRITESET_TX_LIMIT);
      return NO_ERROR;
    }

  entry.hash = log_writeset_fnv1a (class_oid, packed, len);
  entry.kind = kind;
  tdes->ws_hashes.push_back (entry);

  /* TEST ONLY (writeset PoC 검증): 수집한 해시를 서버 에러로그로 남긴다. 검증 후 제거할 것. */
  er_log_debug (ARG_FILE_LINE, "writeset insert hash: trid=%d kind=%s class=%d|%d|%d packed_len=%d hash=%016llx\n",
		tdes->trid, (kind == LOG_WRITESET_KIND_REF ? "REF" : "WRITE"), (int) class_oid->volid,
		(int) class_oid->pageid, (int) class_oid->slotid, len, (unsigned long long) entry.hash);

  return NO_ERROR;
}

/*
 * log_writeset_add_key - add a distinct writeset WRITE key hash to the transaction
 *
 *   tdes(in/out): transaction descriptor
 *   class_oid(in): class OID of the modified instance
 *   packed(in): packed primary key image
 *   len(in): length of the packed image
 *
 * return: NO_ERROR, or ER_OUT_OF_VIRTUAL_MEMORY on allocation failure
 */
int
log_writeset_add_key (THREAD_ENTRY * thread_p, LOG_TDES * tdes, const OID * class_oid, const char *packed, int len)
{
  return log_writeset_push (thread_p, tdes, class_oid, packed, len, LOG_WRITESET_KIND_WRITE);
}

/*
 * log_writeset_add_dbvalue - pack a DB_VALUE primary key and add its hash
 *
 *   tdes(in/out): transaction descriptor
 *   class_oid(in): class OID of the modified instance
 *   pk(in): primary key value
 *
 * return: NO_ERROR, or an error code on failure
 */
static int
log_writeset_add_dbvalue_internal (THREAD_ENTRY * thread_p, LOG_TDES * tdes, const OID * class_oid, DB_VALUE * pk)
{
  char *buf = NULL;
  char *ptr;
  int buf_len;
  int packed_len = 0;
  int error;

  if (tdes == NULL || class_oid == NULL || pk == NULL)
    {
      return NO_ERROR;
    }

  if (tdes->ws_overflow)
    {
      return NO_ERROR;
    }

  buf_len = OR_VALUE_ALIGNED_SIZE (pk);
  if (buf_len <= 0)
    {
      return NO_ERROR;
    }

  buf = (char *) malloc ((size_t) buf_len);
  if (buf == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) buf_len);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* 반드시 0 으로 초기화한다: or_pack_mem_value 는 도메인과 값 사이 정렬(or_get_align64)로 건너뛴
   * 패딩 바이트를 채우지 않는데, packed_len 은 그 패딩을 포함한다. memset 없이 malloc 쓰레기가 남으면
   * 같은 행(class_oid, PK)이라도 호출 경로(INSERT vs UPDATE/DELETE)마다 해시가 달라져 의존성 검출이
   * 깨진다. replication.c 는 같은 이유로 memset 하지만(valgrind), 우리는 정합성 문제이므로 전 빌드에서 한다. */
  memset (buf, 0, (size_t) buf_len);

  ptr = or_pack_mem_value (buf, pk, &packed_len);
  if (ptr == NULL)
    {
      free_and_init (buf);
      return NO_ERROR;
    }

  error = log_writeset_add_key (thread_p, tdes, class_oid, buf, packed_len);

  free_and_init (buf);

  return error;
}

int
log_writeset_add_dbvalue (THREAD_ENTRY * thread_p, LOG_TDES * tdes, const OID * class_oid, DB_VALUE * pk)
{
  UINT64 perf_t0 = log_writeset_clock_ns ();
  int error = log_writeset_add_dbvalue_internal (thread_p, tdes, class_oid, pk);

  if (tdes != NULL)
    {
      /* TEST ONLY (writeset perf): per-key pack+hash time, reported once per commit at probe */
      tdes->ws_stat_collect_ns += log_writeset_clock_ns () - perf_t0;
    }
  return error;
}

/*
 * log_writeset_add_ref_dbvalue - add a foreign-key reference hash to the transaction
 *
 *   tdes(in/out): transaction descriptor
 *   ref_class_oid(in): class OID of the referenced (parent) table
 *   fk_value(in): the child row's foreign-key value
 *   parent_pk_domain(in): domain of the parent primary-key b-tree
 *
 * return: NO_ERROR, or an error code on failure
 *
 * Note: The parent row published a WRITE hash over (its class OID, its primary key packed in
 *       its own domain). To make this reference collide with that hash, the child value must be
 *       packed identically: or_pack_mem_value derives the packed domain from the value itself,
 *       so the child value is first cast into the parent primary-key domain. The resulting hash
 *       is stored as a REF - it gates this transaction behind the parent's commit but is never
 *       published, so children of the same parent stay independent of one another.
 *
 *       Composite (multi-column) parent keys are not matched yet: their packed midxkey byte
 *       equivalence across parent and child is not proven, so they are skipped and ordering
 *       falls back to the conservative existing path. NULL foreign keys are skipped, matching
 *       the master's own foreign-key check.
 */
static int
log_writeset_add_ref_dbvalue_internal (THREAD_ENTRY * thread_p, LOG_TDES * tdes, const OID * ref_class_oid,
				       DB_VALUE * fk_value, struct tp_domain *parent_pk_domain)
{
  DB_VALUE casted;
  char *buf = NULL;
  char *ptr;
  int buf_len;
  int packed_len = 0;
  int error;
  TP_DOMAIN_STATUS cast_status;

#if !defined (NDEBUG)
  /* Verify once that a child value cast into the parent primary-key domain packs to the same bytes
   * as the parent's own key - the invariant the REF hash relies on. It uses parameterized domains
   * (tp_domain_resolve) and tp_value_cast, so it must run only after the type system is up. The
   * first foreign-key reference is collected during ordinary DML on a fully booted server, which is
   * always past tp_init in every mode, whereas the history init runs before tp_init on the recreate
   * boot path. Debug builds only. */
  {
    static pthread_once_t selfcheck_once = PTHREAD_ONCE_INIT;
    pthread_once (&selfcheck_once, log_writeset_selfcheck_packing);
  }
#endif /* !NDEBUG */

  if (tdes == NULL || ref_class_oid == NULL || fk_value == NULL || parent_pk_domain == NULL)
    {
      return NO_ERROR;
    }

  if (tdes->ws_overflow)
    {
      return NO_ERROR;
    }

  if (DB_IS_NULL (fk_value))
    {
      return NO_ERROR;
    }

  /* multi-column parent key: packed midxkey equivalence not yet proven, skip */
  if (TP_DOMAIN_TYPE (parent_pk_domain) == DB_TYPE_MIDXKEY)
    {
      return NO_ERROR;
    }

  db_make_null (&casted);

  cast_status = tp_value_cast (fk_value, &casted, parent_pk_domain, false);
  if (cast_status != DOMAIN_COMPATIBLE)
    {
      /* cannot reproduce the parent's packed bytes: skip so the existing ordering path stays
       * conservative rather than emitting a hash that would never match the parent */
      pr_clear_value (&casted);
      return NO_ERROR;
    }

  if (DB_IS_NULL (&casted))
    {
      pr_clear_value (&casted);
      return NO_ERROR;
    }

  buf_len = OR_VALUE_ALIGNED_SIZE (&casted);
  if (buf_len <= 0)
    {
      pr_clear_value (&casted);
      return NO_ERROR;
    }

  buf = (char *) malloc ((size_t) buf_len);
  if (buf == NULL)
    {
      pr_clear_value (&casted);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (size_t) buf_len);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* zero the alignment padding for a stable hash, same reason as the WRITE path */
  memset (buf, 0, (size_t) buf_len);

  ptr = or_pack_mem_value (buf, &casted, &packed_len);
  if (ptr == NULL)
    {
      free_and_init (buf);
      pr_clear_value (&casted);
      return NO_ERROR;
    }

  error = log_writeset_push (thread_p, tdes, ref_class_oid, buf, packed_len, LOG_WRITESET_KIND_REF);

  free_and_init (buf);
  pr_clear_value (&casted);

  return error;
}

int
log_writeset_add_ref_dbvalue (THREAD_ENTRY * thread_p, LOG_TDES * tdes, const OID * ref_class_oid,
			      DB_VALUE * fk_value, struct tp_domain *parent_pk_domain)
{
  UINT64 perf_t0 = log_writeset_clock_ns ();
  int error = log_writeset_add_ref_dbvalue_internal (thread_p, tdes, ref_class_oid, fk_value, parent_pk_domain);

  if (tdes != NULL)
    {
      /* TEST ONLY (writeset perf): per-key cast+pack+hash time, reported once per commit at probe */
      tdes->ws_stat_collect_ns += log_writeset_clock_ns () - perf_t0;
    }
  return error;
}

/*
 * log_writeset_commit_probe - compute this transaction's dependency label
 *
 *   ws_parent_out(out): dependency_seq = min (prev_commit_lsa, writeset parent)
 *
 * Note: Called BEFORE the commit LSA is assigned (outside prior_lsa_mutex), under
 *       the dedicated history latch. The writeset parent is the newest commit LSA
 *       among the keys this transaction touched, floored at history_start. Taking
 *       the min with the commit-order baseline lets an independent transaction
 *       (whose parent stays at history_start) gate through immediately, while a
 *       same-key successor inherits its predecessor's commit LSA. An overflowed
 *       (or empty) writeset degrades to the commit-order baseline.
 */
void
log_writeset_commit_probe (THREAD_ENTRY * thread_p, LOG_TDES * tdes, LOG_LSA * ws_parent_out)
{
  LOG_LSA ws_parent;
  UINT64 perf_t0 = log_writeset_clock_ns ();	/* TEST ONLY (writeset perf): includes latch wait */
  size_t perf_wkeys = 0, perf_rkeys = 0, perf_hits = 0;

  pthread_mutex_lock (&log_Writeset_history.latch);

  if (tdes != NULL && tdes->ws_overflow)
    {
      /* no writeset: fall back to commit order (wait for everything before us) */
      LSA_COPY (ws_parent_out, &log_Writeset_prev_commit_lsa);
      /* TEST ONLY (writeset PoC 검증): overflow 트랜잭션은 commit-order 로 격하됨. dependency_seq 가
       * 직전 커밋을 그대로 가리키면 이 경로다(= 직렬화 배리어). 검증 후 제거. */
      er_log_debug (ARG_FILE_LINE,
		    "writeset probe trid=%d OVERFLOW->commit_order dependency_seq=%lld|%d (prev_commit=%lld|%d)\n",
		    (tdes != NULL ? tdes->trid : -1), (long long) ws_parent_out->pageid, (int) ws_parent_out->offset,
		    (long long) log_Writeset_prev_commit_lsa.pageid, (int) log_Writeset_prev_commit_lsa.offset);
      pthread_mutex_unlock (&log_Writeset_history.latch);
      return;
    }

  LSA_COPY (&ws_parent, &log_Writeset_history.history_start);

  if (tdes != NULL)
    {
      /* Both WRITE and REF keys probe the history: a REF makes this transaction wait behind the
       * parent row's commit exactly as a WRITE would wait behind the previous writer of the key. */
      for (const LOG_WRITESET_ENTRY & e : tdes->ws_hashes)
	{
	  auto it = log_Writeset_history.map.find (e.hash);

	  /* TEST ONLY (writeset perf): key-kind and map-hit counters */
	  if (e.kind == LOG_WRITESET_KIND_WRITE)
	    {
	      perf_wkeys++;
	    }
	  else
	    {
	      perf_rkeys++;
	    }

	  if (it != log_Writeset_history.map.end ())
	    {
	      perf_hits++;
	      if (LSA_GT (&it->second, &ws_parent))
		{
		  LSA_COPY (&ws_parent, &it->second);
		}
	    }
	}
    }

  /* dependency_seq = min (prev_commit_lsa, ws_parent); NULL acts as the smallest LSA */
  if (LSA_ISNULL (&log_Writeset_prev_commit_lsa) || LSA_LT (&ws_parent, &log_Writeset_prev_commit_lsa))
    {
      LSA_COPY (ws_parent_out, &ws_parent);
    }
  else
    {
      LSA_COPY (ws_parent_out, &log_Writeset_prev_commit_lsa);
    }

  /* TEST ONLY (writeset PoC 검증): 이 트랜잭션의 최종 의존 라벨을 남긴다. release 에서도 er_log_debug=yes
   * 면 보인다. dependency_seq 가 NULL 이면 독립(병렬 가능), 직전 커밋을 가리키면 사실상 직렬.
   * ws_keys 는 이 트랜잭션이 건드린 행 수(중복 포함), history_start 는 현재 floor. 검증 후 제거. */
  er_log_debug (ARG_FILE_LINE,
		"writeset probe trid=%d ws_keys=%zu dependency_seq=%lld|%d (ws_parent=%lld|%d prev_commit=%lld|%d "
		"history_start=%lld|%d) wkeys=%zu rkeys=%zu hits=%zu map_size=%zu probe_us=%llu collect_ns=%llu\n",
		(tdes != NULL ? tdes->trid : -1),
		(tdes != NULL ? tdes->ws_hashes.size () : (size_t) 0), (long long) ws_parent_out->pageid,
		(int) ws_parent_out->offset, (long long) ws_parent.pageid, (int) ws_parent.offset,
		(long long) log_Writeset_prev_commit_lsa.pageid, (int) log_Writeset_prev_commit_lsa.offset,
		(long long) log_Writeset_history.history_start.pageid, (int) log_Writeset_history.history_start.offset,
		perf_wkeys, perf_rkeys, perf_hits, log_Writeset_history.map.size (),
		(unsigned long long) ((log_writeset_clock_ns () - perf_t0) / 1000),
		(unsigned long long) (tdes != NULL ? tdes->ws_stat_collect_ns : 0));

  pthread_mutex_unlock (&log_Writeset_history.latch);
}

/*
 * log_writeset_commit_flush - publish this commit's keys into the global history
 *
 *   commit_lsa(in): the assigned commit LSA of this transaction
 *
 * Note: Called AFTER the commit LSA is assigned and BEFORE the transaction's row
 *       locks are released. Row X-locks serialize same-key transactions, so a
 *       later same-key transaction cannot probe until this flush has published the
 *       key. Runs under the dedicated history latch, never on the abort path.
 */
void
log_writeset_commit_flush (THREAD_ENTRY * thread_p, LOG_TDES * tdes, const LOG_LSA * commit_lsa)
{
  if (tdes == NULL || commit_lsa == NULL || LSA_ISNULL (commit_lsa))
    {
      return;
    }

  UINT64 perf_t0 = log_writeset_clock_ns ();	/* TEST ONLY (writeset perf): includes latch wait */
  size_t perf_published = 0;

  pthread_mutex_lock (&log_Writeset_history.latch);

  /* advance the commit-order baseline monotonically (flush order may differ from
   * commit order once we run outside prior_lsa_mutex) */
  if (LSA_ISNULL (&log_Writeset_prev_commit_lsa) || LSA_GT (commit_lsa, &log_Writeset_prev_commit_lsa))
    {
      LSA_COPY (&log_Writeset_prev_commit_lsa, commit_lsa);
    }

  if (!tdes->ws_overflow && !tdes->ws_hashes.empty ())
    {
      /* Only WRITE keys are published into the history. REF keys probed the history above (so the
       * transaction waits behind the parent's commit) but must not be published: publishing a
       * reference would make a later sibling that points at the same parent match this sibling and
       * serialize behind it for no reason. Count WRITE keys so both the capacity test and the
       * inserts see the real published amount. */
      size_t write_count = 0;
      for (const LOG_WRITESET_ENTRY & e : tdes->ws_hashes)
	{
	  if (e.kind == LOG_WRITESET_KIND_WRITE)
	    {
	      write_count++;
	    }
	}
      perf_published = write_count;

      if (write_count > 0)
	{
	  /* CAP 초과하면 히스토리를 통째로 비우고 보수적 floor 를 이 commit LSA 로 올린다
	   * (= MySQL m_writeset_history.clear() + m_writeset_history_start = seq). */
	  if (log_Writeset_history.map.size () + write_count > (size_t) LOG_WRITESET_HISTORY_CAP)
	    {
	      /* TEST ONLY (writeset PoC 검증): 히스토리가 가득 차서 통째로 비우는 경로. 부팅 초기화
	       * ("INIT (server boot)")와 구분되는 태그. 검증 후 제거. */
	      er_log_debug (ARG_FILE_LINE,
			    "writeset history CLEAR (full): prev_count=%zu + tx=%zu > cap=%d, new history_start=%lld|%d\n",
			    log_Writeset_history.map.size (), write_count, LOG_WRITESET_HISTORY_CAP,
			    (long long) commit_lsa->pageid, (int) commit_lsa->offset);
	      log_Writeset_history.map.clear ();
	      LSA_COPY (&log_Writeset_history.history_start, commit_lsa);
	    }

	  /* 이 트랜잭션의 WRITE 키들에 현재 commit LSA 를 기록(신규 삽입 또는 갱신). */
	  for (const LOG_WRITESET_ENTRY & e : tdes->ws_hashes)
	    {
	      if (e.kind == LOG_WRITESET_KIND_WRITE)
		{
		  log_Writeset_history.map[e.hash] = *commit_lsa;
		}
	    }
	}
    }
  else if (tdes->ws_overflow)
    {
      /* MySQL 한도초과(write-set limit reached) fallback 과 동일: overflow tx 는 writeset 을 못
       * 남기므로 히스토리를 통째 clear 하고 floor(history_start)를 이 커밋으로 올린다. 그래야 이후
       * 트랜잭션들이 이 overflow 커밋 이후에만 적용되어(= 이 커밋을 기다려) 같은 행 순서 붕괴를
       * 막는다. (probe 는 이미 commit-order 로 격하됨 = MySQL 의 commit_parent 유지.)
       * MySQL 은 m_writeset_history.clear() + m_writeset_history_start = seq 를 무조건 하지만,
       * 우리 flush 는 커밋 순서와 달라질 수 있어 floor 는 단조 상향만 한다. */
      log_Writeset_history.map.clear ();
      if (LSA_GT (commit_lsa, &log_Writeset_history.history_start))
	{
	  LSA_COPY (&log_Writeset_history.history_start, commit_lsa);
	}
      /* TEST ONLY (writeset PoC 검증): overflow 커밋이 전역 히스토리를 통째 비우고 floor 를 이 커밋으로
       * 올렸다 → 이후 트랜잭션들이 이 커밋 뒤로 직렬화된다. 검증 후 제거. */
      er_log_debug (ARG_FILE_LINE,
		    "writeset FALLBACK(overflow) flush trid=%d: cleared history, new history_start=%lld|%d\n",
		    tdes->trid, (long long) log_Writeset_history.history_start.pageid,
		    (int) log_Writeset_history.history_start.offset);
    }

  /* TEST ONLY (writeset perf): per-commit publish cost and history size */
  er_log_debug (ARG_FILE_LINE, "writeset flush trid=%d flush_us=%llu published=%zu map_size=%zu\n",
		tdes->trid, (unsigned long long) ((log_writeset_clock_ns () - perf_t0) / 1000), perf_published,
		log_Writeset_history.map.size ());

  pthread_mutex_unlock (&log_Writeset_history.latch);
}

#if !defined (NDEBUG)
/*
 * log_writeset_selfcheck_pack - pack a DB_VALUE exactly as the writeset hash input is built
 *
 * return: malloc'd buffer with alignment padding zeroed (caller frees), or NULL on failure
 */
static char *
log_writeset_selfcheck_pack (DB_VALUE * v, int *packed_len)
{
  char *buf;
  char *ptr;
  int buf_len;

  *packed_len = 0;

  buf_len = OR_VALUE_ALIGNED_SIZE (v);
  if (buf_len <= 0)
    {
      return NULL;
    }

  buf = (char *) malloc ((size_t) buf_len);
  if (buf == NULL)
    {
      return NULL;
    }

  memset (buf, 0, (size_t) buf_len);

  ptr = or_pack_mem_value (buf, v, packed_len);
  if (ptr == NULL)
    {
      free_and_init (buf);
      return NULL;
    }

  return buf;
}

/*
 * log_writeset_selfcheck_case - verify one parent/child domain pair
 *
 * Confirms that a child value cast into the parent primary-key domain packs to the same bytes as
 * the parent's own primary-key value. This is the invariant the foreign-key REF hash depends on:
 * the parent publishes a hash over its key packed in its own domain, and the child reproduces it
 * by casting its foreign-key value into that domain first. When the two domains differ only in
 * precision/scale/collation (a valid foreign key does not force those to match), the cast is what
 * makes the bytes line up.
 */
static void
log_writeset_selfcheck_case (const char *name, TP_DOMAIN * parent_dom, TP_DOMAIN * child_dom, DB_VALUE * source)
{
  DB_VALUE parent_val, child_val, ref_val;
  char *parent_buf = NULL;
  char *ref_buf = NULL;
  char *child_buf = NULL;
  int parent_len = 0;
  int ref_len = 0;
  int child_len = 0;
  bool ok = false;
  bool cast_was_needed = false;

  db_make_null (&parent_val);
  db_make_null (&child_val);
  db_make_null (&ref_val);

  if (parent_dom == NULL || child_dom == NULL)
    {
      er_log_debug (ARG_FILE_LINE, "writeset SELFCHECK[%s]: SKIP (domain unavailable)\n", name);
      return;
    }

  /* parent stores its primary key in the parent domain; the child stores the same logical value
   * in its (possibly wider) foreign-key domain */
  if (tp_value_cast (source, &parent_val, parent_dom, false) != DOMAIN_COMPATIBLE
      || tp_value_cast (source, &child_val, child_dom, false) != DOMAIN_COMPATIBLE)
    {
      er_log_debug (ARG_FILE_LINE, "writeset SELFCHECK[%s]: SKIP (source cast failed)\n", name);
      goto cleanup;
    }

  /* the collection path casts the child value into the parent domain before packing */
  if (tp_value_cast (&child_val, &ref_val, parent_dom, false) != DOMAIN_COMPATIBLE)
    {
      er_log_debug (ARG_FILE_LINE, "writeset SELFCHECK[%s]: SKIP (ref cast failed)\n", name);
      goto cleanup;
    }

  parent_buf = log_writeset_selfcheck_pack (&parent_val, &parent_len);
  ref_buf = log_writeset_selfcheck_pack (&ref_val, &ref_len);
  child_buf = log_writeset_selfcheck_pack (&child_val, &child_len);
  if (parent_buf == NULL || ref_buf == NULL || child_buf == NULL)
    {
      er_log_debug (ARG_FILE_LINE, "writeset SELFCHECK[%s]: SKIP (pack failed)\n", name);
      goto cleanup;
    }

  ok = (parent_len == ref_len && memcmp (parent_buf, ref_buf, (size_t) parent_len) == 0);

  /* whether the raw child bytes already differed from the parent (so the cast was load-bearing) */
  cast_was_needed = !(child_len == parent_len && memcmp (child_buf, parent_buf, (size_t) parent_len) == 0);

  er_log_debug (ARG_FILE_LINE, "writeset SELFCHECK[%s]: %s (parent_len=%d ref_len=%d cast_was_needed=%s)\n",
		name, (ok ? "PASS" : "FAIL"), parent_len, ref_len, (cast_was_needed ? "yes" : "no"));
  assert (ok);

cleanup:
  if (parent_buf != NULL)
    {
      free_and_init (parent_buf);
    }
  if (ref_buf != NULL)
    {
      free_and_init (ref_buf);
    }
  if (child_buf != NULL)
    {
      free_and_init (child_buf);
    }
  pr_clear_value (&parent_val);
  pr_clear_value (&child_val);
  pr_clear_value (&ref_val);
}

/*
 * log_writeset_selfcheck_packing - boot-time verification of FK reference packing equivalence
 *
 * Covers fixed built-in types (where parent and child share the domain and the cast is a no-op)
 * and the string/numeric cases where a child column may legally differ in precision/scale from the
 * parent primary key, so the cast into the parent domain is what makes the packed bytes match.
 */
static void
log_writeset_selfcheck_packing (void)
{
  TP_DOMAIN *d_int;
  TP_DOMAIN *d_bigint;
  TP_DOMAIN *d_date;
  TP_DOMAIN *d_char_def;
  TP_DOMAIN *d_char10;
  TP_DOMAIN *d_char20;
  TP_DOMAIN *d_num_10_2;
  TP_DOMAIN *d_num_15_4;
  int char_coll;
  DB_VALUE src;

  d_int = tp_domain_resolve_default (DB_TYPE_INTEGER);
  d_bigint = tp_domain_resolve_default (DB_TYPE_BIGINT);
  d_date = tp_domain_resolve_default (DB_TYPE_DATE);

  d_char_def = tp_domain_resolve_default (DB_TYPE_CHAR);
  char_coll = (d_char_def != NULL ? d_char_def->collation_id : 0);
  d_char10 = tp_domain_resolve (DB_TYPE_CHAR, NULL, 10, 0, NULL, char_coll);
  d_char20 = tp_domain_resolve (DB_TYPE_CHAR, NULL, 20, 0, NULL, char_coll);

  d_num_10_2 = tp_domain_resolve (DB_TYPE_NUMERIC, NULL, 10, 2, NULL, 0);
  d_num_15_4 = tp_domain_resolve (DB_TYPE_NUMERIC, NULL, 15, 4, NULL, 0);

  /* INT / BIGINT / DATE: parent and child share the built-in domain, cast is a no-op */
  db_make_int (&src, 42);
  log_writeset_selfcheck_case ("INT", d_int, d_int, &src);
  pr_clear_value (&src);

  db_make_bigint (&src, (DB_BIGINT) 1234567890123LL);
  log_writeset_selfcheck_case ("BIGINT", d_bigint, d_bigint, &src);
  pr_clear_value (&src);

  db_make_date (&src, 12, 25, 2024);
  log_writeset_selfcheck_case ("DATE", d_date, d_date, &src);
  pr_clear_value (&src);

  /* CHAR: identical precision, then a wider child that must be refit to the parent width */
  db_make_string (&src, "abc");
  log_writeset_selfcheck_case ("CHAR_SAME_PREC", d_char10, d_char10, &src);
  log_writeset_selfcheck_case ("CHAR_DIFF_PREC", d_char10, d_char20, &src);
  pr_clear_value (&src);

  /* NUMERIC: child carries a different precision/scale than the parent primary key */
  db_make_string (&src, "123.45");
  log_writeset_selfcheck_case ("NUMERIC", d_num_10_2, d_num_15_4, &src);
  pr_clear_value (&src);
}
#endif /* !NDEBUG */

#endif /* SERVER_MODE || SA_MODE */
