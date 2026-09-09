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
 * test_log_writeset_stress.cpp - contention and large-data measurements for the
 *                                writeset global commit history.
 *
 * These target the two review axes the integration bench could not isolate:
 *   1. large data:  one big transaction (near TX_LIMIT) and the map at/over
 *                   HISTORY_CAP (the CLEAR path).
 *   2. concurrency: many threads hammering probe/flush on the single global
 *                   latch, plus the reverse-order defence firing under load.
 *
 * The numbers are an UPPER BOUND on contention: threads do nothing but pound
 * the latch, whereas real transactions interleave other work. Use these to see
 * where scaling breaks, not for the realistic percentage (that is the HA bench).
 *
 * Design note: worker threads call only probe/flush. The writeset is collected
 * up front on the main thread, so no worker touches the REF cast path
 * (db_private_alloc via the TLS THREAD_ENTRY), which is only initialized for the
 * main thread. probe/flush pass thread_p through without dereferencing it.
 *
 * Tag [stress]; heavy (seconds to tens of seconds, hundreds of MB). Run
 * explicitly on a release build: test_log_writeset "[stress]".
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>

#include "test_log_writeset_common.hpp"

using namespace wstest;

namespace
{
  using clock_type = std::chrono::steady_clock;

  double
  ms_since (clock_type::time_point t0)
  {
    auto d = clock_type::now () - t0;
    return std::chrono::duration<double, std::milli> (d).count ();
  }

  void
  report (const std::string &label, double value, const char *unit)
  {
    std::printf ("[stress] %-52s %12.3f %s\n", label.c_str (), value, unit);
    std::fflush (stdout);
  }

  /* run `fn` on `nthreads` threads that all start together (spin on a gate), each
   * looping `iters` times; return the wall-clock milliseconds of the parallel
   * region. fn takes the thread index. */
  template <typename Fn>
  double
  run_parallel (int nthreads, int iters, Fn fn)
  {
    std::atomic<int> ready {0};
    std::atomic<bool> go {false};
    std::vector<std::thread> workers;
    clock_type::time_point start;

    for (int t = 0; t < nthreads; t++)
      {
	workers.emplace_back ([&, t] ()
	  {
	    ready.fetch_add (1);
	    while (!go.load ())
	      {
		std::this_thread::yield ();
	      }
	    for (int i = 0; i < iters; i++)
	      {
		fn (t);
	      }
	  });
      }

    while (ready.load () < nthreads)
      {
	std::this_thread::yield ();
      }
    start = clock_type::now ();
    go.store (true);
    for (auto &w : workers)
      {
	w.join ();
      }
    return ms_since (start);
  }
}

/* A2: one large transaction near the per-tx key limit. Measures the collect
 * total and, crucially, the probe/flush latch-hold time for a single commit
 * that iterates the whole key set under the lock. */
TEST_CASE ("A2: large single transaction collect/probe/flush cost", "[stress]")
{
  ws_history_guard history;
  OID cls = oid_of (1, 100, 1);
  const int nkeys = LOG_WRITESET_TX_LIMIT - 1;	/* just under overflow */
  DB_VALUE v;

  log_tdes *tx = make_tdes (1);
  for (int i = 0; i < nkeys; i++)
    {
      db_make_int (&v, i);
      log_writeset_add_dbvalue (NULL, tx, &cls, &v);
    }
  REQUIRE (!tx->ws_overflow);
  REQUIRE (tx->ws_hashes.size () == (size_t) nkeys);

  double collect_ms = tx->ws_stat_collect_ns / 1e6;

  LOG_LSA dep;
  auto t0 = clock_type::now ();
  log_writeset_commit_probe (NULL, tx, &dep);
  double probe_ms = ms_since (t0);

  LOG_LSA commit = lsa_of (10, 0);
  t0 = clock_type::now ();
  log_writeset_commit_flush (NULL, tx, &commit);
  double flush_ms = ms_since (t0);

  report ("A2 keys", (double) nkeys, "keys");
  report ("A2 collect total", collect_ms, "ms");
  report ("A2 collect per-key", collect_ms * 1e6 / nkeys, "ns");
  report ("A2 probe (latch hold)", probe_ms, "ms");
  report ("A2 flush (latch hold)", flush_ms, "ms");

  REQUIRE (log_Writeset_history.map.size () == (size_t) nkeys);
  delete tx;
}

/* A4: fill the map past HISTORY_CAP and verify the CLEAR path. Heavy: crosses
 * LOG_WRITESET_HISTORY_CAP keys (rounds derived from the compiled CAP, so this
 * follows a 2M or a 10M build without edits; ~50-80 bytes/entry of resident
 * memory). Confirms the floor jump and that an evicted key is no longer treated
 * as independent (reverse-order safety survives a CLEAR). */
TEST_CASE ("A4: map CAP crossing clears history and raises the floor", "[stress]")
{
  ws_history_guard history;
  OID cls = oid_of (1, 100, 1);
  OID parent_cls = oid_of (1, 200, 1);
  const int chunk = 240000;	/* < TX_LIMIT, so no overflow */
  DB_VALUE v;
  int next_key = 0;

  /* an early parent key that a child references, so a read slot exists on it
   * before the CLEAR wipes the map */
  log_tdes *parent = make_tdes (1);
  add_write_int (parent, &parent_cls, -1);
  LOG_LSA parent_lsa = lsa_of (5, 0);
  log_writeset_commit_flush (NULL, parent, &parent_lsa);
  log_tdes *child = make_tdes (2);
  add_ref_int (child, &parent_cls, -1);
  LOG_LSA child_lsa = lsa_of (6, 0);
  log_writeset_commit_flush (NULL, child, &child_lsa);

  size_t before = log_Writeset_history.map.size ();
  bool cleared = false;
  LOG_LSA clear_lsa = lsa_of (0, 0);
  int commit_page = 100;

  /* keep flushing 240k-key transactions until a flush trips the CAP clear
   * (detected by the map shrinking instead of growing). Rounds are derived from
   * the compiled CAP so this works at 2M or 10M alike. */
  const int max_rounds = LOG_WRITESET_HISTORY_CAP / chunk + 3;
  for (int round = 0; round < max_rounds && !cleared; round++)
    {
      log_tdes *tx = make_tdes (100 + round);
      for (int i = 0; i < chunk; i++)
	{
	  db_make_int (&v, next_key++);
	  log_writeset_add_dbvalue (NULL, tx, &cls, &v);
	}
      LOG_LSA commit = lsa_of (commit_page++, 0);
      log_writeset_commit_flush (NULL, tx, &commit);
      size_t after = log_Writeset_history.map.size ();

      if (after < before)
	{
	  cleared = true;
	  clear_lsa = commit;
	  /* after a CAP clear the map holds only this last transaction's keys */
	  REQUIRE (after == (size_t) chunk);
	  REQUIRE (LSA_EQ (&log_Writeset_history.history_start, &commit));
	}
      before = after;
      delete tx;
    }

  REQUIRE (cleared);
  report ("A4 clear fired at LSA page", (double) clear_lsa.pageid, "");

  /* reverse-order safety after CLEAR: the parent key's read slot was wiped, so a
   * later parent write no longer sees the child's stamp. It must not come back
   * as independent (NULL) - the floor keeps it serialized behind the clear. */
  log_tdes *parent_delete = make_tdes (999);
  add_write_int (parent_delete, &parent_cls, -1);
  LOG_LSA dep = probe (parent_delete);
  REQUIRE (!LSA_ISNULL (&dep));
  delete parent_delete;

  delete parent;
  delete child;
}

/* helpers shared by the concurrency measurements: build a preloaded map and a
 * per-thread worker transaction whose keys all hit that map. */
namespace
{
  /* preload `total` distinct WRITE keys [0, total) into the map */
  void
  preload_write_keys (OID *cls, int total)
  {
    DB_VALUE v;
    /* flush in chunks below TX_LIMIT */
    int done = 0;
    int page = 100;
    while (done < total)
      {
	int n = std::min (total - done, LOG_WRITESET_TX_LIMIT - 1);
	log_tdes *filler = make_tdes (10000 + page);
	for (int i = 0; i < n; i++)
	  {
	    db_make_int (&v, done + i);
	    log_writeset_add_dbvalue (NULL, filler, cls, &v);
	  }
	LOG_LSA lsa = lsa_of (page++, 0);
	log_writeset_commit_flush (NULL, filler, &lsa);
	delete filler;
	done += n;
      }
  }

  /* a worker tx of `keys` WRITE keys starting at `base` (disjoint ranges give
   * disjoint keys; a shared base gives a hot key set) */
  log_tdes *
  make_worker (int trid, OID *cls, int base, int keys)
  {
    DB_VALUE v;
    log_tdes *tx = make_tdes (trid);
    for (int i = 0; i < keys; i++)
      {
	db_make_int (&v, base + i);
	log_writeset_add_dbvalue (NULL, tx, cls, &v);
      }
    return tx;
  }
}

/* A5: probe per-key cost swept across map sizes from tiny up to near-CAP, all on
 * the same harness so the points are directly comparable. The curve SHAPE is the
 * point: flat-then-step suggests a cache/TLB boundary, monotonic growth would
 * contradict the O(1) claim. Each size runs on a freshly initialized map so the
 * load factor is natural. Heavy: the near-CAP points preload millions of keys. */
TEST_CASE ("A5: probe cost across map sizes up to near-CAP", "[stress]")
{
  const long sizes[] = { 10000, 100000, 500000, 1000000, 2000000, 3000000,
    5000000, 7000000, (long) LOG_WRITESET_HISTORY_CAP * 9 / 10
  };

  for (long loaded : sizes)
    {
      if (loaded > (long) LOG_WRITESET_HISTORY_CAP * 95 / 100)
	{
	  continue;		/* stay clear of the CLEAR threshold */
	}

      ws_history_guard history;
      OID cls = oid_of (1, 100, 1);
      DB_VALUE v;

      preload_write_keys (&cls, (int) loaded);
      REQUIRE (log_Writeset_history.map.size () == (size_t) loaded);

      /* 1000 keys that all hit the loaded map */
      log_tdes *worker = make_tdes (902);
      for (int i = 0; i < 1000; i++)
	{
	  db_make_int (&v, i);
	  log_writeset_add_dbvalue (NULL, worker, &cls, &v);
	}

      const int reps = 100;
      LOG_LSA dep;
      auto t0 = clock_type::now ();
      for (int r = 0; r < reps; r++)
	{
	  log_writeset_commit_probe (NULL, worker, &dep);
	}
      double total_ms = ms_since (t0);

      report ("A5 map=" + std::to_string (loaded) + " probe per-key", total_ms * 1e6 / (reps * 1000), "ns");

      delete worker;
    }
}

/* B2: probe+flush throughput as the thread count rises. Single global latch, so
 * expect throughput to flatten (or degrade) rather than scale with cores. */
TEST_CASE ("B2: latch throughput vs thread count", "[stress]")
{
  const int thread_counts[] = { 1, 2, 4, 8, 16, 32, 64, 80, 128, 256, 512 };
  const int keys_per_tx = 1000;
  const int iters = 200;

  for (int nthreads : thread_counts)
    {
      ws_history_guard history;
      OID cls = oid_of (1, 100, 1);

      /* disjoint key ranges per thread; preload them all so every probe hits */
      preload_write_keys (&cls, nthreads * keys_per_tx);
      std::vector<log_tdes *> tx (nthreads);
      for (int t = 0; t < nthreads; t++)
	{
	  tx[t] = make_worker (t + 1, &cls, t * keys_per_tx, keys_per_tx);
	}

      LOG_LSA commit = lsa_of (500, 0);
      double wall = run_parallel (nthreads, iters, [&] (int t)
	{
	  LOG_LSA dep;
	  log_writeset_commit_probe (NULL, tx[t], &dep);
	  log_writeset_commit_flush (NULL, tx[t], &commit);
	});

      long ops = (long) nthreads * iters * 2;	/* probe + flush */
      report ("B2 threads=" + std::to_string (nthreads) + " wall", wall, "ms");
      report ("B2 threads=" + std::to_string (nthreads) + " throughput", ops / wall * 1000.0, "ops/s");

      for (auto *p : tx)
	{
	  delete p;
	}
    }
}

/* B8: thread sweep with the commit size fixed at the CAP-maximal PK insert for
 * 512 threads (keys_per_tx = 90% of HISTORY_CAP / 512 = 17,577 keys, one key per
 * row). Every sweep point commits the same amount of work, so the curve isolates
 * how concurrency alone stretches per-commit latency under the single latch. */
TEST_CASE ("B8: thread sweep x CAP-max PK commits", "[stress]")
{
  const int thread_counts[] = { 1, 2, 4, 8, 16, 32, 64, 80, 128, 256, 512 };
  const int keys_per_tx = (int) ((long) LOG_WRITESET_HISTORY_CAP / 512 * 9 / 10);
  const int iters = 10;

  for (int nthreads : thread_counts)
    {
      ws_history_guard history;
      OID cls = oid_of (1, 100, 1);

      preload_write_keys (&cls, nthreads * keys_per_tx);
      std::vector<log_tdes *> tx (nthreads);
      for (int t = 0; t < nthreads; t++)
	{
	  tx[t] = make_worker (t + 1, &cls, t * keys_per_tx, keys_per_tx);
	}

      LOG_LSA commit = lsa_of (500, 0);
      double wall = run_parallel (nthreads, iters, [&] (int t)
	{
	  LOG_LSA dep;
	  log_writeset_commit_probe (NULL, tx[t], &dep);
	  log_writeset_commit_flush (NULL, tx[t], &commit);
	});

      long ops = (long) nthreads * iters * 2;
      double rows_per_sec = (double) nthreads * iters * keys_per_tx / (wall / 1000.0);
      report ("B8 threads=" + std::to_string (nthreads) + " wall", wall, "ms");
      report ("B8 threads=" + std::to_string (nthreads) + " per-op (system)", wall / ops * 1000.0, "us");
      report ("B8 threads=" + std::to_string (nthreads) + " per-commit latency", wall / iters, "ms");
      report ("B8 threads=" + std::to_string (nthreads) + " row capacity", rows_per_sec, "rows/s");

      for (auto *p2 : tx)
	{
	  delete p2;
	}
    }
}

/* B3: keys-per-commit x threads. The latch is held for the whole key loop, so a
 * large commit under high concurrency should cost far more than a small one. */
TEST_CASE ("B3: latch hold scales with keys per commit", "[stress]")
{
  const int nthreads = 16;
  /* disjoint preload = nthreads x ksize keys, so the sweep tops out under
   * HISTORY_CAP: 16 x 500,000 = 8M < 10M */
  const int commit_sizes[] = { 10, 1000, 10000, 100000, 500000 };
  const int iters = 50;

  for (int ksize : commit_sizes)
    {
      ws_history_guard history;
      OID cls = oid_of (1, 100, 1);

      preload_write_keys (&cls, nthreads * ksize);
      std::vector<log_tdes *> tx (nthreads);
      for (int t = 0; t < nthreads; t++)
	{
	  tx[t] = make_worker (t + 1, &cls, t * ksize, ksize);
	}

      LOG_LSA commit = lsa_of (500, 0);
      double wall = run_parallel (nthreads, iters, [&] (int t)
	{
	  LOG_LSA dep;
	  log_writeset_commit_probe (NULL, tx[t], &dep);
	  log_writeset_commit_flush (NULL, tx[t], &commit);
	});

      long ops = (long) nthreads * iters * 2;
      report ("B3 keys/commit=" + std::to_string (ksize) + " wall", wall, "ms");
      report ("B3 keys/commit=" + std::to_string (ksize) + " per-op", wall / ops * 1000.0, "us");

      for (auto *p : tx)
	{
	  delete p;
	}
    }
}

/* B3s (supplementary): the B3 keys-per-commit sweep with a single thread. The
 * map is preloaded to the same size as the B3 point (16 x ksize), so the only
 * difference from B3 is the absence of contention. If the per-key jump B3 shows
 * from 100k keys up persists here, it is a property of one call touching that
 * much data, not of the 16-thread load. */
TEST_CASE ("B3s: keys per commit, single thread (supplementary)", "[stress]")
{
  const int commit_sizes[] = { 10, 1000, 10000, 100000, 500000 };
  const int iters = 50;

  for (int ksize : commit_sizes)
    {
      ws_history_guard history;
      OID cls = oid_of (1, 100, 1);

      preload_write_keys (&cls, 16 * ksize);	/* same map size as the B3 point */
      log_tdes *tx = make_worker (1, &cls, 0, ksize);

      LOG_LSA commit = lsa_of (500, 0);
      LOG_LSA dep;
      auto t0 = clock_type::now ();
      for (int i = 0; i < iters; i++)
	{
	  log_writeset_commit_probe (NULL, tx, &dep);
	  log_writeset_commit_flush (NULL, tx, &commit);
	}
      double wall = ms_since (t0);

      long ops = (long) iters * 2;
      report ("B3s keys/commit=" + std::to_string (ksize) + " per-op", wall / ops * 1000.0, "us");
      report ("B3s keys/commit=" + std::to_string (ksize) + " per-key", wall / ops * 1e6 / ksize, "ns");

      delete tx;
    }
}

/* B4: hot key vs disjoint keys at a fixed thread count. The contention point is
 * the single global latch, taken regardless of key overlap, so throughput
 * should be similar - key collision does not add latch contention. */
TEST_CASE ("B4: hot key vs disjoint keys", "[stress]")
{
  const int nthreads = 16;
  const int keys_per_tx = 1000;
  const int iters = 200;

  double hot_ms = 0.0;
  double disjoint_ms = 0.0;

  {
    ws_history_guard history;
    OID cls = oid_of (1, 100, 1);
    preload_write_keys (&cls, keys_per_tx);	/* one shared key set */
    std::vector<log_tdes *> tx (nthreads);
    for (int t = 0; t < nthreads; t++)
      {
	tx[t] = make_worker (t + 1, &cls, 0, keys_per_tx);	/* same base = hot */
      }
    LOG_LSA commit = lsa_of (500, 0);
    hot_ms = run_parallel (nthreads, iters, [&] (int t)
      {
	LOG_LSA dep;
	log_writeset_commit_probe (NULL, tx[t], &dep);
	log_writeset_commit_flush (NULL, tx[t], &commit);
      });
    for (auto *p : tx)
      {
	delete p;
      }
  }

  {
    ws_history_guard history;
    OID cls = oid_of (1, 100, 1);
    preload_write_keys (&cls, nthreads * keys_per_tx);
    std::vector<log_tdes *> tx (nthreads);
    for (int t = 0; t < nthreads; t++)
      {
	tx[t] = make_worker (t + 1, &cls, t * keys_per_tx, keys_per_tx);	/* disjoint */
      }
    LOG_LSA commit = lsa_of (500, 0);
    disjoint_ms = run_parallel (nthreads, iters, [&] (int t)
      {
	LOG_LSA dep;
	log_writeset_commit_probe (NULL, tx[t], &dep);
	log_writeset_commit_flush (NULL, tx[t], &commit);
      });
    for (auto *p : tx)
      {
	delete p;
      }
  }

  report ("B4 hot-key wall", hot_ms, "ms");
  report ("B4 disjoint wall", disjoint_ms, "ms");
}

/* B5: worst corner - many threads x large commits against a large loaded map. */
TEST_CASE ("B5: worst corner (many threads x large commits x large map)", "[stress]")
{
  const int nthreads = 32;
  const int keys_per_tx = 50000;
  const int iters = 5;

  ws_history_guard history;
  OID cls = oid_of (1, 100, 1);

  preload_write_keys (&cls, nthreads * keys_per_tx);	/* ~1.6M keys */
  std::vector<log_tdes *> tx (nthreads);
  for (int t = 0; t < nthreads; t++)
    {
      tx[t] = make_worker (t + 1, &cls, t * keys_per_tx, keys_per_tx);
    }

  LOG_LSA commit = lsa_of (900, 0);
  double wall = run_parallel (nthreads, iters, [&] (int t)
    {
      LOG_LSA dep;
      log_writeset_commit_probe (NULL, tx[t], &dep);
      log_writeset_commit_flush (NULL, tx[t], &commit);
    });

  long ops = (long) nthreads * iters * 2;
  report ("B5 wall", wall, "ms");
  report ("B5 per-op (probe+flush of 50k keys)", wall / ops, "ms");

  for (auto *p : tx)
    {
      delete p;
    }
}

/* B6: reverse-order defence firing under load. A parent write that hits a key
 * whose read slot is populated must consult read_seq (the defence path). Compare
 * throughput against the same write hitting keys with an empty read slot
 * (single-slot equivalent). The delta is the pure cost of the read-slot check;
 * expected negligible (one extra LSA compare per key). */
TEST_CASE ("B6: reverse-order defence firing vs baseline", "[stress]")
{
  const int nthreads = 16;
  const int keys_per_tx = 1000;
  const int iters = 200;

  double defence_ms = 0.0;
  double baseline_ms = 0.0;

  /* baseline: WRITE keys hit entries that have only a write slot */
  {
    ws_history_guard history;
    OID cls = oid_of (1, 100, 1);
    preload_write_keys (&cls, nthreads * keys_per_tx);
    std::vector<log_tdes *> tx (nthreads);
    for (int t = 0; t < nthreads; t++)
      {
	tx[t] = make_worker (t + 1, &cls, t * keys_per_tx, keys_per_tx);
      }
    LOG_LSA commit = lsa_of (500, 0);
    baseline_ms = run_parallel (nthreads, iters, [&] (int t)
      {
	LOG_LSA dep;
	log_writeset_commit_probe (NULL, tx[t], &dep);
	log_writeset_commit_flush (NULL, tx[t], &commit);
      });
    for (auto *p : tx)
      {
	delete p;
      }
  }

  /* defence: the same keys, but each also carries a read-slot stamp from a
   * child reference, so the WRITE probe takes the max(write_seq, read_seq) path */
  {
    ws_history_guard history;
    OID parent_cls = oid_of (1, 200, 1);
    int total = nthreads * keys_per_tx;

    preload_write_keys (&parent_cls, total);	/* write slots */
    /* stamp read slots via child references to the same keys */
    {
      DB_VALUE v;
      int done = 0;
      int page = 300;
      TP_DOMAIN *int_domain = tp_domain_resolve_default (DB_TYPE_INTEGER);
      REQUIRE (int_domain != NULL);
      while (done < total)
	{
	  int n = std::min (total - done, LOG_WRITESET_TX_LIMIT - 1);
	  log_tdes *ref = make_tdes (20000 + page);
	  for (int i = 0; i < n; i++)
	    {
	      db_make_int (&v, done + i);
	      log_writeset_add_ref_dbvalue (NULL, ref, &parent_cls, &v, int_domain);
	    }
	  LOG_LSA lsa = lsa_of (page++, 0);
	  log_writeset_commit_flush (NULL, ref, &lsa);
	  delete ref;
	  done += n;
	}
    }

    std::vector<log_tdes *> tx (nthreads);
    for (int t = 0; t < nthreads; t++)
      {
	tx[t] = make_worker (t + 1, &parent_cls, t * keys_per_tx, keys_per_tx);	/* WRITE hits read-stamped keys */
      }
    LOG_LSA commit = lsa_of (500, 0);
    defence_ms = run_parallel (nthreads, iters, [&] (int t)
      {
	LOG_LSA dep;
	log_writeset_commit_probe (NULL, tx[t], &dep);
	log_writeset_commit_flush (NULL, tx[t], &commit);
      });
    for (auto *p : tx)
      {
	delete p;
      }
  }

  report ("B6 baseline (write slot only)", baseline_ms, "ms");
  report ("B6 defence (read slot consulted)", defence_ms, "ms");
}

/* helper for the FK-shaped cases below: a worker transaction with the key
 * pattern of an INSERT into a child table with a foreign key - every row adds
 * one WRITE key (the child PK) and one REF key (the parent PK it points at).
 * Built on the main thread because REF collection casts the value through the
 * main thread's runtime; workers only call probe/flush. */
namespace
{
  /* rows rows; row i = WRITE (child_base + i) on child_cls
   *                  + REF (parent_base + i % parent_count) on parent_cls */
  log_tdes *
  make_fk_worker (int trid, OID *child_cls, OID *parent_cls, int child_base, int rows,
		  int parent_base, int parent_count)
  {
    DB_VALUE v;
    TP_DOMAIN *int_domain = tp_domain_resolve_default (DB_TYPE_INTEGER);
    log_tdes *tx = make_tdes (trid);

    for (int i = 0; i < rows; i++)
      {
	db_make_int (&v, child_base + i);
	log_writeset_add_dbvalue (NULL, tx, child_cls, &v);
	db_make_int (&v, parent_base + i % parent_count);
	log_writeset_add_ref_dbvalue (NULL, tx, parent_cls, &v, int_domain);
      }
    return tx;
  }
}

/* F1: the A2 shape with FK rows. The per-tx key budget (TX_LIMIT) counts keys,
 * and an FK row spends two of them, so the largest FK transaction holds half
 * the rows of the largest PK transaction: rows = TX_LIMIT/2 - 1, keys just
 * under the limit. Every parent is distinct, so flush publishes one entry per
 * key (children get the write slot, parents get the read slot). */
TEST_CASE ("F1: large single FK transaction collect/probe/flush cost", "[stress]")
{
  ws_history_guard history;
  OID child_cls = oid_of (1, 100, 1);
  OID parent_cls = oid_of (1, 200, 1);
  const int rows = LOG_WRITESET_TX_LIMIT / 2 - 1;

  log_tdes *tx = make_fk_worker (1, &child_cls, &parent_cls, 0, rows, 0, rows);
  REQUIRE (!tx->ws_overflow);
  REQUIRE (tx->ws_hashes.size () == (size_t) rows * 2);

  double collect_ms = tx->ws_stat_collect_ns / 1e6;

  LOG_LSA dep;
  auto t0 = clock_type::now ();
  log_writeset_commit_probe (NULL, tx, &dep);
  double probe_ms = ms_since (t0);

  LOG_LSA commit = lsa_of (10, 0);
  t0 = clock_type::now ();
  log_writeset_commit_flush (NULL, tx, &commit);
  double flush_ms = ms_since (t0);

  report ("F1 rows", (double) rows, "rows");
  report ("F1 keys (2 per row)", (double) rows * 2, "keys");
  report ("F1 collect total", collect_ms, "ms");
  report ("F1 collect per-row", collect_ms * 1e6 / rows, "ns");
  report ("F1 probe (latch hold)", probe_ms, "ms");
  report ("F1 flush (latch hold)", flush_ms, "ms");

  REQUIRE (log_Writeset_history.map.size () == (size_t) rows * 2);
  delete tx;
}

/* F2: the B8 thread sweep with FK rows, CLEAR excluded. The row count per
 * commit stays at the B8 value (17,577 rows), so the same number of inserts is
 * committed and the only change is the key pattern: each row adds a child WRITE
 * key (disjoint per thread) plus a REF key into one parent pool shared by all
 * threads. Distinct keys published = threads x 17,577 children + 17,577
 * parents; at 512 threads that is about 9.02M, still under HISTORY_CAP, so the
 * CLEAR path never fires and the comparison against B8 isolates what the extra
 * REF key per row costs. */
TEST_CASE ("F2: FK thread sweep, same rows as B8, no CLEAR", "[stress]")
{
  const int thread_counts[] = { 1, 2, 4, 8, 16, 32, 64, 80, 128, 256, 512 };
  const int rows_per_tx = (int) ((long) LOG_WRITESET_HISTORY_CAP / 512 * 9 / 10);
  const int iters = 10;

  for (int nthreads : thread_counts)
    {
      ws_history_guard history;
      OID child_cls = oid_of (1, 100, 1);
      OID parent_cls = oid_of (1, 200, 1);

      preload_write_keys (&child_cls, nthreads * rows_per_tx);
      preload_write_keys (&parent_cls, rows_per_tx);	/* shared parent pool */
      std::vector<log_tdes *> tx (nthreads);
      for (int t = 0; t < nthreads; t++)
	{
	  tx[t] = make_fk_worker (t + 1, &child_cls, &parent_cls, t * rows_per_tx, rows_per_tx, 0, rows_per_tx);
	}

      LOG_LSA commit = lsa_of (500, 0);
      double wall = run_parallel (nthreads, iters, [&] (int t)
	{
	  LOG_LSA dep;
	  log_writeset_commit_probe (NULL, tx[t], &dep);
	  log_writeset_commit_flush (NULL, tx[t], &commit);
	});

      double rows_per_sec = (double) nthreads * iters * rows_per_tx / (wall / 1000.0);
      report ("F2 threads=" + std::to_string (nthreads) + " wall", wall, "ms");
      report ("F2 threads=" + std::to_string (nthreads) + " per-commit latency", wall / iters, "ms");
      report ("F2 threads=" + std::to_string (nthreads) + " row capacity", rows_per_sec, "rows/s");

      for (auto *p2 : tx)
	{
	  delete p2;
	}
    }
}

/* F4 (supplementary to F2): the FK commit shape swept by rows per commit on a
 * single thread. F2's 1-thread point (17,577 rows = 35,154 keys) costs 3.9x the
 * PK equivalent, more than the 2x the doubled key count explains. The PK-side
 * single-thread sweep (B3s) showed per-key cost inflating once one call walks
 * enough data (19ns at 10k keys -> 130ns at 100k keys), but 35k keys sits
 * between those points. This sweep locates where the FK shape crosses that
 * boundary; the 17,577-row point reproduces F2's commit exactly. */
TEST_CASE ("F4: FK rows per commit, single thread (F2 supplementary)", "[stress]")
{
  const int row_counts[] = { 10, 1000, 10000, 17577, 50000, 100000, 250000 };
  const int iters = 50;

  for (int rows : row_counts)
    {
      ws_history_guard history;
      OID child_cls = oid_of (1, 100, 1);
      OID parent_cls = oid_of (1, 200, 1);

      /* same shape as F2 at one thread: children + shared parent pool, all hit */
      preload_write_keys (&child_cls, rows);
      preload_write_keys (&parent_cls, rows);
      log_tdes *tx = make_fk_worker (1, &child_cls, &parent_cls, 0, rows, 0, rows);

      LOG_LSA commit = lsa_of (500, 0);
      LOG_LSA dep;
      auto t0 = clock_type::now ();
      for (int i = 0; i < iters; i++)
	{
	  log_writeset_commit_probe (NULL, tx, &dep);
	  log_writeset_commit_flush (NULL, tx, &commit);
	}
      double wall = ms_since (t0);

      long ops = (long) iters * 2;
      report ("F4 rows=" + std::to_string (rows) + " per-op", wall / ops * 1000.0, "us");
      report ("F4 rows=" + std::to_string (rows) + " per-key", wall / ops * 1e6 / ((long) rows * 2), "ns");

      delete tx;
    }
}

/* F5: few transactions x mass FK inserts - the FK counterpart of the extended
 * B3. 16 concurrent transactions each commit a large FK row set (up to 250k
 * rows = 500k keys walked per call). Children and parents are both disjoint per
 * thread, so the preloaded map is 16x the walked keys - the same walked:stored
 * ratio as B3, keeping the two sweeps directly comparable. CAP bound:
 * 16 x 250,000 rows x 2 keys = 8M < 10M. */
TEST_CASE ("F5: few transactions x mass FK inserts (16 threads, rows sweep)", "[stress]")
{
  const int nthreads = 16;
  const int row_counts[] = { 500, 5000, 50000, 250000 };	/* walked keys: 1k, 10k, 100k, 500k */
  const int iters = 50;

  for (int rows : row_counts)
    {
      ws_history_guard history;
      OID child_cls = oid_of (1, 100, 1);
      OID parent_cls = oid_of (1, 200, 1);

      preload_write_keys (&child_cls, nthreads * rows);
      preload_write_keys (&parent_cls, nthreads * rows);	/* per-thread disjoint parents */
      std::vector<log_tdes *> tx (nthreads);
      for (int t = 0; t < nthreads; t++)
	{
	  tx[t] = make_fk_worker (t + 1, &child_cls, &parent_cls, t * rows, rows, t * rows, rows);
	}

      LOG_LSA commit = lsa_of (500, 0);
      double wall = run_parallel (nthreads, iters, [&] (int t)
	{
	  LOG_LSA dep;
	  log_writeset_commit_probe (NULL, tx[t], &dep);
	  log_writeset_commit_flush (NULL, tx[t], &commit);
	});

      long ops = (long) nthreads * iters * 2;
      report ("F5 rows=" + std::to_string (rows) + " per-op", wall / ops * 1000.0, "us");
      report ("F5 rows=" + std::to_string (rows) + " per-key", wall / ops * 1e6 / ((long) rows * 2), "ns");

      for (auto *p2 : tx)
	{
	  delete p2;
	}
    }
}

/* F3: FK at 512 threads with the distinct-key population over HISTORY_CAP,
 * CLEAR included. Same rows per commit as F2, but every thread references its
 * own parents, so the population is 512 x 17,577 x 2 = 18M distinct keys
 * against a 10M CAP. Nothing is preloaded: flushes grow the map toward the CAP
 * and the crossing commit drops the whole map (map.clear() under the latch),
 * after which flushes insert fresh entries instead of updating existing ones.
 * By publish arithmetic (each commit publishes 35,154 keys, a crossing happens
 * near every 10M published) the run of 5,120 commits clears roughly 18 times.
 * This is the exposure the doubled key count of FK rows creates: the same
 * insert workload walks the map past the CAP where the PK shape (B8) stayed
 * under it. */
TEST_CASE ("F3: FK 512 threads over CAP, CLEAR included", "[stress]")
{
  const int nthreads = 512;
  const int rows_per_tx = (int) ((long) LOG_WRITESET_HISTORY_CAP / 512 * 9 / 10);
  const int iters = 10;

  ws_history_guard history;
  OID child_cls = oid_of (1, 100, 1);
  OID parent_cls = oid_of (1, 200, 1);

  std::vector<log_tdes *> tx (nthreads);
  for (int t = 0; t < nthreads; t++)
    {
      tx[t] = make_fk_worker (t + 1, &child_cls, &parent_cls, t * rows_per_tx, rows_per_tx,
			      t * rows_per_tx, rows_per_tx);
    }

  LOG_LSA commit = lsa_of (500, 0);
  double wall = run_parallel (nthreads, iters, [&] (int t)
    {
      LOG_LSA dep;
      log_writeset_commit_probe (NULL, tx[t], &dep);
      log_writeset_commit_flush (NULL, tx[t], &commit);
    });

  double rows_per_sec = (double) nthreads * iters * rows_per_tx / (wall / 1000.0);
  report ("F3 rows/commit", (double) rows_per_tx, "rows");
  report ("F3 wall", wall, "ms");
  report ("F3 per-commit latency (thread view)", wall / iters, "ms");
  report ("F3 row capacity", rows_per_sec, "rows/s");

  for (auto *p2 : tx)
    {
      delete p2;
    }
}
