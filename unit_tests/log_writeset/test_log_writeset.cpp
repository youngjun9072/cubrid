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
 * test_log_writeset.cpp - behavior and micro-benchmark tests for the writeset
 *                         collection and global commit history (writeset PoC)
 *
 * Behavior cases pin down the dependency rules the parallel slave relies on:
 * who waits behind whom, what is published into the history, and how overflow
 * degrades to commit order. Benchmarks measure the per-key unit costs that the
 * integration bench (doc 6) can only observe mixed with workload contention.
 */

#include "test_log_writeset_common.hpp"

using namespace wstest;

TEST_CASE ("independent transaction gets a NULL dependency", "[writeset]")
{
  ws_history_guard history;
  OID cls = oid_of (1, 100, 1);
  log_tdes *t1 = make_tdes (1);

  add_write_int (t1, &cls, 1);

  LOG_LSA dep = probe (t1);
  REQUIRE (LSA_ISNULL (&dep));

  delete t1;
}

TEST_CASE ("same key waits behind its previous writer", "[writeset]")
{
  ws_history_guard history;
  OID cls = oid_of (1, 100, 1);
  LOG_LSA l1 = lsa_of (10, 0);

  log_tdes *t1 = make_tdes (1);
  add_write_int (t1, &cls, 1);
  log_writeset_commit_flush (NULL, t1, &l1);

  log_tdes *t2 = make_tdes (2);
  add_write_int (t2, &cls, 1);

  LOG_LSA dep = probe (t2);
  REQUIRE (LSA_EQ (&dep, &l1));
  REQUIRE (!t2->ws_dependency_is_read);	/* write-origin: exact completion of l1 is sufficient */

  delete t1;
  delete t2;
}

TEST_CASE ("a different key is not serialized behind an unrelated commit", "[writeset]")
{
  ws_history_guard history;
  OID cls = oid_of (1, 100, 1);
  LOG_LSA l1 = lsa_of (10, 0);

  log_tdes *t1 = make_tdes (1);
  add_write_int (t1, &cls, 1);
  log_writeset_commit_flush (NULL, t1, &l1);

  log_tdes *t2 = make_tdes (2);
  add_write_int (t2, &cls, 2);

  LOG_LSA dep = probe (t2);
  REQUIRE (LSA_ISNULL (&dep));

  delete t1;
  delete t2;
}

TEST_CASE ("dependency is the newest commit among all touched keys", "[writeset]")
{
  ws_history_guard history;
  OID cls = oid_of (1, 100, 1);
  LOG_LSA l1 = lsa_of (10, 0);
  LOG_LSA l2 = lsa_of (20, 0);

  log_tdes *t1 = make_tdes (1);
  add_write_int (t1, &cls, 1);
  log_writeset_commit_flush (NULL, t1, &l1);

  log_tdes *t2 = make_tdes (2);
  add_write_int (t2, &cls, 2);
  log_writeset_commit_flush (NULL, t2, &l2);

  log_tdes *t3 = make_tdes (3);
  add_write_int (t3, &cls, 1);
  add_write_int (t3, &cls, 2);

  LOG_LSA dep = probe (t3);
  REQUIRE (LSA_EQ (&dep, &l2));

  delete t1;
  delete t2;
  delete t3;
}

TEST_CASE ("foreign-key reference waits behind the parent's writer", "[writeset]")
{
  ws_history_guard history;
  OID parent_cls = oid_of (1, 200, 1);
  LOG_LSA l1 = lsa_of (10, 0);

  /* parent INSERT publishes its primary key */
  log_tdes *parent = make_tdes (1);
  add_write_int (parent, &parent_cls, 7);
  log_writeset_commit_flush (NULL, parent, &l1);

  /* child references the same value through its foreign key: the REF hash must
   * collide with the parent's WRITE hash (byte-identical packing invariant) */
  log_tdes *child = make_tdes (2);
  add_ref_int (child, &parent_cls, 7);

  LOG_LSA dep = probe (child);
  REQUIRE (LSA_EQ (&dep, &l1));

  delete parent;
  delete child;
}

TEST_CASE ("references stamp the read slot: siblings stay parallel, a later parent write waits", "[writeset]")
{
  ws_history_guard history;
  OID parent_cls = oid_of (1, 200, 1);
  LOG_LSA l1 = lsa_of (10, 0);
  LOG_LSA l2 = lsa_of (20, 0);

  log_tdes *parent = make_tdes (1);
  add_write_int (parent, &parent_cls, 7);
  log_writeset_commit_flush (NULL, parent, &l1);

  /* first child references the parent and commits: its own dependency is the
   * parent's writer (write slot), and it stamps the read slot at flush */
  log_tdes *child1 = make_tdes (2);
  add_ref_int (child1, &parent_cls, 7);
  LOG_LSA dep1 = probe (child1);
  REQUIRE (LSA_EQ (&dep1, &l1));
  REQUIRE (!child1->ws_dependency_is_read);
  log_writeset_commit_flush (NULL, child1, &l2);

  /* a sibling referencing the same parent still depends on the parent only:
   * references consult the write slot, never the read slot, so child1's stamp
   * does not chain the siblings */
  log_tdes *child2 = make_tdes (3);
  add_ref_int (child2, &parent_cls, 7);
  LOG_LSA dep2 = probe (child2);
  REQUIRE (LSA_EQ (&dep2, &l1));
  REQUIRE (!child2->ws_dependency_is_read);

  /* a later write to the parent row now sees the newest referencer (l2), not
   * just the previous writer (l1): the reverse-order blind spot of doc 5 is
   * closed. The label carries the read-origin flag so the slave gate waits for
   * the gap-free frontier instead of that one transaction's completion. */
  log_tdes *parent_delete = make_tdes (4);
  add_write_int (parent_delete, &parent_cls, 7);
  LOG_LSA dep3 = probe (parent_delete);
  REQUIRE (LSA_EQ (&dep3, &l2));
  REQUIRE (parent_delete->ws_dependency_is_read);

  delete parent;
  delete child1;
  delete child2;
  delete parent_delete;
}

TEST_CASE ("read slot on a never-written key: parallel siblings, monotonic stamp, flagged writer", "[writeset]")
{
  ws_history_guard history;
  OID parent_cls = oid_of (1, 200, 1);
  LOG_LSA l5 = lsa_of (50, 0);
  LOG_LSA l3 = lsa_of (30, 0);

  /* the referenced key has no write history (e.g. the parent row predates the
   * current history window): the reference creates the entry with only the
   * read slot filled */
  log_tdes *child1 = make_tdes (1);
  add_ref_int (child1, &parent_cls, 9);
  LOG_LSA dep1 = probe (child1);
  REQUIRE (LSA_ISNULL (&dep1));
  log_writeset_commit_flush (NULL, child1, &l5);

  /* a sibling still sees an empty write slot: independent */
  log_tdes *child2 = make_tdes (2);
  add_ref_int (child2, &parent_cls, 9);
  LOG_LSA dep2 = probe (child2);
  REQUIRE (LSA_ISNULL (&dep2));

  /* an out-of-order (older) reference flush must not move the read slot backwards */
  log_tdes *child3 = make_tdes (3);
  add_ref_int (child3, &parent_cls, 9);
  log_writeset_commit_flush (NULL, child3, &l3);

  /* the parent's writer waits for the newest referencer (l5, not l3) with the
   * read-origin flag set */
  log_tdes *parent_delete = make_tdes (4);
  add_write_int (parent_delete, &parent_cls, 9);
  LOG_LSA dep3 = probe (parent_delete);
  REQUIRE (LSA_EQ (&dep3, &l5));
  REQUIRE (parent_delete->ws_dependency_is_read);

  delete child1;
  delete child2;
  delete child3;
  delete parent_delete;
}

TEST_CASE ("overflow demotes to commit order and raises the history floor", "[writeset]")
{
  ws_history_guard history;
  OID cls = oid_of (1, 100, 1);
  LOG_LSA l1 = lsa_of (10, 0);
  LOG_LSA l3 = lsa_of (30, 0);

  log_tdes *t1 = make_tdes (1);
  add_write_int (t1, &cls, 1);
  log_writeset_commit_flush (NULL, t1, &l1);

  /* an overflowed transaction waits for everything before it (= prev commit) */
  log_tdes *ovf = make_tdes (2);
  ovf->ws_overflow = true;
  LOG_LSA dep = probe (ovf);
  REQUIRE (LSA_EQ (&dep, &l1));
  REQUIRE (!ovf->ws_dependency_is_read);

  /* and its flush clears the history and raises the floor, so everything
   * after waits for the overflow commit */
  log_writeset_commit_flush (NULL, ovf, &l3);

  log_tdes *t3 = make_tdes (3);
  add_write_int (t3, &cls, 99);
  LOG_LSA dep3 = probe (t3);
  REQUIRE (LSA_EQ (&dep3, &l3));

  delete t1;
  delete ovf;
  delete t3;
}

TEST_CASE ("per-transaction key limit flips the transaction to overflow", "[writeset]")
{
  ws_history_guard history;
  OID cls = oid_of (1, 100, 1);
  log_tdes *t1 = make_tdes (1);

  for (int i = 0; i <= LOG_WRITESET_TX_LIMIT && !t1->ws_overflow; i++)
    {
      add_write_int (t1, &cls, i);
    }
  REQUIRE (t1->ws_overflow);
  REQUIRE (t1->ws_hashes.empty ());	/* the collected writeset is dropped */

  delete t1;
}

/* ------------------------------------------------------------------------- *
 * Concurrency correctness (the single global latch must protect the map so
 * concurrent commits produce the same slots as if they ran one at a time).
 * Workers call only probe/flush; the writeset is collected up front on the
 * main thread, so no worker touches the cast path (db_private_alloc / TLS
 * THREAD_ENTRY). See test_log_writeset_stress.cpp for the timing sweeps.
 * ------------------------------------------------------------------------- */

TEST_CASE ("concurrent flushes on disjoint keys all publish (no lost update under the latch)", "[writeset]")
{
  ws_history_guard history;
  OID cls = oid_of (1, 100, 1);
  const int nthreads = 8;
  const int per_thread = 500;

  /* each thread flushes its own disjoint key range with a distinct commit LSA */
  std::vector<log_tdes *> tx (nthreads);
  for (int t = 0; t < nthreads; t++)
    {
      tx[t] = make_tdes (t + 1);
      for (int i = 0; i < per_thread; i++)
	{
	  add_write_int (tx[t], &cls, t * per_thread + i);
	}
    }

  std::vector<std::thread> workers;
  for (int t = 0; t < nthreads; t++)
    {
      LOG_LSA commit = lsa_of (10 + t, 0);
      workers.emplace_back ([tx, t, commit] ()
	{
	  log_writeset_commit_flush (NULL, tx[t], &commit);
	});
    }
  for (auto &w : workers)
    {
      w.join ();
    }

  /* every disjoint key from every thread must be present: nthreads * per_thread
   * entries, none lost to a race on the map */
  REQUIRE (log_Writeset_history.map.size () == (size_t) (nthreads * per_thread));

  for (int t = 0; t < nthreads; t++)
    {
      delete tx[t];
    }
}

TEST_CASE ("concurrent siblings referencing one parent stay parallel", "[writeset]")
{
  ws_history_guard history;
  OID parent_cls = oid_of (1, 200, 1);
  LOG_LSA lp = lsa_of (10, 0);
  const int nsiblings = 16;

  /* parent published, then many children reference the same parent key */
  log_tdes *parent = make_tdes (1);
  add_write_int (parent, &parent_cls, 7);
  log_writeset_commit_flush (NULL, parent, &lp);

  std::vector<log_tdes *> child (nsiblings);
  for (int i = 0; i < nsiblings; i++)
    {
      child[i] = make_tdes (100 + i);
      add_ref_int (child[i], &parent_cls, 7);
    }

  /* all siblings probe concurrently; each must depend only on the parent's
   * writer (references consult the write slot only), never on one another */
  std::vector<LOG_LSA> dep (nsiblings);
  std::vector<char> is_read (nsiblings);
  std::vector<std::thread> workers;
  for (int i = 0; i < nsiblings; i++)
    {
      workers.emplace_back ([&child, &dep, &is_read, i] ()
	{
	  LOG_LSA d;
	  log_writeset_commit_probe (NULL, child[i], &d);
	  dep[i] = d;
	  is_read[i] = child[i]->ws_dependency_is_read;
	});
    }
  for (auto &w : workers)
    {
      w.join ();
    }

  for (int i = 0; i < nsiblings; i++)
    {
      REQUIRE (LSA_EQ (&dep[i], &lp));
      REQUIRE (!is_read[i]);
      delete child[i];
    }
  delete parent;
}

TEST_CASE ("concurrent references then a parent write waits behind the newest referencer", "[writeset]")
{
  ws_history_guard history;
  OID parent_cls = oid_of (1, 200, 1);
  const int nsiblings = 16;
  LOG_LSA lp = lsa_of (10, 0);

  log_tdes *parent = make_tdes (1);
  add_write_int (parent, &parent_cls, 7);
  log_writeset_commit_flush (NULL, parent, &lp);

  /* children flush references to the same parent concurrently, each with a
   * distinct commit LSA; the newest among them is lseq(nsiblings) */
  std::vector<log_tdes *> child (nsiblings);
  LOG_LSA newest = lsa_of (0, 0);
  for (int i = 0; i < nsiblings; i++)
    {
      child[i] = make_tdes (100 + i);
      add_ref_int (child[i], &parent_cls, 7);
      LOG_LSA c = lsa_of (20 + i, 0);
      if (LSA_GT (&c, &newest))
	{
	  newest = c;
	}
    }

  std::vector<std::thread> workers;
  for (int i = 0; i < nsiblings; i++)
    {
      LOG_LSA commit = lsa_of (20 + i, 0);
      workers.emplace_back ([&child, i, commit] ()
	{
	  log_writeset_commit_flush (NULL, child[i], &commit);
	});
    }
  for (auto &w : workers)
    {
      w.join ();
    }

  /* a later write to the parent row must wait for the newest referencer (read
   * slot holds the max under concurrent stamps: no lost update, monotonic) */
  log_tdes *parent_delete = make_tdes (900);
  add_write_int (parent_delete, &parent_cls, 7);
  LOG_LSA dep = probe (parent_delete);
  REQUIRE (LSA_EQ (&dep, &newest));
  REQUIRE (parent_delete->ws_dependency_is_read);

  for (int i = 0; i < nsiblings; i++)
    {
      delete child[i];
    }
  delete parent;
  delete parent_delete;
}
