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

#include "catch2/catch.hpp"

#include "dbtype.h"
#include "log_impl.h"
#include "log_writeset.h"
#include "object_domain.h"
#include "oid.h"

namespace
{
  /* every case runs on a freshly initialized global history; initialize resets the
   * map, history_start and the commit-order baseline, so cases stay isolated */
  struct ws_history_guard
  {
    ws_history_guard ()
    {
      REQUIRE (log_writeset_history_initialize () == NO_ERROR);
    }
    ~ws_history_guard ()
    {
      log_writeset_history_finalize ();
    }
  };

  LOG_LSA
  lsa_of (INT64 pageid, short offset)
  {
    LOG_LSA lsa;

    lsa.pageid = pageid;
    lsa.offset = offset;
    return lsa;
  }

  OID
  oid_of (short volid, int pageid, short slotid)
  {
    OID oid;

    oid.volid = volid;
    oid.pageid = pageid;
    oid.slotid = slotid;
    return oid;
  }

  /* a transaction descriptor with only the writeset-related fields prepared;
   * the writeset functions touch nothing else of log_tdes */
  log_tdes *
  make_tdes (int trid)
  {
    log_tdes *tdes = new log_tdes ();

    tdes->trid = trid;
    tdes->ws_hashes.clear ();
    tdes->ws_overflow = false;
    LSA_SET_NULL (&tdes->ws_dependency_seq);
    tdes->ws_stat_collect_ns = 0;
    return tdes;
  }

  void
  add_write_int (log_tdes * tdes, const OID * cls, int key)
  {
    DB_VALUE pk;

    db_make_int (&pk, key);
    REQUIRE (log_writeset_add_dbvalue (NULL, tdes, cls, &pk) == NO_ERROR);
  }

  void
  add_ref_int (log_tdes * tdes, const OID * parent_cls, int key)
  {
    DB_VALUE fk;
    TP_DOMAIN *int_domain = tp_domain_resolve_default (DB_TYPE_INTEGER);

    REQUIRE (int_domain != NULL);
    db_make_int (&fk, key);
    REQUIRE (log_writeset_add_ref_dbvalue (NULL, tdes, parent_cls, &fk, int_domain) == NO_ERROR);
  }

  LOG_LSA
  probe (log_tdes * tdes)
  {
    LOG_LSA dep;

    log_writeset_commit_probe (NULL, tdes, &dep);
    return dep;
  }
}

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

TEST_CASE ("hash unit cost benchmarks", "[benchmark]")
{
  ws_history_guard history;
  OID cls = oid_of (1, 100, 1);
  OID parent_cls = oid_of (1, 200, 1);
  TP_DOMAIN *int_domain = tp_domain_resolve_default (DB_TYPE_INTEGER);
  DB_VALUE v;
  int seq = 0;

  REQUIRE (int_domain != NULL);

  log_tdes *scratch = make_tdes (900);

  BENCHMARK ("WRITE add x1000 (pack+hash; read mean/1000 for per-key ns)")
  {
    for (int i = 0; i < 1000; i++)
      {
	db_make_int (&v, seq++);
	log_writeset_add_dbvalue (NULL, scratch, &cls, &v);
      }
    scratch->ws_hashes.clear ();
    return seq;
  };

  BENCHMARK ("REF add x1000 (cast+pack+hash; read mean/1000 for per-key ns)")
  {
    for (int i = 0; i < 1000; i++)
      {
	db_make_int (&v, seq++);
	log_writeset_add_ref_dbvalue (NULL, scratch, &parent_cls, &v, int_domain);
      }
    scratch->ws_hashes.clear ();
    return seq;
  };

  /* fill the history with 100k distinct keys so probe/flush run against a
   * realistically loaded map */
  {
    log_tdes *filler = make_tdes (901);
    LOG_LSA fill_lsa = lsa_of (100, 0);

    for (int i = 0; i < 100000; i++)
      {
	db_make_int (&v, 1000000 + i);
	log_writeset_add_dbvalue (NULL, filler, &cls, &v);
      }
    log_writeset_commit_flush (NULL, filler, &fill_lsa);
    delete filler;
  }

  log_tdes *worker = make_tdes (902);
  for (int i = 0; i < 1000; i++)
    {
      db_make_int (&v, 1000000 + i);	/* all 1000 keys hit the map */
      log_writeset_add_dbvalue (NULL, worker, &cls, &v);
    }

  LOG_LSA dep;
  BENCHMARK ("probe x1000 keys vs 100k map (read mean/1000 for per-key ns)")
  {
    log_writeset_commit_probe (NULL, worker, &dep);
    return dep.pageid;
  };

  LOG_LSA flush_lsa = lsa_of (200, 0);
  BENCHMARK ("flush x1000 keys vs 100k map (update; read mean/1000 for per-key ns)")
  {
    log_writeset_commit_flush (NULL, worker, &flush_lsa);
    return 0;
  };

  delete worker;
  delete scratch;
}
