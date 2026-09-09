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
 * test_log_writeset_common.hpp - shared fixtures and helpers for the writeset
 *                                unit test translation units (behavior,
 *                                benchmark, stress). Helpers are inline in the
 *                                wstest namespace so an unused one in any single
 *                                translation unit does not warn.
 */

#ifndef _TEST_LOG_WRITESET_COMMON_HPP_
#define _TEST_LOG_WRITESET_COMMON_HPP_

#include <thread>
#include <vector>

#include "catch2/catch.hpp"

#include "dbtype.h"
#include "log_impl.h"
#include "log_writeset.h"
#include "object_domain.h"
#include "oid.h"

namespace wstest
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

  inline LOG_LSA
  lsa_of (INT64 pageid, short offset)
  {
    LOG_LSA lsa;

    lsa.pageid = pageid;
    lsa.offset = offset;
    return lsa;
  }

  inline OID
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
  inline log_tdes *
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

  inline void
  add_write_int (log_tdes * tdes, const OID * cls, int key)
  {
    DB_VALUE pk;

    db_make_int (&pk, key);
    REQUIRE (log_writeset_add_dbvalue (NULL, tdes, cls, &pk) == NO_ERROR);
  }

  inline void
  add_ref_int (log_tdes * tdes, const OID * parent_cls, int key)
  {
    DB_VALUE fk;
    TP_DOMAIN *int_domain = tp_domain_resolve_default (DB_TYPE_INTEGER);

    REQUIRE (int_domain != NULL);
    db_make_int (&fk, key);
    REQUIRE (log_writeset_add_ref_dbvalue (NULL, tdes, parent_cls, &fk, int_domain) == NO_ERROR);
  }

  inline LOG_LSA
  probe (log_tdes * tdes)
  {
    LOG_LSA dep;

    log_writeset_commit_probe (NULL, tdes, &dep);
    return dep;
  }
}

#endif /* _TEST_LOG_WRITESET_COMMON_HPP_ */
