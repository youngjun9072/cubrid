#
#  Copyright 2008 Search Solution Corporation
#  Copyright 2016 CUBRID Corporation
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#

project (test_log_writeset)

set (TEST_LOG_WRITESET_SRC
  test_main.cpp
  test_log_writeset.cpp
  )

SET_SOURCE_FILES_PROPERTIES(
  ${TEST_LOG_WRITESET_SRC}
  PROPERTIES LANGUAGE CXX
  )

add_executable(test_log_writeset
  ${TEST_LOG_WRITESET_SRC}
  )

target_compile_definitions(test_log_writeset PRIVATE
  SERVER_MODE
  CATCH_CONFIG_ENABLE_BENCHMARKING
  ${COMMON_DEFS}
  )

target_include_directories(test_log_writeset PRIVATE
  ${TEST_INCLUDES}
  )

target_link_libraries(test_log_writeset PRIVATE
  test_common
  )
if(UNIX)
  target_link_libraries(test_log_writeset PRIVATE
    cubrid
    )
else()
  message( SEND_ERROR "log_writeset unit testing is for unix")
endif ()

add_dependencies(test_log_writeset
  ${TESTS_EXTERNAL_TARGETS}
  )
