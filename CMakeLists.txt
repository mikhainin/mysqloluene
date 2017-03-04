# Copyright (c) 2006, 2014, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

cmake_minimum_required (VERSION 2.8)

PROJECT (MYSQLOLUENE)

SET (CMAKE_MODULE_PATH "${MYSQLOLUENE_SOURCE_DIR}/cmake" )
# TODO: set the path via commandline parameter or find it
# using separated .cmake-script
# include("/Users/mikhailgalanin/src/mysql-5.7.16/cmake/plugin.cmake")
# include("/Users/mikhailgalanin/src/mysql-5.7.16/cmake/mysql_version.cmake")

find_package (MYSQL)
set (USE_MYSQL 1)

include_directories (${mysql_INCLUDE})
include_directories (${CMAKE_CURRENT_SOURCE_DIR})

include_directories("/Users/mikhailgalanin/src/mysql-5.7.16/sql/")
include_directories("/Users/mikhailgalanin/src/mysql-5.7.16/include/")
include_directories("/Users/mikhailgalanin/src/mysql-5.7.16//libbinlogevents/export")
include_directories("/Users/mikhailgalanin/src/mysql-5.7.16//libbinlogevents/include")
include_directories("/Users/mikhailgalanin/src/tarantool-c/")
include_directories("/Users/mikhailgalanin/src/tarantool-c/include")
include_directories("/Users/mikhailgalanin/src/tarantool-c/third_party/msgpuck")

link_directories("/Users/mikhailgalanin/src/mysql-5.7.16/sql")
link_directories("/Users/mikhailgalanin/src/mysql-5.7.16/libservices")
link_directories("/Users/mikhailgalanin/src/mysql-5.7.16/dbug")

link_directories("/Users/mikhailgalanin/src/tarantool-c/build/tnt")
link_directories("/Users/mikhailgalanin/src/tarantool-c/build/third_party/msgpuck/")

SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DSAFEMALLOC -DSAFE_MUTEX -g")
SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DSAFEMALLOC -DSAFE_MUTEX -g")

ADD_DEFINITIONS(-DMYSQL_SERVER)
ADD_DEFINITIONS(-DMYSQL_DYNAMIC_PLUGIN)
# ADD_DEFINITIONS(-DDBUG_OFF) # TODO: do it depending on mysql build type
ADD_DEFINITIONS( -DENABLED_DEBUG_SYNC -DSAFE_MUTEX) # TODO: do it depending on mysql build type
# ADD_DEFINITIONS(-DHAVE_STRUCT_TIMESPEC)

set(CMAKE_SHARED_MODULE_LOADER_C_FLAG "-Wl,-bundle_loader,")
set(CMAKE_SHARED_MODULE_LOADER_CXX_FLAG "-Wl,-bundle_loader,")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -Wl,-bundle_loader,/Users/mikhailgalanin/src/mysql-5.7.16/sql/mysqld")
# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-bundle_loader,/Users/mikhailgalanin/src/mysql-5.7.16/sql/mysqld")

SET(MYSQLOLUENE_PLUGIN_DYNAMIC "ha_mysqloluene")
SET(MYSQLOLUENE_SOURCES 
	src/ha_mysqloluene.cc 
	src/tnt_connection.cc
	src/tnt_row.cc
)

# IF(WITH_MYSQLOLUENE_STORAGE_ENGINE AND NOT WITHOUT_MYSQLOLUENE_STORAGE_ENGINE)
#   MYSQL_ADD_PLUGIN(mysqloluene ${MYSQLOLUENE_SOURCES} STORAGE_ENGINE DEFAULT)
# ELSEIF(NOT WITHOUT_EXAMPLE_STORAGE_ENGINE)
#   MYSQL_ADD_PLUGIN(mysqloluene ${MYSQLOLUENE_SOURCES} STORAGE_ENGINE MODULE_ONLY)
# ENDIF()
ADD_LIBRARY(mysqloluene SHARED MODULE ${MYSQLOLUENE_SOURCES})

# -Wl,-bundle_loader,/Users/mikhailgalanin/src/mysql-5.7.16/sql/mysqld

TARGET_LINK_LIBRARIES(mysqloluene mysqlservices dbug msgpuck tarantool) # /Users/mikhailgalanin/src/mysql-5.7.16/sql/mysqld)
SET_TARGET_PROPERTIES(mysqloluene PROPERTIES ENABLE_EXPORTS TRUE)
# set_target_properties(mysqloluene PROPERTIES COMPILE_FLAGS "-Wl,-bundle_loader,/Users/mikhailgalanin/src/mysql-5.7.16/sql/mysqld" )

