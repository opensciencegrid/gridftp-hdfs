
cmake_minimum_required( VERSION 2.6 )
project( gridftp-hdfs )

set( CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake )

find_package( Hdfs REQUIRED )
find_package( Jvm REQUIRED )

include (FindPkgConfig)
pkg_check_modules(OPENSSL REQUIRED libcrypto)
pkg_check_modules(GLOBUS_GRIDFTP_SERVER REQUIRED globus-gridftp-server)
pkg_check_modules(GLOBUS_COMMON REQUIRED globus-common)

if( CMAKE_COMPILER_IS_GNUCXX )
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror" )
endif()

if( CMAKE_COMPILER_IS_GNUCC )
  set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror" )
endif()

SET( CMAKE_SHARED_LINKER_FLAGS "-Wl,--no-undefined")
SET( CMAKE_MODULE_LINKER_FLAGS "-Wl,--no-undefined")
SET( CMAKE_EXE_LINKER_FLAGS "-Wl,--no-undefined")

include_directories(${HDFS_INCLUDES} ${JVM_INCLUDES} ${JVM_MD_INCLUDES} ${OPENSSL_INCLUDEDIR} ${GLOBUS_GRIDFTP_SERVER_INCLUDE_DIRS} ${GLOBUS_COMMON_INCLUDE_DIRS})

include_directories( ${PROJECT_SOURCE_DIR} )

add_library(globus_gridftp_server_hdfs MODULE
              src/gridftp_hdfs_buffers.c
              src/gridftp_hdfs.c
              src/gridftp_hdfs_cksm.c
              src/gridftp_hdfs_recv.c
              src/gridftp_hdfs_send.c
              src/gridftp_hdfs_stat.c
              src/gridftp_hdfs_error.h
              src/gridftp_hdfs.h
           )
target_link_libraries(globus_gridftp_server_hdfs -lz -lrt -pthread ${HDFS_LIB} ${GLOBUS_GRIDFTP_SERVER_LIBRARIES} ${GLOBUS_COMMON_LIBRARIES} ${OPENSSL_LIBRARIES})

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/conf/gridftp-inetd.conf.in ${CMAKE_CURRENT_BINARY_DIR}/conf/gridftp-inetd.conf @ONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/scripts/gridftp-hdfs-standalone.in ${CMAKE_CURRENT_BINARY_DIR}/scripts/gridftp-hdfs-standalone @ONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/scripts/xinetd/gridftp-hdfs.in ${CMAKE_CURRENT_BINARY_DIR}/scripts/xinetd/gridftp-hdfs @ONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/scripts/xinetd/gridftp-hdfs-inetd.in ${CMAKE_CURRENT_BINARY_DIR}/scripts/xinetd/gridftp-hdfs-inetd @ONLY)

SET(LIB_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/lib" CACHE PATH "Install path for libraries")
SET(SHARE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}/share" CACHE PATH "Data directory for helper files")
SET(SYSCONF_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/etc" CACHE PATH "System configuration directory")

install(
  TARGETS globus_gridftp_server_hdfs
  LIBRARY DESTINATION ${LIB_INSTALL_DIR} )

install(
  PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/scripts/gridftp-hdfs-standalone
  DESTINATION bin)

install(
  PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/scripts/xinetd/gridftp-hdfs-inetd
  DESTINATION sbin)

set_target_properties(
  globus_gridftp_server_hdfs
  PROPERTIES
  VERSION   "0.0.1"
  SOVERSION "0"
  LINK_INTERFACE_LIBRARIES "" )

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/scripts/xinetd/gridftp-hdfs
  DESTINATION ${SYSCONF_INSTALL_DIR}/xinetd.d
)

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/conf/gridftp-inetd.conf ${CMAKE_SOURCE_DIR}/conf/gridftp-debug.conf ${CMAKE_SOURCE_DIR}/conf/replica-map.conf
  DESTINATION ${SYSCONF_INSTALL_DIR}/gridftp-hdfs
)

install(
  FILES ${CMAKE_SOURCE_DIR}/conf/gridftp-hdfs
  DESTINATION ${SYSCONF_INSTALL_DIR}/sysconfig
)

install(
  FILES ${CMAKE_SOURCE_DIR}/conf/gridftp-hdfs-environment
  DESTINATION ${SHARE_INSTALL_PREFIX}/gridftp-hdfs
)

install(
  FILES ${CMAKE_SOURCE_DIR}/conf/globus-gridftp-server-plugin
  DESTINATION ${SHARE_INSTALL_PREFIX}/osg/sysconfig
)

install(
  FILES ${CMAKE_SOURCE_DIR}/conf/gridftp.d/gridftp-hdfs.conf ${CMAKE_SOURCE_DIR}/conf/gridftp.d/gridftp-hdfs.osg-extensions.conf
  DESTINATION ${SYSCONF_INSTALL_DIR}/gridftp.d
)

