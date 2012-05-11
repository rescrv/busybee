## Copyright (c) 2012, Cornell University
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met:
##
##     * Redistributions of source code must retain the above copyright notice,
##       this list of conditions and the following disclaimer.
##     * Redistributions in binary form must reproduce the above copyright
##       notice, this list of conditions and the following disclaimer in the
##       documentation and/or other materials provided with the distribution.
##     * Neither the name of BusyBee nor the names of its contributors may be
##       used to endorse or promote products derived from this software without
##       specific prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
## AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
## IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
## ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
## LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
## CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
## SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
## INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
## CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
## ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
## POSSIBILITY OF SUCH DAMAGE.

ACLOCAL_AMFLAGS = -I m4 ${ACLOCAL_FLAGS}
AM_CXXFLAGS = -fvisibility=hidden -fPIC $(WANAL_CXXFLAGS) $(E_CFLAGS)

EXTRA_DIST = LICENSE

include_HEADERS = busybee_constants.h busybee_returncode.h busybee_mta.h busybee_sta.h busybee_st.h

noinst_HEADERS = bench.h

lib_LTLIBRARIES = libbusybee-mta.la libbusybee-sta.la libbusybee-st.la

libbusybee_mta_la_SOURCES = busybee.cc
libbusybee_mta_la_CPPFLAGS = -DBUSYBEE_MTA
libbusybee_mta_la_LDFLAGS = -static

libbusybee_sta_la_SOURCES = busybee.cc
libbusybee_sta_la_CPPFLAGS = -DBUSYBEE_STA
libbusybee_sta_la_LDFLAGS = -static

libbusybee_st_la_SOURCES = busybee.cc
libbusybee_st_la_CPPFLAGS = -DBUSYBEE_ST
libbusybee_st_la_LDFLAGS = -static

noinst_PROGRAMS = busybee-bench-mta busybee-bench-sta busybee-bench-st

busybee_bench_mta_SOURCES = busybee-bench-mta.cc
busybee_bench_mta_LDADD = libbusybee-mta.la -le -lrt

busybee_bench_sta_SOURCES = busybee-bench-sta.cc
busybee_bench_sta_LDADD = libbusybee-sta.la -le -lrt

busybee_bench_st_SOURCES = busybee-bench-st.cc
busybee_bench_st_LDADD = libbusybee-st.la -le -lrt

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = busybee.pc
