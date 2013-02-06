// Copyright (c) 2012, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of BusyBee nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

// POSIX
#ifndef _MSC_VER
#include <ifaddrs.h>
#endif

// po6
#include <po6/net/location.h>

// e
#include <e/guard.h>
#include <e/timer.h>

// BusyBee
#include "busybee_utils.h"

bool
busybee_discover(po6::net::ipaddr* ip)
{
#ifndef _MSC_VER
    struct ifaddrs* ifa = NULL;

    if (getifaddrs(&ifa) < 0 || !ifa)
    {
        return false;
    }

    e::guard g = e::makeguard(freeifaddrs, ifa);
    g.use_variable();

    for (struct ifaddrs* ifap = ifa; ifap; ifap = ifap->ifa_next)
    {
        if (strncmp(ifap->ifa_name, "lo", 2) == 0)
        {
            continue;
        }

        if (ifap->ifa_addr->sa_family == AF_INET)
        {
            po6::net::location loc(ifap->ifa_addr, sizeof(sockaddr_in));
            *ip = loc.address;
            return true;
        }
        else if (ifap->ifa_addr->sa_family == AF_INET6)
        {
            po6::net::location loc(ifap->ifa_addr, sizeof(sockaddr_in6));
            *ip = loc.address;
            return true;
        }
    }

    errno = 0;
#endif
    return false;
}

uint64_t
busybee_generate_id()
{
    return e::time(); // XXX weak!
}
