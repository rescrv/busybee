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

// C++
#include <iostream>

// STL
#include <memory>
#include <tr1/random>
#include <vector>

// po6
#include <po6/net/location.h>

// e
#include <e/buffer.h>
#include <e/timer.h>

// BusyBee
#include "busybee_constants.h"

template <typename busybee>
int
benchmark(busybee* bb, po6::threads::mutex* io, const std::vector<po6::net::location>& others)
{
    std::tr1::mt19937 rng;
    std::tr1::uniform_int<> dist(0, others.size() - 1);

#define BILLION 1000000000
    const uint64_t start = e::time();
    uint64_t target = start + BILLION;
    uint64_t ops = 0;

    while (true)
    {
        uint64_t now = e::time();

        if (now > target)
        {
            po6::threads::mutex::hold hold(io);
            std::cout << target / BILLION << " " << ops << std::endl;
            ops = 0;
            target += BILLION;
        }

        po6::net::location loc = others[dist(rng)];
        std::auto_ptr<e::buffer> msg(e::buffer::create(BUSYBEE_HEADER_SIZE + 1));
        msg->resize(BUSYBEE_HEADER_SIZE + 1);
        busybee_returncode ret;

        switch ((ret = bb->send(loc, msg)))
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_QUEUED:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_DISCONNECT:
            case BUSYBEE_CONNECTFAIL:
            case BUSYBEE_ADDFDFAILED:
            default:
                std::cerr << "send error:  " << (unsigned) ret << std::endl;
        }

        switch (bb->recv(&loc, &msg))
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_QUEUED:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_DISCONNECT:
            case BUSYBEE_CONNECTFAIL:
            case BUSYBEE_ADDFDFAILED:
            default:
                std::cerr << "send error:  " << (unsigned) ret << std::endl;
        }

        ++ops;
    }

    abort();
}
