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

#ifndef busybee_bench_h_
#define busybee_bench_h_

// C
#include <cstdio>

// POSIX
#include <unistd.h>

// STL
#include <algorithm>
#include <memory>
#include <tr1/random>
#include <utility>
#include <vector>

// po6
#include <po6/net/location.h>
#include <po6/threads/mutex.h>

// e
#include <e/buffer.h>
#include <e/time.h>

// BusyBee
#include "busybee_constants.h"
#include "busybee_mapper.h"
#include "busybee_returncode.h"
#include "busybee_utils.h"

typedef std::vector<std::pair<uint64_t, po6::net::location> > membership_list;

bool
parse_args(int argc, const char* argv[],
           membership_list* ml,
           po6::net::location* bind_to,
           uint64_t* server_id)
{
    if (argc % 3 != 1 || argc <= 1)
    {
        return false;
    }

    uint64_t bid = atoi(argv[0]);
    *bind_to = po6::net::location();
    *server_id = 0;

    for (size_t i = 1; i < argc; i += 3)
    {
        uint64_t id = atoi(argv[i]);
        po6::net::location loc(argv[i + 1], atoi(argv[i + 2]));
        ml->push_back(std::make_pair(id, loc));

        if (bid == id)
        {
            *bind_to = loc;
            *server_id = id;
        }
    }

    std::sort(ml->begin(), ml->end());
    return true;
}

class benchmapper : public busybee_mapper
{
    public:
        benchmapper(membership_list* ml) : m_ml(ml) {}
        virtual ~benchmapper() throw () {}

    public:
        virtual bool lookup(uint64_t server_id, po6::net::location* loc)
        {
            for (size_t i = 0; i < m_ml->size(); ++i)
            {
                if ((*m_ml)[i].first == server_id)
                {
                    *loc = (*m_ml)[i].second;
                    return true;
                }
            }

            return false;
        }

    private:
        membership_list* m_ml;

    private:
        benchmapper(const benchmapper&);
        benchmapper& operator = (const benchmapper&);
};

template <typename busybee>
int
benchmark(busybee* bb,
          po6::threads::mutex* io,
          const membership_list& ml,
          bool do_send,
          bool do_recv)
{
    std::tr1::mt19937 rng(time(NULL) * getpid());
    std::tr1::uniform_int<> dist(0, ml.size() - 1);

#define BILLION 1000000000ULL
    const uint64_t start = e::time();
    uint64_t target = start + BILLION;
    uint64_t ops = 0;

    while (true)
    {
        uint64_t now = e::time();

        if (now - start > 10 * BILLION)
        {
            break;
        }

        if (now > target)
        {
            po6::threads::mutex::hold hold(io);
            std::cout << target / BILLION << " " << ops << std::endl;
            ops = 0;
            target += BILLION;
        }

        for (size_t i = 0; i < 100; ++i)
        {
            uint64_t server_id = ml[dist(rng)].first;
            std::auto_ptr<e::buffer> msg(e::buffer::create(BUSYBEE_HEADER_SIZE + 1));
            msg->resize(BUSYBEE_HEADER_SIZE + 1);
            busybee_returncode ret;

            if (do_send)
            {
                switch ((ret = bb->send(server_id, msg)))
                {
                    case BUSYBEE_SUCCESS:
                        break;
                    case BUSYBEE_SHUTDOWN:
                    case BUSYBEE_POLLFAILED:
                    case BUSYBEE_DISRUPTED:
                    case BUSYBEE_ADDFDFAIL:
                    case BUSYBEE_TIMEOUT:
                    case BUSYBEE_EXTERNAL:
                    case BUSYBEE_INTERRUPTED:
                    default:
                        po6::threads::mutex::hold hold(io);
                        std::cerr << "send error: " << ret << std::endl;
                }
            }

            if (do_recv)
            {
                switch ((ret = bb->recv(&server_id, &msg)))
                {
                    case BUSYBEE_SUCCESS:
                        break;
                    case BUSYBEE_SHUTDOWN:
                    case BUSYBEE_POLLFAILED:
                    case BUSYBEE_DISRUPTED:
                    case BUSYBEE_ADDFDFAIL:
                    case BUSYBEE_TIMEOUT:
                    case BUSYBEE_EXTERNAL:
                    case BUSYBEE_INTERRUPTED:
                    default:
                        po6::threads::mutex::hold hold(io);
                        std::cerr << "recv error: " << ret << std::endl;
                }
            }

            ++ops;
        }
    }

    return EXIT_SUCCESS;
}

#endif // busybee_bench_h_
