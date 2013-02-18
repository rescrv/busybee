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

// STL
#include <tr1/memory>

// po6
#include <po6/threads/thread.h>

// BusyBee
#include "bench.h"
#include "busybee_mta.h"

typedef std::tr1::shared_ptr<po6::threads::thread> thread_ptr;

static int
usage()
{
    std::cerr << "look at the source (if you need this tool, you need to know what it does)\n";
    return EXIT_FAILURE;
}

int
main(int argc, const char* argv[])
{
    if (argc < 2)
    {
        return usage();
    }

    size_t num_threads = atoi(argv[1]);
    argc -= 2;
    argv += 2;
    membership_list ml;
    po6::net::location bind_to;
    uint64_t server_id;

    if (!parse_args(argc, argv, &ml, &bind_to, &server_id))
    {
        return usage();
    }

    benchmapper bm(&ml);
    busybee_mta bb(&bm, bind_to, server_id, num_threads);
    po6::threads::mutex io;
    std::vector<thread_ptr> threads;

    while (threads.size() < num_threads)
    {
        std::tr1::function<void (busybee_mta* bb, po6::threads::mutex* io,
                                 const membership_list& ml,
                                 bool do_send, bool do_recv)> bench(benchmark<busybee_mta>);
        thread_ptr t(new po6::threads::thread(std::tr1::bind(bench, &bb, &io, ml, true, true)));
        t->start();
        threads.push_back(t);
    }

    for (size_t i = 0; i < threads.size(); ++i)
    {
        threads[i]->join();
    }

    return EXIT_SUCCESS;
}
