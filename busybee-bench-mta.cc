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

// C
#include <cstdlib>

// C++
#include <iostream>

// STL
#include <tr1/memory>

// po6
#include <po6/threads/thread.h>

// BusyBee
#include "busybee_mta.h"

#include "bench.h"

typedef std::tr1::shared_ptr<po6::threads::thread> thread_ptr;

int
main(int argc, const char* argv[])
{
    if (argc % 2 != 0 || argc < 6)
    {
        std::cerr << "usage: " << argv[0] << " <bind ip> <bind port> "
                  << "[<connect ip> <connect port> ...]" << std::endl;
        return EXIT_FAILURE;
    }

    size_t numthreads = atoi(argv[1]);
    po6::net::location us(argv[2], atoi(argv[3]));
    std::vector<po6::net::location> others;

    for (int i = 4; i < argc; i += 2)
    {
        po6::net::location loc(argv[i], atoi(argv[i + 1]));
        others.push_back(loc);
    }

    std::vector<thread_ptr> threads;
    busybee_mta busybee(us.address, us.port, 0, 1);
    po6::threads::mutex io;

    while (threads.size() < numthreads)
    {
        std::tr1::function<int (busybee_mta* bb, po6::threads::mutex* io,
                                const std::vector<po6::net::location>& others,
                                bool do_send, bool do_recv)> bench(benchmark<busybee_mta>);
        thread_ptr t(new po6::threads::thread(std::tr1::bind(bench, &busybee, &io, others, true, true)));
        t->start();
        threads.push_back(t);

    }

    for (size_t i = 0; i < threads.size(); ++i)
    {
        threads[i]->join();
    }

    return 0;
}
