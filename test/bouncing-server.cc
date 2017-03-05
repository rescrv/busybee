// Copyright (c) 2017, Robert Escriva, Cornell University
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
#include <signal.h>

// STL
#include <memory>
#include <vector>

// po6
#include <po6/threads/thread.h>
#include <po6/time.h>

// e
#include <e/compat.h>
#include <e/garbage_collector.h>
#include <e/popt.h>

// BusyBee
#include <busybee.h>
#include "test/bouncing-common.h"

#define XSTR(x) #x
#define STR(x) XSTR(x)
#define ABORT_CASE(x) case (x): std::cerr << XSTR(x) << std::endl; abort();

void
worker(e::garbage_collector* gc, busybee_server* srv)
{
    e::garbage_collector::thread_state ts;
    gc->register_thread(&ts);
    bool shutdown = false;

    while (!shutdown)
    {
        uint64_t server_id;
        std::auto_ptr<e::buffer> msg;

        switch (srv->recv(&ts, -1, &server_id, &msg))
        {
            case BUSYBEE_SUCCESS: break;
            case BUSYBEE_SHUTDOWN: shutdown = true; continue;
            case BUSYBEE_DISRUPTED: continue;
            ABORT_CASE(BUSYBEE_TIMEOUT);
            ABORT_CASE(BUSYBEE_EXTERNAL);
            ABORT_CASE(BUSYBEE_INTERRUPTED);
            ABORT_CASE(BUSYBEE_SEE_ERRNO);
            default:
                abort();
        }

        // XXX random
        // XXX adjust ttl
        assert(msg.get());

        switch (srv->send(server_id, msg))
        {
            case BUSYBEE_SUCCESS: break;
            case BUSYBEE_SHUTDOWN: shutdown = true; continue;
            ABORT_CASE(BUSYBEE_DISRUPTED);
            ABORT_CASE(BUSYBEE_TIMEOUT);
            ABORT_CASE(BUSYBEE_EXTERNAL);
            ABORT_CASE(BUSYBEE_INTERRUPTED);
            ABORT_CASE(BUSYBEE_SEE_ERRNO);
            default:
                abort();
        }
    }

    gc->deregister_thread(&ts);
}

int
main(int argc, const char* argv[])
{
    long num_threads = 1;
    const char* bind_address = "127.0.0.1";
    long bind_port = 2000;
    sigset_t ss;

#if 0
    if (sigfillset(&ss) < 0 ||
        sigprocmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        std::cerr << "could not block signals\n";
        return EXIT_FAILURE;
    }
#endif

    e::argparser ap;
    ap.autohelp();
    ap.arg().name('t', "threads")
            .description("number of threads (default: 1)")
            .as_long(&num_threads);
    ap.arg().name('a', "address")
            .description("bind at address (default: 127.0.0.1)")
            .as_string(&bind_address);
    ap.arg().name('p', "port")
            .description("bind to port (default: 2000)")
            .as_long(&bind_port);

    if (!ap.parse(argc, argv) || ap.args_sz() < 1)
    {
        return EXIT_FAILURE;
    }

    po6::net::location bind_to;

    if (!bind_to.set(bind_address, bind_port))
    {
        return EXIT_FAILURE;
    }

    controller c(ap.args(), ap.args_sz());

    if (c.reverse_lookup(bind_to) == 0)
    {
        return EXIT_FAILURE;
    }

    e::garbage_collector gc;
    std::auto_ptr<busybee_server> srv(busybee_server::create(&c, c.reverse_lookup(bind_to), bind_to, &gc));
    assert(srv.get());
    std::vector<e::compat::shared_ptr<po6::threads::thread> > threads;

    for (long i = 0; i < num_threads; ++i)
    {
        using namespace po6::threads;
        e::compat::shared_ptr<thread> t(new thread(make_func(worker, &gc, srv.get())));
        threads.push_back(t);
        t->start();
    }

    pause();
    //srv->shutdown();

    for (size_t i = 0; i < threads.size(); ++i)
    {
        threads[i]->join();
    }
}
