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

// po6
#include <po6/time.h>

// e
#include <e/popt.h>

// BusyBee
#include <busybee.h>
#include "test/bouncing-common.h"

#define XSTR(x) #x
#define STR(x) XSTR(x)
#define ABORT_CASE(x) case (x): std::cerr << XSTR(x) << std::endl; abort();

int
main(int argc, const char* argv[])
{
    long messages = 1000;
    long payload = 0;
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
    ap.arg().name('m', "messages")
            .description("number of messages to send (default: 1000)")
            .as_long(&messages);
    ap.arg().name('p', "payload")
            .description("payload size of messages (default: 0)")
            .as_long(&payload);

    if (!ap.parse(argc, argv) || ap.args_sz() % 2 != 0)
    {
        return EXIT_FAILURE;
    }

    controller c(ap.args(), ap.args_sz());
    std::auto_ptr<busybee_client> cl(busybee_client::create(&c));
    assert(cl.get());

    for (long i = 0; i < messages; ++i)
    {
        const uint64_t send_id = c.server_from_index(i);
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + 2 * sizeof(uint64_t)
                        + sizeof(uint8_t)
                        + payload;
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE)
            << send_id << po6::monotonic_time() << uint8_t(255);
        msg->resize(sz);

        switch (cl->send(send_id, msg))
        {
			case BUSYBEE_SUCCESS: break;
			ABORT_CASE(BUSYBEE_SHUTDOWN);
			ABORT_CASE(BUSYBEE_DISRUPTED);
			ABORT_CASE(BUSYBEE_TIMEOUT);
			ABORT_CASE(BUSYBEE_EXTERNAL);
			ABORT_CASE(BUSYBEE_INTERRUPTED);
			ABORT_CASE(BUSYBEE_SEE_ERRNO);
            default:
                abort();
        }

        uint64_t recv_id;

        switch (cl->recv(-1, &recv_id, &msg))
        {
			case BUSYBEE_SUCCESS: break;
			ABORT_CASE(BUSYBEE_SHUTDOWN);
			ABORT_CASE(BUSYBEE_DISRUPTED);
			ABORT_CASE(BUSYBEE_TIMEOUT);
			ABORT_CASE(BUSYBEE_EXTERNAL);
			ABORT_CASE(BUSYBEE_INTERRUPTED);
			ABORT_CASE(BUSYBEE_SEE_ERRNO);
            default:
                abort();
        }

        if (send_id != recv_id)
        {
            abort();
        }
    }
}
