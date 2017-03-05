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

#ifndef busybee_internal_h_
#define busybee_internal_h_

// STL
#include <iostream>
#include <sstream>

// BusyBee
#include <busybee.h>

#define BEGIN_BUSYBEE_NAMESPACE \
    namespace busybee __attribute__ ((visibility ("hidden"))) {
#define END_BUSYBEE_NAMESPACE }

#define BBPOLLIN 1
#define BBPOLLOUT 2
#define BBPOLLET 4

// busybee_returncode starts at 4608 so grow downward for internal return values
#define INTERNAL_NEWLY_IDENTIFIED 4607
#define INTERNAL_TIME_TO_CLOSE    4606

BEGIN_BUSYBEE_NAMESPACE

struct debug_printer
{
    debug_printer(const char* file, unsigned line)
        : ostr()
    {
        ostr << file << ":" << line << " ";
    }
    ~debug_printer()
    {
        ostr << "\n";
        std::cerr << ostr.str();
    }

    std::ostringstream ostr;
};

template<typename T>
debug_printer&
operator << (debug_printer& dp, const T& t)
{
    dp.ostr << t;
    return dp;
}

#ifdef BUSYBEE_DEBUG
#define DEBUG(X) \
    do { \
        debug_printer dp(__FILE__, __LINE__); \
        dp << X; \
    } while (0);
#else
#define DEBUG(X) \
    do { \
    } while (0);
#endif

class poller
{
    public:
        poller();
        virtual ~poller() throw ();

    public:
        virtual busybee_returncode init() = 0;
        virtual busybee_returncode add(int fd, uint32_t events) = 0;
        virtual busybee_returncode del(int fd) = 0;
        virtual busybee_returncode poll(int timeout, int* fd, uint32_t* events) = 0;
        virtual int poll_fd() = 0;
};

poller* new_poller();

END_BUSYBEE_NAMESPACE

#endif // busybee_internal_h_
