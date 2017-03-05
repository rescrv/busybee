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

// Linux
#include <sys/epoll.h>

// BusyBee
#include "busybee-internal.h"

BEGIN_BUSYBEE_NAMESPACE

class epoll_poller : public poller
{
    public:
        epoll_poller();
        virtual ~epoll_poller() throw ();

    public:
        virtual busybee_returncode init();
        virtual busybee_returncode add(int fd, uint32_t events);
        virtual busybee_returncode del(int fd);
        virtual busybee_returncode poll(int timeout, int* fd, uint32_t* events);
        virtual int poll_fd();

    private:
        uint32_t bb2epoll(uint32_t events);
        uint32_t epoll2bb(uint32_t events);

    private:
        po6::io::fd m_epfd;
        sigset_t m_sigmask;
};

poller*
new_poller()
{
    return new epoll_poller();
}

END_BUSYBEE_NAMESPACE

using busybee::epoll_poller;

epoll_poller :: epoll_poller()
    : m_epfd()
    , m_sigmask()
{
    sigemptyset(&m_sigmask);
}

epoll_poller :: ~epoll_poller() throw ()
{
}

busybee_returncode
epoll_poller :: init()
{
    m_epfd = epoll_create(64);
    return m_epfd.get() >= 0 ? BUSYBEE_SUCCESS : BUSYBEE_SEE_ERRNO;
}

busybee_returncode
epoll_poller :: add(int fd, uint32_t events)
{
    epoll_event ee;
    memset(&ee, 0, sizeof(ee));
    ee.data.fd = fd;
    ee.events = bb2epoll(events);
    return epoll_ctl(m_epfd.get(), EPOLL_CTL_ADD, fd, &ee) == 0
         ? BUSYBEE_SUCCESS : BUSYBEE_SEE_ERRNO;
}

busybee_returncode
epoll_poller :: del(int fd)
{
    epoll_event ee;
    memset(&ee, 0, sizeof(ee));
    ee.data.fd = fd;
    return epoll_ctl(m_epfd.get(), EPOLL_CTL_DEL, fd, &ee) == 0
         ? BUSYBEE_SUCCESS : BUSYBEE_SEE_ERRNO;
}

busybee_returncode
epoll_poller :: poll(int timeout, int* fd, uint32_t* events)
{
    epoll_event ee;
    memset(&ee, 0, sizeof(ee));
    int ret = epoll_pwait(m_epfd.get(), &ee, 1, timeout, &m_sigmask);
    *fd = ee.data.fd;
    *events = epoll2bb(ee.events);

    switch (ret)
    {
        case 1: return BUSYBEE_SUCCESS;
        case 0: return BUSYBEE_TIMEOUT;
        default: return errno == EINTR ? BUSYBEE_INTERRUPTED
                                       : BUSYBEE_SEE_ERRNO;
    }
}

int
epoll_poller :: poll_fd()
{
    return m_epfd.get();
}

uint32_t
epoll_poller :: bb2epoll(uint32_t bbevents)
{
    uint32_t epevents = 0;
    if ((bbevents & BBPOLLIN)) epevents |= EPOLLIN|EPOLLRDHUP;
    if ((bbevents & BBPOLLOUT)) epevents |= EPOLLOUT;
    if ((bbevents & BBPOLLET)) epevents |= EPOLLET;
    return epevents;
}

uint32_t
epoll_poller :: epoll2bb(uint32_t epevents)
{
    uint32_t bbevents = 0;
    if ((epevents & EPOLLIN)) bbevents |= BBPOLLIN;
    if ((epevents & EPOLLRDHUP)) bbevents |= BBPOLLIN;
    if ((epevents & EPOLLOUT)) bbevents |= BBPOLLOUT;
    if ((epevents & EPOLLET)) bbevents |= BBPOLLET;
    return bbevents;
}
