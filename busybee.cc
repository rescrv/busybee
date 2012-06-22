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

#if HAVE_CONFIG_H
# include "config.h"
#endif

#define __STDC_LIMIT_MACROS

// Linux
#include <sys/epoll.h>

// po6
#include <po6/net/socket.h>
#include <po6/threads/mutex.h>

// e
#include <e/atomic.h>
#include <e/endian.h>
#include <e/guard.h>
#include <e/nonblocking_bounded_fifo.h>
#include <e/pow2.h>

// BusyBee
#include "busybee_constants.h"

// BusyBee Feature Declaration
#ifdef BUSYBEE_MTA
#ifdef BUSYBEE_TYPE
#error BUSYBEE_TYPE defined already
#endif
#define BUSYBEE_TYPE mta
#include "busybee_mta.h"
#ifndef BUSYBEE_MULTITHREADED
#define BUSYBEE_MULTITHREADED
#endif
#ifdef BUSYBEE_SINGLETHREADED
#undef BUSYBEE_SINGLETHREADED
#endif
#ifndef BUSYBEE_ACCEPT
#define BUSYBEE_ACCEPT
#endif
#ifdef BUSYBEE_NOACCEPT
#undef BUSYBEE_NOACCEPT
#endif
#endif

#ifdef BUSYBEE_STA
#ifdef BUSYBEE_TYPE
#error BUSYBEE_TYPE defined already
#endif
#define BUSYBEE_TYPE sta
#include "busybee_sta.h"
#ifndef BUSYBEE_SINGLETHREADED
#define BUSYBEE_SINGLETHREADED
#endif
#ifdef BUSYBEE_MULTITHREADED
#undef BUSYBEE_MULTITHREADED
#endif
#ifndef BUSYBEE_ACCEPT
#define BUSYBEE_ACCEPT
#endif
#ifdef BUSYBEE_NOACCEPT
#undef BUSYBEE_NOACCEPT
#endif
#endif

#ifdef BUSYBEE_ST
#ifdef BUSYBEE_TYPE
#error BUSYBEE_TYPE defined already
#endif
#define BUSYBEE_TYPE st
#include "busybee_st.h"
#ifndef BUSYBEE_SINGLETHREADED
#define BUSYBEE_SINGLETHREADED
#endif
#ifdef BUSYBEE_MULTITHREADED
#undef BUSYBEE_MULTITHREADED
#endif
#ifndef BUSYBEE_NOACCEPT
#define BUSYBEE_NOACCEPT
#endif
#ifdef BUSYBEE_ACCEPT
#undef BUSYBEE_ACCEPT
#endif
#endif

#define _CONCAT(X, Y) X ## Y
#define CONCAT(X, Y) _CONCAT(X, Y)

#define CLASSNAME CONCAT(busybee_, BUSYBEE_TYPE)

// Some Constants
#define IO_BLOCKSIZE 4096
#define NUM_MSGS_PER_RECV (IO_BLOCKSIZE / BUSYBEE_HEADER_SIZE)

///////////////////////////////// Channel Class ////////////////////////////////

class CLASSNAME::channel
{
    public:
        channel();
        ~channel() throw ();

    public:
        void reset();
        void reset(po6::net::socket* soc);

    public:
        po6::net::socket soc; // The socket over which we are communicating.
        po6::net::location loc; // A cached soc.getpeername.
        uint32_t tag;
        uint32_t nexttag;
        e::nonblocking_bounded_fifo<std::auto_ptr<e::buffer> > outgoing; // Messages buffered for writing.
        std::auto_ptr<e::buffer> outnow; // The current message we are writing to the network.
        e::slice outprogress; // A pointer into what we've written so far.
        std::auto_ptr<e::buffer> inprogress; // When reading from the network, we buffer partial reads here.
        size_t inoffset; // How much we've buffered in inbuffer.
        char inbuffer[sizeof(uint32_t)]; // We buffer reads here when we haven't read enough to set the size of inprogress.
        uint32_t events; // Events that were collected while holding the lock.
#ifdef BUSYBEE_MULTITHREADED
        po6::threads::mutex mtx; // Anyone touching the socket should hold this.
#endif // BUSYBEE_MULTITHREADED

    private:
        channel(const channel& other);

    private:
        channel& operator = (const channel& rhs);
};

CLASSNAME :: channel :: channel()
    : soc()
    , loc()
    , tag(0)
    , nexttag(1)
    , outgoing(512)
    , outnow()
    , outprogress()
    , inprogress()
    , inoffset(0)
    , inbuffer()
    , events(0)
#ifdef BUSYBEE_MULTITHREADED
    , mtx()
#endif // BUSYBEE_MULTITHREADED
{
}

CLASSNAME :: channel :: ~channel() throw ()
{
}

void
CLASSNAME :: channel :: reset()
{
    try
    {
        soc.shutdown(SHUT_RDWR);
    }
    catch (po6::error& e)
    {
    }

    try
    {
        soc.close();
    }
    catch (po6::error& e)
    {
    }

    loc = po6::net::location();
    tag = 0;

    while (outgoing.pop(&outnow))
        ;

    outnow.reset();
    outprogress = e::slice();
    inprogress.reset();
    inoffset = 0;
#ifdef BUSYBEE_MULTITHREADED
    e::atomic::store_32_nobarrier(&events, 0);
#endif // BUSYBEE_MULTITHREADED
}

void
CLASSNAME :: channel :: reset(po6::net::socket* _soc)
{
    reset();
    soc.swap(_soc);
    loc = soc.getpeername();
}

///////////////////////////////// Message Class ////////////////////////////////

struct CLASSNAME::message
{
    message() : loc(), buf() {}
    ~message() throw () {}

    po6::net::location loc;
    std::auto_ptr<e::buffer> buf;
};

///////////////////////////////// Pending Class ////////////////////////////////

struct CLASSNAME::pending
{
    pending() : fd(), events() {}
    pending(int f, uint32_t e) : fd(f), events(e) {}
    ~pending() throw () {}

    int fd;
    uint32_t events;
};

///////////////////////////////// BusyBee Class ////////////////////////////////

#ifdef BUSYBEE_MTA
busybee_mta :: busybee_mta(const po6::net::ipaddr& ip,
                           in_port_t incoming,
                           in_port_t outgoing,
                           size_t num_threads)
    : m_epoll(epoll_create(1 << 16))
    , m_listen(ip.family(), SOCK_STREAM, IPPROTO_TCP)
    , m_bindto(ip, outgoing)
    , m_connectlocks(num_threads * 4)
    , m_locations(16)
    , m_incoming(e::next_pow2(num_threads * NUM_MSGS_PER_RECV))
    , m_channels(sysconf(_SC_OPEN_MAX))
    , m_postponed(e::next_pow2(m_channels.size() * 2))
    , m_pause_barrier(num_threads)
    , m_timeout(-1)
    , m_external_fd(0)
    , m_external_events(0)
    , m_shutdown(false)
{

    if (m_epoll.get() < 0)
    {
        throw po6::error(errno);
    }

    for (size_t i = 0; i < m_channels.size(); ++i)
    {
        m_channels[i].reset(new channel());
    }

    // Enable other hosts to connect to us.
    m_listen.set_reuseaddr();
    m_listen.bind(po6::net::location(ip, incoming));
    m_listen.listen(sysconf(_SC_OPEN_MAX));
    m_listen.set_nonblocking();
    channel* chan;
    uint32_t chantag;

    switch (get_channel(m_listen.getsockname(), &chan, &chantag))
    {
        case BUSYBEE_SUCCESS:
            break;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_QUEUED:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_DISCONNECT:
        case BUSYBEE_CONNECTFAIL:
        case BUSYBEE_ADDFDFAIL:
        case BUSYBEE_BUFFERFULL:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        default:
            throw std::runtime_error("Could not create connection to self.");
    }

    m_bindto = chan->soc.getsockname();

    epoll_event ee;
    ee.data.fd = m_listen.get();
    ee.events = EPOLLIN;

    if (epoll_ctl(m_epoll.get(), EPOLL_CTL_ADD, m_listen.get(), &ee) < 0)
    {
        throw po6::error(errno);
    }
}
#endif // mta

#ifdef BUSYBEE_STA
busybee_sta :: busybee_sta(const po6::net::ipaddr& ip,
                           in_port_t incoming,
                           in_port_t outgoing)
    : m_epoll(epoll_create(1 << 16))
    , m_listen(ip.family(), SOCK_STREAM, IPPROTO_TCP)
    , m_bindto(ip, outgoing)
    , m_locations(16)
    , m_incoming(e::next_pow2(NUM_MSGS_PER_RECV))
    , m_channels(sysconf(_SC_OPEN_MAX))
    , m_postponed(e::next_pow2(m_channels.size() * 2))
    , m_timeout(-1)
    , m_external_fd(0)
    , m_external_events(0)
{

    if (m_epoll.get() < 0)
    {
        throw po6::error(errno);
    }

    for (size_t i = 0; i < m_channels.size(); ++i)
    {
        m_channels[i].reset(new channel());
    }

    // Enable other hosts to connect to us.
    m_listen.set_reuseaddr();
    m_listen.bind(po6::net::location(ip, incoming));
    m_listen.listen(sysconf(_SC_OPEN_MAX));
    m_listen.set_nonblocking();
    channel* chan;
    uint32_t chantag;

    switch (get_channel(m_listen.getsockname(), &chan, &chantag))
    {
        case BUSYBEE_SUCCESS:
            break;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_QUEUED:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_DISCONNECT:
        case BUSYBEE_CONNECTFAIL:
        case BUSYBEE_ADDFDFAIL:
        case BUSYBEE_BUFFERFULL:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        default:
            throw std::runtime_error("Could not create connection to self.");
    }

    m_bindto = chan->soc.getsockname();

    epoll_event ee;
    ee.data.fd = m_listen.get();
    ee.events = EPOLLIN;

    if (epoll_ctl(m_epoll.get(), EPOLL_CTL_ADD, m_listen.get(), &ee) < 0)
    {
        throw po6::error(errno);
    }
}
#endif // sta

#ifdef BUSYBEE_ST
busybee_st :: busybee_st()
    : m_epoll(epoll_create(1 << 16))
    , m_locations(16)
    , m_incoming(e::next_pow2(NUM_MSGS_PER_RECV))
    , m_channels(sysconf(_SC_OPEN_MAX))
    , m_postponed(e::next_pow2(m_channels.size() * 2))
    , m_timeout(-1)
    , m_external_fd(0)
    , m_external_events(0)
{

    if (m_epoll.get() < 0)
    {
        throw po6::error(errno);
    }

    for (size_t i = 0; i < m_channels.size(); ++i)
    {
        m_channels[i].reset(new channel());
    }
}
#endif // st

CLASSNAME :: ~CLASSNAME() throw ()
{
#ifdef BUSYBEE_MULTITHREADED
    if (!m_shutdown)
    {
        shutdown();
    }
#endif // BUSYBEE_MULTITHREADED
}

#ifdef BUSYBEE_MULTITHREADED
void
CLASSNAME :: shutdown()
{
    m_shutdown = true;
    m_pause_barrier.shutdown();
}

void
CLASSNAME :: pause()
{
    m_pause_barrier.pause();
}

void
CLASSNAME :: unpause()
{
    m_pause_barrier.unpause();
}
#endif // BUSYBEE_MULTITHREADED

void
CLASSNAME :: set_timeout(int timeout)
{
    m_timeout = timeout;
}

int
CLASSNAME :: add_external_fd(int fd, uint32_t events)
{
    assert(fd > 0);
#ifdef BUSYBEE_MULTITHREADED
    po6::threads::mutex::hold hold(&m_channels[fd]->mtx);
#endif // BUSYBEE_MULTITHREADED
    work_close(m_channels[fd].get());
    m_channels[fd]->tag = -1;
    epoll_event ee;
    ee.data.fd = fd;
    ee.events = events;
    return epoll_ctl(m_epoll.get(), EPOLL_CTL_ADD, fd, &ee);
}

void
CLASSNAME :: get_last_external(int* fd, uint32_t* events)
{
    *fd = m_external_fd;
    *events = m_external_events;
}

busybee_returncode
CLASSNAME :: drop(const po6::net::location& to)
{
    channel* chan;
    uint32_t chantag;
    busybee_returncode res = get_channel(to, &chan, &chantag);

    // Getting a channel fails
    if (res != BUSYBEE_SUCCESS)
    {
        return res;
    }

#ifdef BUSYBEE_MULTITHREADED
    po6::threads::mutex::hold hold(&chan->mtx);
#endif // BUSYBEE_MULTITHREADED

    if (chantag != chan->tag)
    {
        return BUSYBEE_SUCCESS;
    }

    work_close(chan);
    return BUSYBEE_SUCCESS;
}

busybee_returncode
CLASSNAME :: send(const po6::net::location& to,
                  std::auto_ptr<e::buffer> msg)
{
    assert(msg->size() >= BUSYBEE_HEADER_SIZE);
    assert(msg->size() <= BUSYBEE_MAX_MSG_SIZE);
    channel* chan;
    uint32_t chantag;
    busybee_returncode res = get_channel(to, &chan, &chantag);

    // Getting a channel fails
    if (res != BUSYBEE_SUCCESS)
    {
        return res;
    }

    // Pack the size into the header
    *msg << static_cast<uint32_t>(msg->size());

    if (!chan->outgoing.push(msg))
    {
        return BUSYBEE_BUFFERFULL;
    }

#ifdef BUSYBEE_MULTITHREADED
    // Acquire the lock to call work_write (if multithreaded)
    if (!chan->mtx.trylock())
    {
        e::atomic::or_32_nobarrier(&chan->events, EPOLLOUT);

        if (!chan->mtx.trylock())
        {
            return BUSYBEE_QUEUED;
        }

        uint32_t events = EPOLLOUT;
        e::atomic::and_32_nobarrier(&chan->events, ~events);
    }

    // We've acquired the lock.  This means that we must always unlock, and
    // then postpone events that were generated while we held the lock.  The
    // destructors run in reverse order, so the channel will be unlocked,
    // and then postponed.
#endif // BUSYBEE_MULTITHREADED
    e::guard g1 = e::makeobjguard(*this, & CLASSNAME ::postpone_event, chan);
    g1.use_variable();
#ifdef BUSYBEE_MULTITHREADED
    e::guard g2 = e::makeobjguard(chan->mtx, &po6::threads::mutex::unlock);
    g2.use_variable();
#endif // BUSYBEE_MULTITHREADED

    if (chantag != chan->tag)
    {
        return BUSYBEE_DISCONNECT;
    }

    if (!work_write(chan, &res))
    {
        return res;
    }

    return BUSYBEE_SUCCESS;
}

busybee_returncode
CLASSNAME :: recv(po6::net::location* from,
                  std::auto_ptr<e::buffer>* msg)
{
    while (true)
    {
#ifdef BUSYBEE_MULTITHREADED
        m_pause_barrier.pausepoint();

        if (m_shutdown)
        {
            return BUSYBEE_SHUTDOWN;
        }
#endif // BUSYBEE_MULTITHREADED

        message m;

        if (m_incoming.pop(&m))
        {
            *from = m.loc;
            *msg = m.buf;
            return BUSYBEE_SUCCESS;
        }

        int status;
        int fd;
        uint32_t events;

        if ((status = receive_event(&fd, &events)) <= 0)
        {
            if (status < 0 &&
                    errno != EAGAIN &&
                    errno != EINTR &&
                    errno != EWOULDBLOCK)
            {
                return BUSYBEE_POLLFAILED;
            }

            if (status >= 0 && errno == EINTR && m_timeout >= 0)
            {
                return BUSYBEE_TIMEOUT;
            }

            continue;
        }

#ifdef BUSYBEE_ACCEPT
        // Ignore file descriptors opened elsewhere.
        if (fd == m_listen.get())
        {
            if ((events & EPOLLIN))
            {
                work_accept();
            }

            continue;
        }
#endif

#ifdef BUSYBEE_MULTITHREADED
        // Acquire the lock to call work_write (if multithreaded)
        if (!m_channels[fd]->mtx.trylock())
        {
            e::atomic::or_32_nobarrier(&m_channels[fd]->events, events);

            if (!m_channels[fd]->mtx.trylock())
            {
                continue;
            }
        }

        e::atomic::and_32_nobarrier(&m_channels[fd]->events, ~events);

        // We've acquired the lock.  This means that we must always unlock, and
        // then postpone events that were generated while we held the lock.  The
        // destructors run in reverse order, so the channel will be unlocked,
        // and then postponed.
#endif // BUSYBEE_MULTITHREADED
        // We always want to postpone events because they may be generated in
        // any kind of loop.
        e::guard g1 = e::makeobjguard(*this, & CLASSNAME ::postpone_event, m_channels[fd].get());
        g1.use_variable();
#ifdef BUSYBEE_MULTITHREADED
        e::guard g2 = e::makeobjguard(m_channels[fd]->mtx, &po6::threads::mutex::unlock);
        g2.use_variable();
#endif // BUSYBEE_MULTITHREADED

        if (m_channels[fd]->tag == 0)
        {
            continue;
        }

        // Handle read I/O.
        if ((events & EPOLLIN))
        {
            busybee_returncode res;

            if (work_read(m_channels[fd].get(), from, msg, &res))
            {
                return res;
            }
        }

        // Handle write I/O.
        if ((events & EPOLLOUT))
        {
            busybee_returncode res;

            if (!work_write(m_channels[fd].get(), &res))
            {
                *from = m_channels[fd]->loc;
                return res;
            }
        }

        // Close the connection on error or hangup.
        if ((events & EPOLLERR) || (events & EPOLLHUP))
        {
            *from = m_channels[fd]->loc;
            work_close(m_channels[fd].get());
            return BUSYBEE_DISCONNECT;
        }
    }
}

#ifdef BUSYBEE_ACCEPT
bool
CLASSNAME :: deliver(const po6::net::location& from,
                     std::auto_ptr<e::buffer> msg)
{
    message m;
    m.loc = from;
    m.buf = msg;
    return m_incoming.push(m);
}

po6::net::location
CLASSNAME :: inbound()
{
    return m_listen.getsockname();
}

po6::net::location
CLASSNAME :: outbound()
{
    return m_bindto;
}
#endif // BUSYBEE_ACCEPT

busybee_returncode
CLASSNAME :: get_channel(const po6::net::location& to, channel** chan, uint32_t* chantag)
{
    unsigned count = 0;
    std::pair<int, uint32_t> locid;

    while (true)
    {
        if (m_locations.lookup(to, &locid))
        {
            *chan = m_channels[locid.first].get();
            *chantag = locid.second;
            return BUSYBEE_SUCCESS;
        }
        else
        {
            try
            {
                po6::net::socket soc(to.address.family(), SOCK_STREAM, IPPROTO_TCP);
#ifdef BUSYBEE_MULTITHREADED
                po6::threads::mutex::hold chanhold(&m_channels[soc.get()]->mtx);
                uint64_t hash = po6::net::location::hash(to);
                e::striped_lock<po6::threads::mutex>::hold lochold(&m_connectlocks, hash);
#endif
                soc.set_reuseaddr();
#ifdef BUSYBEE_ACCEPT
                soc.bind(m_bindto);
#endif // BUSYBEE_ACCEPT
                soc.connect(to);
                return get_channel(&soc, chan, chantag);
            }
            catch (po6::error& e)
            {
                if (count != 0)
                {
                    return BUSYBEE_CONNECTFAIL;
                }

                ++ count;
            }
        }
    }
}

busybee_returncode
CLASSNAME :: get_channel(po6::net::socket* soc, channel** ret, uint32_t* chantag)
{
    int fd = soc->get();
    assert(fd >= 0);

#ifdef HAVE_SO_NOSIGPIPE
    int sigpipeopt = 1;
    soc->set_sockopt(SOL_SOCKET, SO_NOSIGPIPE, &sigpipeopt, sizeof(sigpipeopt));
#endif // HAVE_SO_NOSIGPIPE

    soc->set_nonblocking();
    soc->set_tcp_nodelay();
    *ret = m_channels[fd].get();
    m_channels[fd]->reset(soc);
    std::pair<int, uint32_t> locid(fd, m_channels[fd]->nexttag);

    if (m_locations.insert(m_channels[fd]->loc, locid))
    {
        if (add_descriptor(fd) < 0)
        {
            m_locations.remove(m_channels[fd]->loc);
            return BUSYBEE_ADDFDFAIL;
        }

        m_channels[fd]->tag = m_channels[fd]->nexttag;
        ++m_channels[fd]->nexttag;
        *chantag = m_channels[fd]->tag;
        e::atomic::or_32_nobarrier(&m_channels[fd]->events, EPOLLIN);
        postpone_event(m_channels[fd].get());
        return BUSYBEE_SUCCESS;
    }

    abort();
}

int
CLASSNAME :: add_descriptor(int fd)
{
    epoll_event ee;
    ee.data.fd = fd;
    ee.events = EPOLLIN|EPOLLOUT|EPOLLET;
    return epoll_ctl(m_epoll.get(), EPOLL_CTL_ADD, fd, &ee);
}

void
CLASSNAME :: postpone_event(channel* chan)
{
    int fd = chan->soc.get();

    if (fd < 0)
    {
        return;
    }

    uint32_t events = e::atomic::exchange_32_nobarrier(&chan->events, 0);

    if (events)
    {
        pending p(fd, events);
        m_postponed.push(p);
    }
}

int
CLASSNAME :: receive_event(int*fd, uint32_t* events)
{
    pending p;

    if (m_postponed.pop(&p))
    {
        *fd = p.fd;
        *events = p.events;
        return 1;
    }

    epoll_event ee;
    int ret = epoll_wait(m_epoll.get(), &ee, 1, m_timeout < 0 ? 50 : m_timeout);
    *fd = ee.data.fd;
    *events = ee.events;
    return ret;
}

#ifdef BUSYBEE_ACCEPT
int
CLASSNAME :: work_accept()
{
    try
    {
        po6::net::socket soc;
        m_listen.accept(&soc);
        channel* chan;
        uint32_t chantag;
        get_channel(&soc, &chan, &chantag);
        assert(chantag == chan->tag);
        return chan->soc.get();
    }
    catch (po6::error& e)
    {
        if (e != EAGAIN && e != EINTR && e != EWOULDBLOCK)
        {
            return -1;
        }
    }

    return -1;
}
#endif // BUSYBEE_ACCEPT

void
CLASSNAME:: work_close(channel* chan)
{
    int fd = chan->soc.get();

    if (fd < 0)
    {
        return;
    }

#ifdef BUSYBEE_MULTITHREADED
    uint64_t hash = po6::net::location::hash(chan->loc);
    e::striped_lock<po6::threads::mutex>::hold hold(&m_connectlocks, hash);
#endif
    bool rem = m_locations.remove(chan->loc);
    assert(rem);
    m_channels[fd]->reset();
}

bool
CLASSNAME :: work_read(channel* chan,
                       po6::net::location* from,
                       std::auto_ptr<e::buffer>* msg,
                       busybee_returncode* res)
{
    assert(chan->inoffset <= 4);

    if (chan->soc.get() < 0)
    {
        return false;
    }

    uint8_t buffer[IO_BLOCKSIZE];
    ssize_t rem;

    // Restore leftovers from last time.
    if (chan->inoffset)
    {
        memmove(buffer, chan->inbuffer, chan->inoffset);
    }

    // Read into our temporary local buffer.
    int flags = 0;
#ifdef HAVE_MSG_NOSIGNAL
    flags |= MSG_NOSIGNAL;
#endif // HAVE_MSG_NOSIGNAL
    rem = chan->soc.recv(buffer + chan->inoffset,
                         IO_BLOCKSIZE - chan->inoffset,
                         flags);

    // If we are done with this socket (error or closed).
    if ((rem < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
            || rem == 0)
    {
        *from = chan->loc;
        *res = BUSYBEE_DISCONNECT;
        work_close(chan);
        return true;
    }
    else if (rem < 0)
    {
        return false;
    }

    // If we could read more.
    if (static_cast<size_t>(rem) == IO_BLOCKSIZE - chan->inoffset)
    {
        e::atomic::or_32_nobarrier(&chan->events, EPOLLIN);
    }

    // We know rem is >= 0, so add the amount of preexisting data.
    rem += chan->inoffset;
    chan->inoffset = 0;
    uint8_t* data = buffer;
    bool ret = false;

    // XXX If this fails to allocate memory at any time, we need to just close
    // the channel.
    while (rem > 0)
    {
        if (!chan->inprogress.get())
        {
            if (rem < static_cast<ssize_t>(sizeof(uint32_t)))
            {
                memmove(chan->inbuffer, data, rem);
                chan->inoffset = rem;
                rem = 0;
            }
            else
            {
                uint32_t sz;
                e::unpack32be(data, &sz);
                // XXX sanity check sz to prevent memory exhaustion.
                chan->inprogress.reset(e::buffer::create(sz));
                memmove(chan->inprogress->data(), data, sizeof(uint32_t));
                chan->inprogress->resize(sizeof(uint32_t));
                rem -= sizeof(uint32_t);
                data += sizeof(uint32_t);
            }
        }
        else
        {
            uint32_t sz = chan->inprogress->capacity() - chan->inprogress->size();
            sz = std::min(static_cast<uint32_t>(rem), sz);
            rem -= sz;
            memmove(chan->inprogress->data() + chan->inprogress->size(),
                    data, sz);
            chan->inprogress->resize(chan->inprogress->size() + sz);
            data += sz;

            if (chan->inprogress->size() == chan->inprogress->capacity())
            {
                if (ret)
                {
                    message m;
                    m.loc = chan->loc;
                    m.buf = chan->inprogress;
                    // XXX This may fail
                    m_incoming.push(m);
                }
                else
                {
                    *from = chan->loc;
                    *msg = chan->inprogress;
                    *res = BUSYBEE_SUCCESS;
                    ret = true;
                }

                assert(!chan->inprogress.get());
            }
        }
    }

    return ret;
}

bool
CLASSNAME :: work_write(channel* chan,
                        busybee_returncode* res)
{
    if (chan->soc.get() < 0)
    {
        return true;
    }

    if (chan->outprogress.empty())
    {
        if (!chan->outgoing.pop(&chan->outnow))
        {
            return true;
        }

        chan->outprogress = chan->outnow->as_slice();
    }

    int flags = 0;
#ifdef HAVE_MSG_NOSIGNAL
    flags |= MSG_NOSIGNAL;
#endif // HAVE_MSG_NOSIGNAL
    ssize_t ret = chan->soc.send(chan->outprogress.data(), chan->outprogress.size(), flags);

    if (ret < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
    {
        *res = BUSYBEE_DISCONNECT;
        work_close(chan);
        return false;
    }

    if (ret > 0)
    {
        chan->outprogress.advance(ret);
    }

    if (chan->outprogress.empty())
    {
        if (chan->outgoing.pop(&chan->outnow))
        {
            chan->outprogress = chan->outnow->as_slice();
            e::atomic::or_32_nobarrier(&chan->events, EPOLLOUT);
        }
    }

    return true;
}
