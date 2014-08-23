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

#ifdef BUSYBEE_DEBUG
#define DEBUG std::cerr << __FILE__ << ":" << __LINE__ << " " << this << " "
#else
#define DEBUG if (0) std::cerr << __FILE__ << ":" << __LINE__ << " " << this << " "
#endif

#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

// POSIX
#include <signal.h>
#include <sys/types.h>
#include <poll.h>

// Linux
#ifdef HAVE_EPOLL_CTL
#include <sys/epoll.h>
#include <sys/eventfd.h>
#endif

// BSD / OS X
#ifdef HAVE_KQUEUE
#include <sys/event.h>
#endif

// po6
#include <po6/net/socket.h>
#include <po6/threads/mutex.h>

// e
#include <e/atomic.h>
#include <e/endian.h>
#include <e/guard.h>

// BusyBee
#include "busybee_constants.h"
#include "busybee_utils.h"

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

#ifdef min
#undef min
#endif

#define CLASSNAME CONCAT(busybee_, BUSYBEE_TYPE)

#ifdef HAVE_EPOLL_CTL
#define EPOLL_CREATE(N) epoll_create(N)
#elif defined HAVE_KQUEUE
#define EPOLL_CREATE(N) kqueue()
#endif

#ifdef HAVE_MSG_NOSIGNAL
#define SENDRECV_FLAGS (MSG_NOSIGNAL)
#else
#define SENDRECV_FLAGS 0
#endif // HAVE_MSG_NOSIGNAL

// Some Constants
#define IO_BLOCKSIZE 4096
#define HIDDEN __attribute__((visibility("hidden")))

#define BBMSG_IDENTIFY 0x80000000UL
#define BBMSG_FIN      0x40000000UL
#define BBMSG_ACK      0x20000000UL
#define BBMSG_SIZE     0x1fffffffUL
#define BBMSG_FLAGS    0xe0000000UL

///////////////////////////////// Message Class ////////////////////////////////

struct HIDDEN CLASSNAME::send_message
{
    send_message();
    send_message(send_message* n, std::auto_ptr<e::buffer> m);
    ~send_message() throw ();
    send_message* next;
    std::auto_ptr<e::buffer> msg;

    private:
        send_message(const send_message&);
        send_message& operator = (const send_message&);
};

CLASSNAME :: send_message :: send_message()
    : next(NULL)
    , msg()
{
}

CLASSNAME :: send_message :: send_message(send_message* n, std::auto_ptr<e::buffer> m)
    : next(n)
    , msg(m)
{
}

CLASSNAME :: send_message :: ~send_message() throw ()
{
}

///////////////////////////////// Message Class ////////////////////////////////

struct HIDDEN CLASSNAME::recv_message
{
    recv_message();
    recv_message(recv_message* n, uint64_t i, std::auto_ptr<e::buffer> m);
    ~recv_message() throw ();
    recv_message* next;
    uint64_t id;
    std::auto_ptr<e::buffer> msg;

    private:
        recv_message(const recv_message&);
        recv_message& operator = (const recv_message&);
};

CLASSNAME :: recv_message :: recv_message()
    : next(NULL)
    , id(0)
    , msg()
{
}

CLASSNAME :: recv_message :: recv_message(recv_message* n, uint64_t i, std::auto_ptr<e::buffer> m)
    : next(n)
    , id(i)
    , msg(m)
{
}

CLASSNAME :: recv_message :: ~recv_message() throw ()
{
}

///////////////////////////////// Channel Class ////////////////////////////////

class HIDDEN CLASSNAME::channel
{
    public:
        channel();
        ~channel() throw ();

    public:
        void lock();
        void unlock();
        void reset(size_t channels_sz);
        void initialize(po6::net::socket* soc);

    public:
        enum { NOTCONNECTED, CONNECTED, IDENTIFIED, CRASHING, EXTERNAL } state;
        uint64_t id;
        uint64_t tag;
        po6::net::socket soc;
        bool sender_has_it;
        bool recver_has_it;
        bool need_send;
        bool need_recv;
#ifdef BUSYBEE_MULTITHREADED
        po6::threads::mutex mtx;
#endif // BUSYBEE_MULTITHREADED
        // recv state
        unsigned short recv_partial_header_sz;
        char recv_partial_header[sizeof(uint32_t)];
        std::auto_ptr<e::buffer> recv_partial_msg;
        uint32_t recv_flags;
        // send state
        send_message* send_queue; // must hold mtx
        send_message** send_end; // must hold mtx
        uint8_t* send_ptr; // must flip sender_has_it false->true

    private:
        channel(const channel&);
        channel& operator = (const channel&);
};

CLASSNAME :: channel :: channel()
    : state(NOTCONNECTED)
    , id(0)
    , tag(0)
    , soc()
    , sender_has_it(false)
    , recver_has_it(false)
    , need_send(false)
    , need_recv(false)
#ifdef BUSYBEE_MULTITHREADED
    , mtx()
#endif // BUSYBEE_MULTITHREADED
    , recv_partial_header_sz(0)
    , recv_partial_msg()
    , recv_flags(0)
    , send_queue(NULL)
    , send_end(&send_queue)
    , send_ptr(NULL)
{
}

CLASSNAME :: channel :: ~channel() throw ()
{
}

void
CLASSNAME :: channel :: lock()
{
#ifdef BUSYBEE_MULTITHREADED
    mtx.lock();
#endif // BUSYBEE_MULTITHREADED
}

void
CLASSNAME :: channel :: unlock()
{
#ifdef BUSYBEE_MULTITHREADED
    mtx.unlock();
#endif // BUSYBEE_MULTITHREADED
}

void
CLASSNAME :: channel :: reset(size_t channels_sz)
{
    state = channel::NOTCONNECTED;
    id    = 0;
    tag  += channels_sz;
    sender_has_it = false;
    recver_has_it = false;
    need_send = false;
    need_recv = false;

    if (soc.get() >= 0)
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
    }

    recv_partial_header_sz = 0;
    recv_partial_msg.reset();
    recv_flags = 0;

    while (send_queue)
    {
        send_message* tmp = send_queue;
        send_queue = send_queue->next;
        delete tmp;
    }

    send_queue = NULL;
    send_end = &send_queue;
    send_ptr = NULL;
}

///////////////////////////////// BusyBee Class ////////////////////////////////

#ifdef BUSYBEE_MTA
busybee_mta :: busybee_mta(e::garbage_collector* gc,
                           busybee_mapper* mapper,
                           const po6::net::location& bind_to,
                           uint64_t server_id,
                           size_t num_threads)
    : m_epoll(EPOLL_CREATE(64))
    , m_listen(bind_to.address.family(), SOCK_STREAM, IPPROTO_TCP)
    , m_channels_sz(sysconf(_SC_OPEN_MAX))
    , m_channels(new channel[m_channels_sz])
    , m_server2channel(gc)
    , m_mapper(mapper)
    , m_server_id(server_id)
    , m_anon_lock()
    , m_anon_id(1)
    , m_timeout(-1)
    , m_recv_lock()
    , m_recv_queue(NULL)
    , m_recv_end(&m_recv_queue)
    , m_recv_queue_setaside(NULL)
    , m_recv_end_setaside(NULL)
    , m_sigmask()
    , m_pipebuf(new char[num_threads])
    , m_eventfdread()
    , m_eventfdwrite()
    , m_pause_lock()
    , m_pause_all_paused(&m_pause_lock)
    , m_pause_may_unpause(&m_pause_lock)
    , m_shutdown(false)
    , m_pause_count(num_threads)
    , m_pause_paused(0)
    , m_pause_num(0)
{
    assert(m_server_id == 0 || m_server_id >= (1ULL << 32ULL));
    po6::threads::mutex::hold holdr(&m_recv_lock);
    po6::threads::mutex::hold holdp(&m_pause_lock);
    e::atomic::store_32_nobarrier(&m_pause_paused, 0);

    if (m_epoll.get() < 0)
    {
        throw po6::error(errno);
    }

    add_signals();

    int eventfd[2];

    if (pipe(eventfd) < 0)
    {
        throw po6::error(errno);
    }

    m_eventfdread = eventfd[0];
    m_eventfdwrite = eventfd[1];

    m_listen.set_reuseaddr();
    m_listen.bind(bind_to);
    m_listen.listen(m_channels_sz);
    m_listen.set_nonblocking();

    if (add_event(m_listen.get(), EPOLLIN) < 0)
    {
        throw po6::error(errno);
    }

    if (add_event(m_eventfdread.get(),EPOLLIN) < 0)
    {
        throw po6::error(errno);
    }

    for (size_t i = 0; i < m_channels_sz; ++i)
    {
        m_channels[i].tag = m_channels_sz + i;
    }

    sigemptyset(&m_sigmask);
    DEBUG << "initialized busybee_mta instance at " << this << std::endl;
}
#endif // mta

#ifdef BUSYBEE_STA
busybee_sta :: busybee_sta(e::garbage_collector* gc,
                           busybee_mapper* mapper,
                           const po6::net::location& bind_to,
                           uint64_t server_id)
    : m_epoll(EPOLL_CREATE(64))
    , m_listen(bind_to.address.family(), SOCK_STREAM, IPPROTO_TCP)
    , m_channels_sz(sysconf(_SC_OPEN_MAX))
    , m_channels(new channel[m_channels_sz])
    , m_server2channel(gc)
    , m_mapper(mapper)
    , m_server_id(server_id)
    , m_anon_id(1)
    , m_timeout(-1)
    , m_recv_queue(NULL)
    , m_recv_end(&m_recv_queue)
    , m_sigmask()
{
    assert(m_server_id == 0 || m_server_id >= (1ULL << 32ULL));

    if (m_epoll.get() < 0)
    {
        throw po6::error(errno);
    }

    add_signals();

    m_listen.set_reuseaddr();
    m_listen.bind(bind_to);
    m_listen.listen(m_channels_sz);
    m_listen.set_nonblocking();

    if (add_event(m_listen.get(), EPOLLIN) < 0)
    {
        throw po6::error(errno);
    }

    for (size_t i = 0; i < m_channels_sz; ++i)
    {
        m_channels[i].tag = m_channels_sz + i;
    }

    sigemptyset(&m_sigmask);
    DEBUG << "initialized busybee_sta instance at " << this << std::endl;
}
#endif // sta

#ifdef BUSYBEE_ST
busybee_st :: busybee_st(e::garbage_collector* gc,
                         busybee_mapper* mapper,
                         uint64_t server_id)
    : m_epoll(EPOLL_CREATE(64))
    , m_channels_sz(sysconf(_SC_OPEN_MAX))
    , m_channels(new channel[m_channels_sz])
    , m_server2channel(gc)
    , m_mapper(mapper)
    , m_server_id(server_id)
    , m_anon_id(1)
    , m_timeout(-1)
    , m_recv_queue(NULL)
    , m_recv_end(&m_recv_queue)
    , m_sigmask()
{
    assert(m_server_id == 0 || m_server_id >= (1ULL << 32ULL));

    if (m_epoll.get() < 0)
    {
        throw po6::error(errno);
    }

    add_signals();

    for (size_t i = 0; i < m_channels_sz; ++i)
    {
        m_channels[i].tag = m_channels_sz + i;
    }

    sigemptyset(&m_sigmask);
    DEBUG << "initialized busybee_st instance at " << this << std::endl;
}
#endif // st

CLASSNAME :: ~CLASSNAME() throw ()
{
#ifdef BUSYBEE_MULTITHREADED
    shutdown();
    po6::threads::mutex::hold hold(&m_recv_lock);
#endif // BUSYBEE_MULTITHREADED

    while (m_recv_queue)
    {
        recv_message* m = m_recv_queue;
        m_recv_queue = m_recv_queue->next;
        delete m;
    }
}

void
CLASSNAME :: add_signals()
{
#ifdef HAVE_KQUEUE
    struct kevent ee[5];
    memset(&ee, 0, sizeof(ee));
    EV_SET(&ee[0], SIGTERM, EVFILT_SIGNAL, EV_ADD | EV_CLEAR, 0, 0, NULL);
    EV_SET(&ee[1], SIGHUP, EVFILT_SIGNAL, EV_ADD | EV_CLEAR, 0, 0, NULL);
    EV_SET(&ee[2], SIGINT, EVFILT_SIGNAL, EV_ADD | EV_CLEAR, 0, 0, NULL);
    EV_SET(&ee[3], SIGALRM, EVFILT_SIGNAL, EV_ADD | EV_CLEAR, 0, 0, NULL);
    EV_SET(&ee[4], SIGUSR1, EVFILT_SIGNAL, EV_ADD | EV_CLEAR, 0, 0, NULL);

    if(kevent(m_epoll.get(), ee, 5, NULL, 0, NULL) < 0)
    {
        throw po6::error(errno);
    }
#endif
}

#ifdef BUSYBEE_MULTITHREADED
void
CLASSNAME :: shutdown()
{
    po6::threads::mutex::hold hold(&m_pause_lock);
    DEBUG << "shutdown called" << std::endl;
    m_shutdown = true;
    up_the_semaphore();
}

void
CLASSNAME :: pause()
{
    po6::threads::mutex::hold hold(&m_pause_lock);
    assert(e::atomic::load_32_nobarrier(&m_pause_paused) == 0);
    e::atomic::store_32_nobarrier(&m_pause_paused, 1);
    DEBUG << "pause called" << std::endl;
    up_the_semaphore();

    // switch out the queue of messages so that threads pause quicker
    m_recv_lock.lock();
    m_recv_queue_setaside = m_recv_queue;
    m_recv_end_setaside = m_recv_end;
    m_recv_queue = NULL;
    m_recv_end = &m_recv_queue;
    m_recv_lock.unlock();

    while (m_pause_num < m_pause_count)
    {
        DEBUG << "paused " << m_pause_num << "/" << m_pause_count << std::endl;
        m_pause_all_paused.wait();
    }

    DEBUG << "all threads paused" << std::endl;
}

void
CLASSNAME :: unpause()
{
    po6::threads::mutex::hold hold(&m_pause_lock);
    DEBUG << "unpause called" << std::endl;
    e::atomic::store_32_nobarrier(&m_pause_paused, 0);
    m_pause_may_unpause.broadcast();
    // restore our switched-out queue
    m_recv_lock.lock();

    if (m_recv_queue_setaside)
    {
        *m_recv_end = m_recv_queue_setaside;
        m_recv_end = m_recv_end_setaside;
        m_recv_queue_setaside = NULL;
        m_recv_end_setaside = NULL;
    }

    m_recv_lock.unlock();
}

void
CLASSNAME :: wake_one()
{
    uint64_t num = 1;
    ssize_t ret = m_eventfdwrite.write(&m_pipebuf, num);
    assert(ret == 1); // XXX
}
#endif // BUSYBEE_MULTITHREADED

void
CLASSNAME :: set_id(uint64_t server_id)
{
    DEBUG << "changing id from " << m_server_id << " to " << server_id << std::endl;
    m_server_id = server_id;
}

void
CLASSNAME :: set_timeout(int timeout)
{
    m_timeout = timeout;
}

void
CLASSNAME :: set_ignore_signals()
{
    sigfillset(&m_sigmask);
}

void
CLASSNAME :: unset_ignore_signals()
{
    sigemptyset(&m_sigmask);
}

#ifdef BUSYBEE_ST
busybee_returncode
busybee_st :: set_external_fd(int fd)
{
    channel* chan = &m_channels[fd];
    chan->lock();

    if (chan->state == channel::EXTERNAL)
    {
        DEBUG << "fd=" << fd << " already in EXTERNAL state" << std::endl;
        chan->unlock();
        return BUSYBEE_SUCCESS;
    }

    assert(chan->state == channel::NOTCONNECTED);

    if (add_event(fd, EPOLLIN) < 0 && errno != EEXIST)
    {
        DEBUG << "failed to add file descriptor to epoll" << std::endl;
        return BUSYBEE_POLLFAILED;
    }

    DEBUG << "setting fd=" << fd << " to EXTERNAL state" << std::endl;
    chan->state = channel::EXTERNAL;
    chan->unlock();
    return BUSYBEE_SUCCESS;
}
#endif

busybee_returncode
CLASSNAME :: get_addr(uint64_t server_id, po6::net::location* addr)
{
    channel* chan = NULL;
    uint64_t chan_tag = UINT64_MAX;

    if (!m_server2channel.get(server_id, &chan_tag))
    {
        return BUSYBEE_DISRUPTED;
    }

    chan = &m_channels[(chan_tag) % m_channels_sz];
#ifdef BUSYBEE_MULTITHREADED
    po6::threads::mutex::hold hold(&chan->mtx);
#endif

    try
    {
        *addr = chan->soc.getpeername();
        return BUSYBEE_SUCCESS;
    }
    catch (po6::error& e)
    {
        errno = e;
        return BUSYBEE_DISRUPTED;
    }
}

#ifdef BUSYBEE_MULTITHREADED
bool
CLASSNAME :: deliver(uint64_t server_id, std::auto_ptr<e::buffer> msg)
{
    DEBUG << "deliver(" << server_id << ", 0x" << msg->hex() << ")" << std::endl;
    recv_message* m(new recv_message(NULL, server_id, msg));
#ifdef BUSYBEE_MULTITHREADED
    po6::threads::mutex::hold hold(&m_recv_lock);
#endif // BUSYBEE_MULTITHREADED
    *m_recv_end = m;
    m_recv_end = &(m->next);
    return true;
}
#endif // BUSYBEE_MULTITHREADED

int
CLASSNAME :: poll_fd()
{
    return m_epoll.get();
}

busybee_returncode
CLASSNAME :: drop(uint64_t server_id)
{
    channel* chan = NULL;
    uint64_t chan_tag = UINT64_MAX;

    if (!m_server2channel.get(server_id, &chan_tag))
    {
        return BUSYBEE_SUCCESS;
    }

    chan = &m_channels[(chan_tag) % m_channels_sz];
    chan->lock();
    chan->state = channel::CRASHING;
    busybee_returncode rc;
    work_close(chan, &rc);
    chan->unlock();
    return BUSYBEE_SUCCESS;
}

busybee_returncode
CLASSNAME :: send(uint64_t server_id,
                  std::auto_ptr<e::buffer> msg)
{
    assert(msg->size() >= BUSYBEE_HEADER_SIZE);
    assert(msg->size() <= BUSYBEE_MAX_MSG_SIZE);
    *msg << static_cast<uint32_t>(msg->size());
    DEBUG << "send(" << server_id << ", " << msg->hex() << ")" << std::endl;
    std::auto_ptr<send_message> sm(new send_message(NULL, msg));

    while (true)
    {
        channel* chan = NULL;
        uint64_t chan_tag = UINT64_MAX;
        busybee_returncode rc = get_channel(server_id, &chan, &chan_tag);

        if (rc != BUSYBEE_SUCCESS)
        {
            DEBUG << "_get_channel failed: " << rc << std::endl;
            return rc;
        }

        chan->lock();

        if (chan_tag != chan->tag ||
            chan->state < channel::CONNECTED ||
            chan->state > channel::IDENTIFIED)
        {
            chan->unlock();
            continue;
        }

        bool queue_was_free = !chan->sender_has_it;
        bool queue_was_empty = !chan->send_queue;
        *chan->send_end = sm.get();
        chan->send_end = &sm->next;
        sm.release();
        chan->sender_has_it = chan->sender_has_it || queue_was_empty;
        chan->unlock();

        if (queue_was_empty && queue_was_free)
        {
            // we have exclusive send access
            rc = BUSYBEE_SUCCESS;

            if (!work_send(chan, &rc))
            {
                return rc;
            }
        }

        return BUSYBEE_SUCCESS;
    }
}

busybee_returncode
CLASSNAME :: recv(uint64_t* id, std::auto_ptr<e::buffer>* msg)
{
#ifdef BUSYBEE_MULTITHREADED
    bool need_to_pause = false;
#endif // BUSYBEE_MULTITHREADED
    //DEBUG << "recv(" << id << ", " << msg << ")" << std::endl;
    *id = 0;

    while (true)
    {
#ifdef BUSYBEE_MULTITHREADED
        if (need_to_pause ||
            e::atomic::load_32_nobarrier(&m_pause_paused) != 0)
        {
            bool did_we_pause = false;
            po6::threads::mutex::hold hold(&m_pause_lock);

            while (e::atomic::load_32_nobarrier(&m_pause_paused) != 0 &&
                   !m_shutdown)
            {
                ++m_pause_num;
                assert(m_pause_num <= m_pause_count);

                if (m_pause_num == m_pause_count)
                {
                    m_pause_all_paused.signal();
                }

                did_we_pause = true;
                m_pause_may_unpause.wait();
                --m_pause_num;
            }

            if (!did_we_pause)
            {
                m_recv_lock.lock();
                // this is a barrier to ensure we see m_recv_queue
                m_recv_lock.unlock();
            }
        }

        if (m_shutdown)
        {
            return BUSYBEE_SHUTDOWN;
        }
#endif // BUSYBEE_MULTITHREADED

        // this is a racy read; we assume that some thread will see the latest
        // value (likely the one that last changed it).
        if (m_recv_queue)
        {
#ifdef BUSYBEE_MULTITHREADED
            m_recv_lock.lock();

            if (!m_recv_queue)
            {
                m_recv_lock.unlock();
                continue;
            }
#endif // BUSYBEE_MULTITHREADED

            recv_message* m = m_recv_queue;

            if (m_recv_queue->next)
            {
                m_recv_queue = m_recv_queue->next;
            }
            else
            {
                m_recv_queue = NULL;
                m_recv_end = &m_recv_queue;
            }

#ifdef BUSYBEE_MULTITHREADED
            m_recv_lock.unlock();
#endif // BUSYBEE_MULTITHREADED
            *id = m->id;
            *msg = m->msg;
            delete m;
            return BUSYBEE_SUCCESS;
        }

        int status;
        int fd;
        uint32_t events;

        if ((status = wait_event(&fd, &events)) <= 0)
        {
            if (status < 0 &&
                errno != EAGAIN &&
                errno != EINTR &&
                errno != EWOULDBLOCK)
            {
                return BUSYBEE_POLLFAILED;
            }

            if (status < 0 && errno == EINTR)
            {
                return BUSYBEE_INTERRUPTED;
            }

            if (status == 0 && m_timeout >= 0)
            {
                return BUSYBEE_TIMEOUT;
            }

            continue;
        }

#ifdef BUSYBEE_MULTITHREADED
        if (fd == m_eventfdread.get())
        {
            if ((events & EPOLLIN))
            {
                char buf;
                m_eventfdread.read(&buf, 1);
                need_to_pause = true;
            }

            continue;
        }
#endif // BUSYBEE_MULTITHREADED

#ifdef BUSYBEE_ACCEPT
        if (fd == m_listen.get())
        {
            if ((events & EPOLLIN))
            {
                work_accept();
            }

            continue;
        }
#endif // BUSYBEE_ACCEPT

        DEBUG << "processing fd=" << fd << " events=" << events << std::endl;
        channel* chan = &m_channels[fd];

        // acquire relevant read/write locks
        chan->lock();
        DEBUG << "channel in state=" << static_cast<unsigned>(chan->state) << std::endl;

        if (chan->state == channel::EXTERNAL)
        {
            chan->unlock();
            *id = fd;
            return BUSYBEE_EXTERNAL;
        }

        if (chan->state < channel::CONNECTED ||
            chan->state > channel::IDENTIFIED)
        {
            chan->unlock();
            continue;
        }

        uint64_t _id = chan->id;
        busybee_returncode rc;

        // this call unlocks chan
        if (!work_dispatch(chan, events, &rc))
        {
            *id = _id;
            msg->reset();
            return rc;
        }
    }
}

#ifdef HAVE_EPOLL_CTL
int
CLASSNAME :: add_event(int fd, uint32_t events)
{
  epoll_event ee;
  memset(&ee, 0, sizeof(ee));
  ee.data.fd = fd;
  ee.events = events;
  return epoll_ctl(m_epoll.get(), EPOLL_CTL_ADD, fd, &ee);
}
#endif

#if defined HAVE_KQUEUE
int
CLASSNAME :: add_event(int fd, uint32_t events)
{
  int flags = (events & EPOLLET) ? EV_ADD | EV_CLEAR : EV_ADD;
  int n = 0;
  struct kevent ee[2];
  memset(&ee, 0, sizeof(ee));

  if(events & EPOLLIN)
  {
    EV_SET(ee, fd, EVFILT_READ, flags, 0, 0, NULL);
    ++n;
  }
  if(events & EPOLLOUT)
  {
    EV_SET(&ee[n], fd, EVFILT_WRITE, flags, 0, 0, NULL);
    ++n;
  }
  return kevent(m_epoll.get(), ee, n, NULL, 0, NULL);
}
#endif

#ifdef HAVE_EPOLL_CTL
int
CLASSNAME :: wait_event(int* fd, uint32_t* events)
{
    //DEBUG << "epoll_pwait" << std::endl;
    epoll_event ee;
    int ret = epoll_pwait(m_epoll.get(), &ee, 1, m_timeout, &m_sigmask);
    *fd = ee.data.fd;
    *events = ee.events;
    return ret;
}
#endif

#ifdef HAVE_KQUEUE
int
CLASSNAME :: wait_event(int* fd, uint32_t* events)
{
    DEBUG << "kevent" << std::endl;
    struct kevent ee;
    struct timespec to = {0,0}; 
    int ret;

    if (m_timeout < 0) 
    {
        ret = kevent(m_epoll.get(), NULL, 0, &ee, 1, NULL);
    }
    else
    {
        to.tv_sec = m_timeout / 1000;
        to.tv_nsec = (m_timeout % 1000) * 1000000;
        ret = kevent(m_epoll.get(), NULL, 0, &ee, 1, &to);
    }

    *fd = ee.ident;

    if (ret > 0)
    {
        switch(ee.filter)
        {
            case EVFILT_READ:
                *events = EPOLLIN;
                break;
            case EVFILT_WRITE:
                *events = EPOLLOUT;
                break;
            case EVFILT_SIGNAL:
                sigset_t origmask;
                sigprocmask(SIG_SETMASK, &m_sigmask, &origmask);
                kill(getpid(), ee.ident); 
                sigprocmask(SIG_SETMASK, &origmask, NULL);
                ret = -1;
                errno = EINTR;
                break;
            default:
                *events = EPOLLERR;
        }
    }

    return ret;
}
#endif

#ifdef BUSYBEE_MULTITHREADED
void
CLASSNAME :: up_the_semaphore()
{
    ssize_t ret = m_eventfdwrite.write(&m_pipebuf, m_pause_count);
    assert(ret == static_cast<ssize_t>(m_pause_count));
}
#endif // BUSYBEE_MULTITHREADED

busybee_returncode
CLASSNAME :: get_channel(uint64_t server_id, channel** _chan, uint64_t* _chan_tag)
{
    if (m_server2channel.get(server_id, _chan_tag))
    {
        *_chan = &m_channels[(*_chan_tag) % m_channels_sz];
        return BUSYBEE_SUCCESS;
    }

    bool cleanup_needed = false;
    *_chan = NULL;
    *_chan_tag = UINT64_MAX;

    try
    {
        po6::net::location dst;

        if (!m_mapper->lookup(server_id, &dst))
        {
            return BUSYBEE_DISRUPTED;
        }

        po6::net::socket soc(dst.address.family(), SOCK_STREAM, IPPROTO_TCP);
        soc.set_nonblocking();

        try
        {
            // Wouldn't it be great to refactor and get rid of exceptions?  Yes,
            // but not worth the cost now.
            soc.connect(dst);
        }
        catch (po6::error& e)
        {
            if (e != EINPROGRESS)
            {
                throw e;
            }
        }

        *_chan = &m_channels[soc.get()];
        (*_chan)->lock();
        assert((*_chan)->state == channel::NOTCONNECTED);
        cleanup_needed = true;
        busybee_returncode rc = setup_channel(&soc, *_chan);

        if (rc != BUSYBEE_SUCCESS)
        {
            (*_chan)->reset(m_channels_sz);
            (*_chan)->unlock();
            return rc;
        }

        (*_chan)->id = server_id;
        m_server2channel.put_ine(server_id, (*_chan)->tag);
        // at this point, the channel is fully setup
        *_chan_tag = (*_chan)->tag;
        return possibly_work_send_or_recv(*_chan);
    }
    catch (po6::error& e)
    {
        if (cleanup_needed)
        {
            (*_chan)->reset(m_channels_sz);
            (*_chan)->unlock();
        }

        return BUSYBEE_DISRUPTED;
    }
}

busybee_returncode
CLASSNAME :: setup_channel(po6::net::socket* soc, channel* chan)
{
    DEBUG << "setting up a new channel" << std::endl;
    chan->soc.swap(soc);
#ifdef HAVE_SO_NOSIGPIPE
    int sigpipeopt = 1;
    chan->soc.set_sockopt(SOL_SOCKET, SO_NOSIGPIPE, &sigpipeopt, sizeof(sigpipeopt));
#endif // HAVE_SO_NOSIGPIPE
    chan->soc.set_tcp_nodelay();
    chan->soc.set_nonblocking();
    chan->state = channel::CONNECTED;

    if (add_event(chan->soc.get(),EPOLLIN|EPOLLOUT|EPOLLET) < 0)
    {
        DEBUG << "failed to add file descriptor to epoll" << std::endl;
        return BUSYBEE_POLLFAILED;
    }

    std::auto_ptr<e::buffer> msg;
    msg.reset(e::buffer::create(sizeof(uint32_t) + sizeof(uint64_t)));
    uint32_t sz = (sizeof(uint32_t) + sizeof(uint64_t)) | BBMSG_IDENTIFY;
    *msg << sz << m_server_id;
    std::auto_ptr<send_message> sm(new send_message(NULL, msg));

    *chan->send_end = sm.get();
    chan->send_end = &sm->next;
    sm.release();
    return BUSYBEE_SUCCESS;
}

busybee_returncode
CLASSNAME :: possibly_work_send_or_recv(channel* chan)
{
    pollfd pfd;
    pfd.fd = chan->soc.get();
    pfd.events = POLLIN|POLLOUT|POLLERR|POLLHUP;
    pfd.revents = 0;

    if (poll(&pfd, 1, 0) > 0)
    {
        DEBUG << "there's already activity, acting on it now" << std::endl;
        uint32_t events = 0;
        if ((pfd.revents & POLLIN)) events |= EPOLLIN;
        if ((pfd.revents & POLLOUT)) events |= EPOLLOUT;
        if ((pfd.revents & POLLERR)) events |= EPOLLERR;
        if ((pfd.revents & POLLHUP)) events |= EPOLLHUP;
        busybee_returncode rc;

        if (!work_dispatch(chan, events, &rc))
        {
            return rc;
        }
    }
    else
    {
        chan->unlock();
    }

    return BUSYBEE_SUCCESS;
}

bool
CLASSNAME :: work_dispatch(channel* chan, uint32_t events, busybee_returncode* rc)
{
    // each bool says whether this thread should read/write
    bool sender_has_it = !chan->sender_has_it && ((events & EPOLLOUT) || (events & EPOLLERR));
    bool recver_has_it = !chan->recver_has_it && ((events & EPOLLIN) || (events & EPOLLHUP));
    // if another thread is writing, record our event
    chan->need_send = chan->sender_has_it && ((events & EPOLLOUT) || (events & EPOLLERR));
    chan->need_recv = chan->recver_has_it && ((events & EPOLLIN) || (events & EPOLLHUP));
    // mark the methods we have it for
    chan->sender_has_it = chan->sender_has_it || sender_has_it;
    chan->recver_has_it = chan->recver_has_it || recver_has_it;
    chan->unlock();

    if (sender_has_it)
    {
        if (!work_send(chan, rc))
        {
            return false;
        }
    }

    if (recver_has_it)
    {
        if (!work_recv(chan, rc))
        {
            return false;
        }
    }

    return true;
}

#ifdef BUSYBEE_ACCEPT
void
CLASSNAME :: work_accept()
{
    bool cleanup_needed = false;
    channel* chan = NULL;

    try
    {
        po6::net::socket soc;
        m_listen.accept(&soc);

        if (soc.get() < 0)
        {
            DEBUG << "looks like a false accept" << std::endl;
            return;
        }

        chan = &m_channels[soc.get()];
        chan->lock();
        assert(chan->state == channel::NOTCONNECTED);
        cleanup_needed = true;
        busybee_returncode rc = setup_channel(&soc, chan);

        if (rc != BUSYBEE_SUCCESS)
        {
            chan->reset(m_channels_sz);
            chan->unlock();
            cleanup_needed = false;
            DEBUG << "could not setup channel for accept" << std::endl;
            return;
        }

        // at this point, the channel is fully setup
        cleanup_needed = false;
        possibly_work_send_or_recv(chan);
    }
    catch (po6::error &e)
    {
        if (cleanup_needed)
        {
            chan->reset(m_channels_sz);
            chan->unlock();
        }
    }
}
#endif // BUSYBEE_ACCEPT

bool
CLASSNAME :: work_close(channel* chan, busybee_returncode* rc)
{
    if (chan->sender_has_it || chan->recver_has_it)
    {
        DEBUG << "work_close becomes a NOP as someone else is writing" << std::endl;
        chan->unlock();
        return true;
    }

    uint64_t old_tag = UINT64_MAX;

    if (m_server2channel.get(chan->id, &old_tag) && chan->tag == old_tag)
    {
        m_server2channel.del_if(chan->id, old_tag);
    }

    DEBUG << "closing " << chan->tag << std::endl;
    chan->reset(m_channels_sz);
    chan->unlock();
    *rc = BUSYBEE_DISRUPTED;
    return false;
}

bool
CLASSNAME :: work_send(channel* chan, busybee_returncode* rc)
{
    uint8_t* data = NULL;
    size_t data_sz = 0;
    // msg must always point to the head of send_queue or be NULL
    e::buffer* msg = NULL;

    chan->lock();
    chan->need_send = false;

    if (!chan->send_queue)
    {
        chan->send_ptr = NULL;
        chan->sender_has_it = false;
        chan->unlock();
        *rc = BUSYBEE_SUCCESS;
        return true;
    }

    msg = chan->send_queue->msg.get();
    chan->unlock();

    while (true)
    {
        if (chan->send_ptr < msg->data() ||
            chan->send_ptr >= msg->data() + msg->size())
        {
            chan->send_ptr = msg->data();
        }

        data = chan->send_ptr;
        data_sz = (msg->data() + msg->size()) - data;
        ssize_t ret = chan->soc.send(data, data_sz, SENDRECV_FLAGS);

        if (ret < 0 &&
            errno != EINTR &&
            errno != EAGAIN &&
            errno != EWOULDBLOCK)
        {
            chan->lock();
            chan->state = channel::CRASHING;
            chan->sender_has_it = false;
            return work_close(chan, rc);
        }
        else if (ret < 0 && errno == EINTR )
        {
            continue;
        }
        else if (ret < 0) // EAGAIN/EWOULDBLOCK
        {
            chan->lock();

            if (chan->need_send)
            {
                chan->need_send = false;
                chan->unlock();
                continue;
            }
            else
            {
                chan->sender_has_it = false;
                chan->unlock();
                *rc = BUSYBEE_SUCCESS;
                return true;
            }
        }
        else if (ret == 0)
        {
            chan->lock();
            chan->sender_has_it = false;
            return work_close(chan, rc);
        }
        else
        {
            send_message* to_delete = NULL;
            chan->lock();
            chan->need_send = false;
            chan->send_ptr += ret;

            if (chan->send_ptr >= msg->data() + msg->size())
            {
                chan->send_ptr = NULL;
                to_delete = chan->send_queue;

                if (chan->send_queue->next)
                {
                    chan->send_queue = chan->send_queue->next;
                    msg = chan->send_queue->msg.get();
                }
                else
                {
                    chan->send_queue = NULL;
                    chan->send_end = &chan->send_queue;
                    msg = NULL;
                }
            }

            bool return_true = false;

            if (!chan->send_queue)
            {
                chan->sender_has_it = false;
                return_true = true;
            }

            chan->unlock();

            if (to_delete)
            {
                delete to_delete;
            }

            if (return_true)
            {
                *rc = BUSYBEE_SUCCESS;
                return true;
            }
        }
    }
}

bool
CLASSNAME :: work_recv(channel* chan, busybee_returncode* rc)
{
    recv_message* recv_queue = NULL;
    recv_message** recv_end = &recv_queue;

    while (true)
    {
        uint8_t buf[IO_BLOCKSIZE];

        // restore leftovers
        if (chan->recv_partial_header_sz)
        {
            DEBUG << "copy " << chan->recv_partial_header_sz << " bytes from last loop" << std::endl;
            memmove(buf, chan->recv_partial_header, chan->recv_partial_header_sz);
        }

        // read into buffer
        uint8_t* tmp_buf = buf + chan->recv_partial_header_sz;
        size_t tmp_buf_sz = IO_BLOCKSIZE - chan->recv_partial_header_sz;
        ssize_t rem = chan->soc.recv(tmp_buf, tmp_buf_sz, SENDRECV_FLAGS);
        DEBUG << "recv'd " << rem << " bytes" << std::endl;

        if (rem < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
        {
            chan->lock();
            chan->state = channel::CRASHING;
            chan->recver_has_it = false;
            return work_close(chan, rc);
        }
        else if (rem < 0 && errno == EINTR )
        {
            continue;
        }
        else if (rem < 0)
        {
            chan->lock();

            if (chan->need_recv)
            {
                chan->need_recv = false;
                chan->unlock();
                continue;
            }

            chan->recver_has_it = false;
            chan->unlock();

            if (recv_queue)
            {
                DEBUG << "copying received messages to global queue" << std::endl;
#ifdef BUSYBEE_MULTITHREADED
                po6::threads::mutex::hold hold(&m_recv_lock);
#endif // BUSYBEE_MULTITHREADED
                *m_recv_end = recv_queue;
                m_recv_end = recv_end;
            }

            return true;
        }
        else if (rem == 0)
        {
            chan->lock();
            chan->recver_has_it = false;
            return work_close(chan, rc);
        }

        DEBUG << "recv'd " << rem << " bytes, so chunking that into messages" << std::endl;
        // We know rem is >= 0, so add the amount of preexisting data.
        rem += chan->recv_partial_header_sz;
        chan->recv_partial_header_sz = 0;
        uint8_t* data = buf;

        while (rem > 0)
        {
            DEBUG << "still need to process " << rem << " bytes" << std::endl;

            if (!chan->recv_partial_msg.get())
            {
                DEBUG << "no message in progress" << std::endl;

                if (rem < static_cast<ssize_t>((sizeof(uint32_t))))
                {
                    DEBUG << "not enough to make a header" << std::endl;
                    memmove(chan->recv_partial_header, data, rem);
                    chan->recv_partial_header_sz = rem;
                    rem = 0;
                }
                else
                {
                    DEBUG << "created header" << std::endl;
                    uint32_t sz;
                    e::unpack32be(data, &sz);
                    chan->recv_flags = BBMSG_FLAGS & sz;
                    sz = BBMSG_SIZE & sz;

                    if (sz < sizeof(uint32_t))
                    {
                        chan->lock();
                        chan->state = channel::CRASHING;
                        chan->recver_has_it = false;
                        return work_close(chan, rc);
                    }

                    chan->recv_partial_msg.reset(e::buffer::create(sz));
                    memmove(chan->recv_partial_msg->data(), data, sizeof(uint32_t));
                    chan->recv_partial_msg->resize(sizeof(uint32_t));
                    rem -= sizeof(uint32_t);
                    data += sizeof(uint32_t);
                }
            }
            else
            {
                uint32_t sz = chan->recv_partial_msg->capacity() - chan->recv_partial_msg->size();
                sz = std::min(static_cast<uint32_t>(rem), sz);
                DEBUG << "filling in message with " << sz << " bytes" << std::endl;
                rem -= sz;
                memmove(chan->recv_partial_msg->data() + chan->recv_partial_msg->size(), data, sz);
                chan->recv_partial_msg->resize(chan->recv_partial_msg->size() + sz);
                data += sz;

                if (chan->recv_partial_msg->size() == chan->recv_partial_msg->capacity())
                {
                    if (chan->recv_flags)
                    {
                        if (!state_transition(chan, rc))
                        {
                            return false;
                        }
                    }
                    else
                    {
                        DEBUG << "received new message " << chan->recv_partial_msg->hex() << std::endl;
                        recv_message* tmp = new recv_message(NULL, chan->id, chan->recv_partial_msg);
                        *recv_end = tmp;
                        recv_end = &tmp->next;
                    }

                    // clear state
                    chan->recv_partial_header_sz = 0;
                    chan->recv_partial_msg.reset();
                    chan->recv_flags = 0;
                }
            }
        }

        DEBUG << "emptied I/O buffer; trying again" << std::endl;
    }
}

bool
CLASSNAME :: state_transition(channel* chan, busybee_returncode* rc)
{
    bool need_close = false;
    bool clean_close = false;
    chan->lock();

    if ((chan->recv_flags & BBMSG_IDENTIFY))
    {
        handle_identify(chan, &need_close, &clean_close);
    }

    chan->unlock();
    *rc = BUSYBEE_SUCCESS;
    return true;
}

void
CLASSNAME :: handle_identify(channel* chan,
                             bool* need_close,
                             bool* clean_close)
{
    if (chan->state == channel::CONNECTED)
    {
        DEBUG << "IDENTIFY message received" << std::endl;

        if (chan->recv_partial_msg->size() != (sizeof(uint32_t) + sizeof(uint64_t)))
        {
            DEBUG << "IDENTIFY message not sized correctly: " << chan->recv_partial_msg->hex() << std::endl;
            *need_close = true;
            *clean_close = false;
            return;
        }

        uint64_t id;
        e::unpack64be(chan->recv_partial_msg->data() + sizeof(uint32_t), &id);

        if (id == 0)
        {
#ifdef BUSYBEE_MULTITHREADED
            po6::threads::mutex::hold hold(&m_anon_lock);
#endif // BUSYBEE_MULTITHREADED

            while (m_server2channel.has(m_anon_id))
            {
                m_anon_id = (m_anon_id + 1) & UINT32_MAX;
            }

            id = m_anon_id;
            m_anon_id = (m_anon_id + 1) & UINT32_MAX;
        }
        else if (id < (1ULL << 32))
        {
            DEBUG << "IDENTIFY message specifies server_id=" << id << ", which is less than 1<<32" << std::endl;
            *need_close = true;
            *clean_close = false;
            return;
        }

        if (chan->id == 0)
        {
            DEBUG << "IDENTIFY message specifies server_id=" << id << std::endl;
            chan->id = id;
            m_server2channel.put_ine(id, chan->tag);
            // XXX dedupe!
        }
        else if (chan->id != id)
        {
            DEBUG << "IDENTIFY message specifies server_id=" << id << ", but channel set to " << chan->id << std::endl;
            *need_close = true;
            *clean_close = false;
            return;
        }

        DEBUG << "IDENTIFY success" << std::endl;
        chan->state = channel::IDENTIFIED;
    }
    else
    {
        DEBUG << "IDENTIFY message received when already identified or not connected" << std::endl;
        *need_close = true;
        *clean_close = false;
        return;
    }

    *need_close = false;
}
