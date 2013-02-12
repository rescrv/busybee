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
#define DEBUG std::cerr << __FILE__ << ":" << __LINE__ << " "
#else
#define DEBUG if (0) std::cerr << __FILE__ << ":" << __LINE__ << " "
#endif

#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

// POSIX
#include <signal.h>
#include <sys/types.h>
#ifndef _MSC_VER
#include <poll.h>
#endif

//Windows
#ifdef _MSC_VER
#include <Winsock2.h>
#include <ws2tcpip.h>
#include <mstcpip.h>
#endif

// Linux
#ifdef HAVE_EPOLL_CTL
#include <sys/epoll.h>
#include <sys/eventfd.h>
#elif defined HAVE_KQUEUE 
#include <sys/event.h>
#else
#error no_event
#endif

// po6
#include <po6/net/socket.h>
#include <po6/threads/mutex.h>

// e
#include <e/atomic.h>
#include <e/endian.h>
#include <e/guard.h>
#include <e/nonblocking_bounded_fifo.h>
#include <e/pow2.h>
#include <e/timer.h>

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
#elif HAVE_KQUEUE
#define EPOLL_CREATE(N) kqueue()
#endif

// Some Constants
#define IO_BLOCKSIZE 4096
#define NUM_MSGS_PER_RECV (IO_BLOCKSIZE / BUSYBEE_HEADER_SIZE)

#define BBMSG_IDENTIFY 0x80000000UL
#define BBMSG_FIN      0x40000000UL
#define BBMSG_ACK      0x20000000UL
#define BBMSG_MASK     0x1fffffffUL
#define BBMSG_FLAGS    0xe0000000UL

///////////////////////////////// Message Class ////////////////////////////////

struct CLASSNAME::send_message
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

struct CLASSNAME::recv_message
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

class CLASSNAME::channel
{
    public:
        channel();
        ~channel() throw ();

    public:
        void reset(uint64_t new_tag);
        void reset(uint64_t new_tag, po6::net::socket* soc);

    public:
        enum { NOTCONNECTED, CONNECTED, IDENTIFIED, FIN_SENT, FINACK_SENT } state;
        uint32_t flags;
#ifdef BUSYBEE_MULTITHREADED
        po6::threads::mutex mtx; // Anyone sending on the socket should hold this.
#endif // BUSYBEE_MULTITHREADED
        uint64_t id;
        uint64_t tag;
        po6::net::socket soc;
        // Queues and buffers for receiving
        recv_message* recv_queue;
        recv_message** recv_end;
        size_t recv_partial_header_sz;
        char recv_partial_header[sizeof(uint32_t)];
        std::auto_ptr<e::buffer> recv_partial_msg;
        // Queues for sending
        send_message* send_queue;
        send_message** send_end;
        e::slice send_progress;

    private:
        channel(const channel&);
        channel& operator = (const channel&);
};

CLASSNAME :: channel :: channel()
    : state(NOTCONNECTED)
    , flags(0)
#ifdef BUSYBEE_MULTITHREADED
    , mtx()
#endif // BUSYBEE_MULTITHREADED
    , id(0)
    , tag(0)
    , soc()
    , recv_queue(NULL)
    , recv_end(&recv_queue)
    , recv_partial_header_sz(0)
    , recv_partial_msg()
    , send_queue(NULL)
    , send_end(&send_queue)
    , send_progress("", 0)
{
}

CLASSNAME :: channel :: ~channel() throw ()
{
}

void
CLASSNAME :: channel :: reset(uint64_t new_tag)
{
    state = channel::NOTCONNECTED;
    id    = 0;
    tag   = new_tag;

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

    while (recv_queue)
    {
        recv_message* tmp = recv_queue;
        recv_queue = recv_queue->next;
        delete tmp;
    }

    recv_end = &recv_queue;
    recv_partial_header_sz = 0;
    recv_partial_msg.reset();

    while (send_queue)
    {
        send_message* tmp = send_queue;
        send_queue = send_queue->next;
        delete tmp;
    }

    send_end = &send_queue;
    send_progress = e::slice(NULL, 0);
}

void
CLASSNAME :: channel :: reset(uint64_t new_tag, po6::net::socket* dst)
{
    reset(new_tag);
    soc.swap(dst);
}

///////////////////////////////// BusyBee Class ////////////////////////////////

#ifdef BUSYBEE_MTA
busybee_mta :: busybee_mta(busybee_mapper* mapper,
                           const po6::net::location& bind_to,
                           uint64_t server_id,
                           size_t num_threads)
    : m_epoll(EPOLL_CREATE(64))
    , m_listen(bind_to.address.family(), SOCK_STREAM, IPPROTO_TCP)
    , m_channels_sz(sysconf(_SC_OPEN_MAX))
    , m_channels(new channel[m_channels_sz])
    , m_server2channel(10)
    , m_mapper(mapper)
    , m_server_id(server_id)
    , m_timeout(-1)
    , m_recv_lock()
    , m_recv_queue(NULL)
    , m_recv_end(&m_recv_queue)
    , m_sigmask()
    , m_pipebuf(new char[num_threads])
    , m_eventfdread()
    , m_eventfdwrite()
    , m_pause_lock()
    , m_pause_all_paused(&m_pause_lock)
    , m_pause_may_unpause(&m_pause_lock)
    , m_shutdown(false)
    , m_pause_count(num_threads)
    , m_pause_paused(false)
    , m_pause_num(0)
{
    po6::threads::mutex::hold holdr(&m_recv_lock);
    po6::threads::mutex::hold holdp(&m_pause_lock);

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
busybee_sta :: busybee_sta(busybee_mapper* mapper,
                           const po6::net::location& bind_to,
                           uint64_t server_id)
    : m_epoll(EPOLL_CREATE(64))
    , m_listen(bind_to.address.family(), SOCK_STREAM, IPPROTO_TCP)
    , m_channels_sz(sysconf(_SC_OPEN_MAX))
    , m_channels(new channel[m_channels_sz])
    , m_server2channel(10)
    , m_mapper(mapper)
    , m_server_id(server_id)
    , m_timeout(-1)
    , m_recv_queue(NULL)
    , m_recv_end(&m_recv_queue)
    , m_sigmask()
{
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
busybee_st :: busybee_st(busybee_mapper* mapper,
                         uint64_t server_id)
#ifdef _MSC_VER
	: m_epoll() //this is the master struct fd_set
	, m_channels_sz(1024) //XXX: find out how to get the right size in windows.
#else
    : m_epoll(EPOLL_CREATE(64))
    , m_channels_sz(sysconf(_SC_OPEN_MAX))
#endif
    , m_channels(new channel[m_channels_sz])
    , m_server2channel(10)
    , m_mapper(mapper)
    , m_server_id(server_id)
	, m_timeout(-1)
#ifdef _MSC_VER
	, m_external(NULL)
#else
    , m_external(-1)
#endif
	, m_recv_queue(NULL)
    , m_recv_end(&m_recv_queue)
#ifndef _MSC_VER
    , m_sigmask()
#endif
{

#ifndef _MSC_VER
    if (m_epoll.get() < 0)
    {
        throw po6::error(errno);
    }
#endif

    add_signals();

    for (size_t i = 0; i < m_channels_sz; ++i)
    {
        m_channels[i].tag = m_channels_sz + i;
    }

#ifndef _MSC_VER
    sigemptyset(&m_sigmask);
#endif
    DEBUG << "initialized busybee_st instance at " << this << std::endl;
}
#endif // st

CLASSNAME :: ~CLASSNAME() throw ()
{
#ifdef BUSYBEE_MULTITHREADED
    shutdown();
#endif // BUSYBEE_MULTITHREADED
}

void
CLASSNAME :: add_signals()
{
#ifdef HAVE_KQUEUE
    struct kevent ee[3];
    memset(&ee, 0, sizeof(ee));
    EV_SET(&ee[0], SIGTERM, EVFILT_SIGNAL, EV_ADD | EV_CLEAR, 0, 0, NULL);
    EV_SET(&ee[1], SIGQUIT, EVFILT_SIGNAL, EV_ADD | EV_CLEAR, 0, 0, NULL);
    EV_SET(&ee[2], SIGINT, EVFILT_SIGNAL, EV_ADD | EV_CLEAR, 0, 0, NULL);

    if(kevent(m_epoll.get(), ee, 3, NULL, 0, NULL) < 0)
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
    m_pause_paused = true;
    DEBUG << "pause called" << std::endl;
    up_the_semaphore();

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
    m_pause_paused = false;
    m_pause_may_unpause.broadcast();
}

void
CLASSNAME :: wake_one()
{
    uint64_t num = 1;
    ssize_t ret = m_eventfdwrite.write(&m_pipebuf, num);
    assert(ret == num);
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
#elif defined HAVE_KQUEUE
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

#ifndef _MSC_VER
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
#endif

#ifdef BUSYBEE_ST
#ifndef _MSC_VER
busybee_returncode
CLASSNAME :: set_external_fd(int fd)
{

    if (add_event(fd, EPOLLIN) < 0)
    {
        DEBUG << "failed to add file descriptor to epoll" << std::endl;
        return BUSYBEE_POLLFAILED;
    }

    m_external = fd;
    return BUSYBEE_SUCCESS;
}
#else
busybee_returncode
CLASSNAME :: set_external_fd(struct fd_set* fd)
{
	// XXX: This is not portable.  It relies on the windows implementation
	// of struct fd_set.

    m_external = fd;
    return BUSYBEE_SUCCESS;
}
#endif
#endif

#ifdef BUSYBEE_MULTITHREADED
bool
CLASSNAME :: deliver(uint64_t server_id, std::auto_ptr<e::buffer> msg)
{
    DEBUG << "processing deliver from " << server_id << " with message " << msg->hex() << std::endl;
    recv_message* m(new recv_message(NULL, server_id, msg));
#ifdef BUSYBEE_MULTITHREADED
    po6::threads::mutex::hold hold(&m_recv_lock);
#endif // BUSYBEE_MULTITHREADED
    *m_recv_end = m;
    m_recv_end = &(m->next);
    DEBUG << "done with deliver" << std::endl;
    return true;
}
#endif // BUSYBEE_MULTITHREADED

#ifndef _MSC_VER
int
CLASSNAME :: poll_fd()
{
    return m_epoll.get();
}
#else
struct fd_set*
CLASSNAME :: poll_fd()
{
	return &m_epoll;
}
#endif

busybee_returncode
CLASSNAME :: drop(uint64_t server_id)
{
    // Get the channel for this server or fail trying
    channel* chan = NULL;
    uint64_t chan_tag = UINT64_MAX;
    busybee_returncode rc = get_channel(server_id, &chan, &chan_tag);

    if (rc != BUSYBEE_SUCCESS)
    {
        return BUSYBEE_SUCCESS;
    }

    if (chan_tag != chan->tag || chan->soc.get() < 0)
    {
        return BUSYBEE_SUCCESS;
    }

    work_close(chan);
    return BUSYBEE_SUCCESS;
}

busybee_returncode
CLASSNAME :: send(uint64_t server_id,
                  std::auto_ptr<e::buffer> msg)
{
    assert(msg->size() >= BUSYBEE_HEADER_SIZE);
    assert(msg->size() <= BUSYBEE_MAX_MSG_SIZE);

    // Pack the size into the header
    *msg << static_cast<uint32_t>(msg->size());
    DEBUG << "processing send to " << server_id << " with message " << msg->hex() << std::endl;

    // Get the channel for this server or fail trying
    channel* chan = NULL;
    uint64_t chan_tag = UINT64_MAX;
    busybee_returncode rc = get_channel(server_id, &chan, &chan_tag);

    if (rc != BUSYBEE_SUCCESS)
    {
        DEBUG << "could not get channel in \"send\" because " << rc << std::endl;
        return rc;
    }

#ifdef BUSYBEE_MULTITHREADED
    po6::threads::mutex::hold hold(&chan->mtx);
#endif // BUSYBEE_MULTITHREADED

    if (chan_tag != chan->tag || chan->soc.get() < 0)
    {
        DEBUG << "disrupted because chan->tag=" << chan->tag
              << " (expected " << chan_tag << ") and fd=" << chan->soc.get()
              << std::endl;
        return BUSYBEE_DISRUPTED;
    }

    bool empty = !chan->send_queue;
    send_message* tmp = new send_message(NULL, msg);
    *chan->send_end = tmp;
    chan->send_end = &tmp->next;

    if (empty)
    {
        DEBUG << "no messages queued, setting current message to be sent" << std::endl;
        chan->send_progress = tmp->msg->as_slice();
    }

    bool need_close = false;
    bool quiet = true;
    DEBUG << "calling work_send" << std::endl;
    work_send(chan, &need_close, &quiet);
    DEBUG << "work_send returned " << (need_close ? "true" : "false") << " "
          << (quiet ? "true" : "false") << std::endl;

    if (need_close)
    {
        DEBUG << "calling work_close" << std::endl;
        work_close(chan);

        if (!quiet)
        {
            DEBUG << "not a quiet close, returning DISRUPTED" << std::endl;
            return BUSYBEE_DISRUPTED;
        }

        DEBUG << "quiet close" << std::endl;
    }

    DEBUG << "\"send\" succeeded" << std::endl;
    return BUSYBEE_SUCCESS;
}

busybee_returncode
CLASSNAME :: recv(uint64_t* id, std::auto_ptr<e::buffer>* msg)
{
#ifdef BUSYBEE_MULTITHREADED
    bool need_to_pause = false;
#endif // BUSYBEE_MULTITHREADED
    DEBUG << "entering \"recv\"" << std::endl;

    while (true)
    {
        DEBUG << "top of the loop" << std::endl;

#ifdef BUSYBEE_MULTITHREADED
        if (need_to_pause)
        {
            bool did_we_pause = false;
            DEBUG << "may need to pause" << std::endl;
            po6::threads::mutex::hold hold(&m_pause_lock);

            while (m_pause_paused && !m_shutdown)
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

            if (did_we_pause)
            {
                DEBUG << "unpaused" << std::endl;
            }
            else
            {
                DEBUG << "did not pause" << std::endl;
                m_recv_lock.lock();
                DEBUG << "queue: " << m_recv_queue;
                m_recv_lock.unlock();
            }
        }

        if (m_shutdown)
        {
            DEBUG << "returning SHUTDOWN" << std::endl;
            return BUSYBEE_SHUTDOWN;
        }
#endif // BUSYBEE_MULTITHREADED

        // this is a racy read; we assume that some thread will see the latest
        // value (likely the one that last changed it).
        if (m_recv_queue)
        {
#ifdef BUSYBEE_MULTITHREADED
            po6::threads::mutex::hold hold(&m_recv_lock);

            if (!m_recv_queue)
            {
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

            *id = m->id;
            *msg = m->msg;
            delete m;
            return BUSYBEE_SUCCESS;
        }

        DEBUG << "making syscall to poll for events" << std::endl;
        int status;
#ifdef _MSC_VER
		fd_set ee;
		fd_set ff;
		memmove(&ee, &m_epoll, sizeof(fd_set));

		if(m_external)
		{
			int i;

			for(i = 0; i < m_external->fd_count; ++i)
			{
				FD_SET(m_external->fd_array[i], &m_epoll);
			}
		}

		memmove(&ff, &ee, sizeof(fd_set));

		DEBUG << "m_timeout = " << m_timeout << std::endl;
		
		if(m_timeout < 0)
		{
			status = select(1, &ee, NULL, &ff, NULL);
		}

		else
		{
			struct timeval ts;
			ts.tv_sec = (m_timeout / 1000);
			ts.tv_usec = ((m_timeout % 1000) * 1000);
			status = select(1, &ee, NULL, &ff, &ts);
		}

		if(status <= 0)
		{

			// The fds are always in a contiguous block, so the
			// last part of the array is the full set of external
			// FDs.  Internally, the system uses fd_count to know
			// where to stop looking, so we can just subtract from it.
			// This ensures that each time we call select, we have the most
			// up to date set of fds from replicant.
			if(m_external)
				m_epoll.fd_count -= m_external->fd_count;
#else
        int fd;
        uint32_t events;

        if ((status = wait_event(&fd, &events)) <= 0)
        {
#endif
			
#ifdef _MSC_VER
            errno = WSAGetLastError();

            if (status < 0 &&
                errno != WSAEINTR &&
                errno != WSAEWOULDBLOCK)
#else
            if (status < 0 &&
                errno != EAGAIN &&
                errno != EINTR &&
                errno != EWOULDBLOCK)
#endif
            {
                DEBUG << "syscall failed" << std::endl;
                return BUSYBEE_POLLFAILED;
            }

#ifdef _MSC_VER
            if (status < 0 && errno == WSAEINTR)
#else
            if (status < 0 && errno == EINTR)
#endif
            {
                DEBUG << "syscall interrupted" << std::endl;
                return BUSYBEE_INTERRUPTED;
            }
#ifdef _MSC_VER
            if (status == 0)
#else
            if (status == 0 && m_timeout >= 0)
#endif
            {
                DEBUG << "syscall timed-out" << std::endl;
                return BUSYBEE_TIMEOUT;
            }

            DEBUG << "syscall EAGAIN or EWOULDBLOCK; continue" << std::endl;
            continue;
        }

        DEBUG << "received events from the syscall" << std::endl;

#ifdef BUSYBEE_MULTITHREADED
        if (fd == m_eventfdread.get())
        {
            DEBUG << "received events for eventfd" << std::endl;

            if ((events & EPOLLIN))
            {
                DEBUG << "event is EPOLLIN" << std::endl;
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
            DEBUG << "received events for listenfd" << std::endl;

            if ((events & EPOLLIN))
            {
                DEBUG << "event is EPOLLIN" << std::endl;
                work_accept();
            }

            continue;
        }
#endif // BUSYBEE_ACCEPT

#ifdef BUSYBEE_ST
#ifdef _MSC_VER
		if(m_external)
		{
			int i;

			for(i = 0; i < m_external->fd_count; ++i)
			{
				if(FD_ISSET(m_external->fd_array[i], &ee))
				{
					DEBUG << "received events for externalfd" << std::endl;
				}
			}
		}
#else
        if (fd == m_external)
        {
            DEBUG << "received events for externalfd" << std::endl;
            return BUSYBEE_EXTERNAL;
        }
#endif //_MSC_VER
#endif // BUSYBEE_ST

#ifdef _MSC_VER
		// Get the channel object
		DEBUG << "processing fd=" << (ee.fd_count > 0 ? ee.fd_array[0] : ff.fd_array[0]) << " as communication channel" << std::endl;
		channel& chan(m_channels[(size_t)(ee.fd_count > 0 ? ee.fd_array[0] : ff.fd_array[0])]);
#else
        // Get the channel object
        DEBUG << "processing fd=" << fd << " as communication channel" << std::endl;
        channel& chan(m_channels[fd]);
#endif // _MSC_VER

#ifdef BUSYBEE_MULTITHREADED
        po6::threads::mutex::hold hold(&chan.mtx);
#endif // BUSYBEE_MULTITHREADED

        if (chan.soc.get() < 0)
        {
            DEBUG << "channel is closed; skipping";
            continue;
        }

        bool need_close = false;
        bool quiet = true;

#ifndef _MSC_VER
		//For now, windows uses only synchronous sends.
        if ((events & EPOLLOUT))
        {
            DEBUG << "event is EPOLLOUT" << std::endl;
            work_send(&chan, &need_close, &quiet);

            DEBUG << "after processing EPOLLOUT, "
                  << "need_close=" << (need_close ? "true" : "false") << " "
                  << "quiet=" << (quiet ? "true" : "false") << std::endl;
        }
#endif // _MSC_VER

#ifdef _MSC_VER
		if(ee.fd_count > 0)
#else
        if ((events & EPOLLIN))
#endif // _MSC_VER
        {
            DEBUG << "event is EPOLLIN" << std::endl;
            work_recv(&chan, &need_close, &quiet);

            DEBUG << "after processing EPOLLIN, "
                  << "need_close=" << (need_close ? "true" : "false") << " "
                  << "quiet=" << (quiet ? "true" : "false") << std::endl;
        }

#ifdef _MSC_VER
		if(ff.fd_count > 0)
#else
        if ((events & EPOLLERR) || (events & EPOLLHUP))
#endif
        {
            need_close = true;
            quiet = false;

            DEBUG << "after processing errors, "
                  << "need_close=" << (need_close ? "true" : "false") << " "
                  << "quiet=" << (quiet ? "true" : "false") << std::endl;
        }

        DEBUG << "after processing all events, "
              << "need_close=" << (need_close ? "true" : "false") << " "
              << "quiet=" << (quiet ? "true" : "false") << std::endl;

        if (need_close)
        {
            DEBUG << "calling work_close" << std::endl;
            *id = chan.id;
            work_close(&chan);

            if (!quiet)
            {
                msg->reset();
                DEBUG << "not a quiet close, returning DISRUPTED" << std::endl;
                return BUSYBEE_DISRUPTED;
            }

            DEBUG << "quiet close" << std::endl;
        }
    }
}

#ifdef BUSYBEE_MULTITHREADED
void
CLASSNAME :: up_the_semaphore()
{
    ssize_t ret = m_eventfdwrite.write(&m_pipebuf, m_pause_count);
    assert(ret == m_pause_count);
}
#endif // BUSYBEE_MULTITHREADED

busybee_returncode
CLASSNAME :: get_channel(uint64_t server_id, channel** chan, uint64_t* chan_tag)
{
    DEBUG << "getting channel " << server_id << std::endl;

    if (m_server2channel.lookup(server_id, chan_tag))
    {
        DEBUG << "already established, using chan_tag " << *chan_tag << std::endl;
        *chan = &m_channels[(*chan_tag) % m_channels_sz];
        return BUSYBEE_SUCCESS;
    }

    *chan = NULL;

    try
    {
        po6::net::location dst;

        if (!m_mapper->lookup(server_id, &dst))
        {
            DEBUG << "no known mapping for " << server_id << std::endl;
            return BUSYBEE_DISRUPTED;
        }

        DEBUG << "mapping for " << server_id << " is " << dst << std::endl;
        po6::net::socket soc(dst.address.family(), SOCK_STREAM, IPPROTO_TCP);
        soc.connect(dst);
        DEBUG << "establishing new connection to " << dst << " fd=" << soc.get() << std::endl;
        *chan = &m_channels[soc.get()];
#ifdef BUSYBEE_MULTITHREADED
        po6::threads::mutex::hold hold(&(*chan)->mtx);
#endif // BUSYBEE_MULTITHREADED
        uint64_t new_tag = (*chan)->tag + m_channels_sz;
        *chan_tag = new_tag;
        void (channel::*func)(uint64_t) = &channel::reset;
        e::guard g = e::makeobjguard(**chan, func, new_tag);

        if (!setup_channel(&soc, *chan, new_tag))
        {
            DEBUG << "failed to setup the channel" << std::endl;
            return BUSYBEE_DISRUPTED;
        }

        assert((*chan)->tag == new_tag);
        (*chan)->id = server_id;
        set_mapping(server_id, (*chan)->tag);
        g.dismiss();
        DEBUG << "successfully created channel to " << server_id << std::endl;
        return BUSYBEE_SUCCESS;
    }
    catch (po6::error& e)
    {
        DEBUG << "exception thrown:  " << e << std::endl;
        return BUSYBEE_DISRUPTED;
    }
}

bool
CLASSNAME :: setup_channel(po6::net::socket* soc, channel* chan, uint64_t new_tag)
{
    DEBUG << "setting up a new channel" << std::endl;
    chan->reset(new_tag, soc);
#ifdef HAVE_SO_NOSIGPIPE
    int sigpipeopt = 1;
    chan->soc.set_sockopt(SOL_SOCKET, SO_NOSIGPIPE, &sigpipeopt, sizeof(sigpipeopt));
#endif // HAVE_SO_NOSIGPIPE
    chan->soc.set_tcp_nodelay();
    chan->state = channel::CONNECTED;
#ifdef _MSC_VER
	FD_SET(chan->soc.get(),&m_epoll);
#else

    if (add_event(chan->soc.get(),EPOLLIN|EPOLLOUT|EPOLLET) < 0)
    {
        DEBUG << "failed to add file descriptor to epoll" << std::endl;
        return false;
    }
#endif

    char buf[sizeof(uint32_t) + sizeof(uint64_t)];
    uint32_t sz = (sizeof(uint32_t) + sizeof(uint64_t)) | BBMSG_IDENTIFY;
    char* tmp = buf;
    tmp = e::pack32be(sz, tmp);
    tmp = e::pack64be(m_server_id, tmp);

    if (chan->soc.xwrite(buf, tmp - buf) != tmp - buf)
    {
        DEBUG << "failed to send IDENTIFY message" << std::endl;
        return false;
    }

    chan->soc.set_nonblocking();
    pollfd pfd;
    pfd.fd = chan->soc.get();
    pfd.events = POLLIN;
    pfd.revents = 0;

#ifdef _MSC_VER
    if (WSAPoll(&pfd, 1, 0) > 0)
#else
	if (poll(&pfd, 1, 0) > 0)
#endif
    {
        DEBUG << "there's already data available for reading, reading it now" << std::endl;
        bool need_close = false;
        bool quiet = false;
        work_recv(chan, &need_close, &quiet);

        if (need_close)
        {
            DEBUG << "need to close the channel; setup failed" << std::endl;
            return false;
        }
    }

    DEBUG << "setup succeeded" << std::endl;
    return true;
}

void
CLASSNAME :: set_mapping(uint64_t server_id, uint64_t chan_tag)
{
    m_server2channel.insert(server_id, chan_tag);
}

#ifdef BUSYBEE_ACCEPT
void
CLASSNAME :: work_accept()
{
    DEBUG << "accepting new connection";
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
#ifdef BUSYBEE_MULTITHREADED
        po6::threads::mutex::hold hold(&chan->mtx);
#endif // BUSYBEE_MULTITHREADED
        uint64_t new_tag = chan->tag + m_channels_sz;
        void (channel::*func)(uint64_t) = &channel::reset;
        e::guard g = e::makeobjguard(*chan, func, new_tag);

        if (!setup_channel(&soc, chan, new_tag))
        {
            DEBUG << "failed to setup the channel" << std::endl;
            return;
        }

        g.dismiss();
        DEBUG << "successfully accepted a new connection" << std::endl;
        return;
    }
    catch (po6::error &e)
    {
        DEBUG << "exception thrown:  " << e << std::endl;
        return;
    }
}
#endif // BUSYBEE_ACCEPT

void
CLASSNAME :: work_close(channel* chan)
{
    uint64_t old_tag;

    if (m_server2channel.lookup(chan->id, &old_tag) && chan->tag == old_tag)
    {
        m_server2channel.remove(chan->id);
    }

    DEBUG << "closing " << chan->tag << std::endl;
    uint64_t new_tag = chan->tag + m_channels_sz;
    chan->reset(new_tag);
}

void
CLASSNAME :: work_recv(channel* chan, bool* need_close, bool* quiet)
{
    // If you modify any return path of this function, keep the following in
    // mind:  If it's an error return, it must set *need_close and optionally
    // may set *quiet.  In these cases, there is no need to clean up because the
    // caller will reset the channel.  In a non-error case return, the
    // recv_queue of the channel must be appended to the global recv_queue.

    DEBUG << "entering work_recv" << std::endl;

    if (*need_close)
    {
        DEBUG << "fail:  already need_close" << std::endl;
        return;
    }

    assert(chan->soc.get() >= 0);
    assert(chan->recv_partial_header_sz <= 4);

    while (true)
    {
        DEBUG << "top of loop" << std::endl;
        uint8_t buf[IO_BLOCKSIZE];
        ssize_t rem;

        // Restore leftovers from last time.
        if (chan->recv_partial_header_sz)
        {
            DEBUG << "copy " << chan->recv_partial_header_sz << " bytes from last loop" << std::endl;
            memmove(buf, chan->recv_partial_header, chan->recv_partial_header_sz);
        }

        // Read into our temporary local buffer.
        int flags = 0;
#ifdef HAVE_MSG_NOSIGNAL
        flags |= MSG_NOSIGNAL;
#endif // HAVE_MSG_NOSIGNAL
        rem = chan->soc.recv(buf + chan->recv_partial_header_sz, IO_BLOCKSIZE - chan->recv_partial_header_sz, flags);
        DEBUG << "recv'd " << rem << " bytes" << std::endl;

#ifdef _MSC_VER
        errno = WSAGetLastError();

        if (rem < 0 && errno != WSAEINTR && errno != WSAEWOULDBLOCK)
#else
        if (rem < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
#endif
        {
            DEBUG << "fail:  received an error and it wasn't a kind one errno=" << errno << std::endl;
            *need_close = true;
            *quiet = false;
            return;
        }
        else if (rem < 0 && errno == EINTR)
        {
            continue;
        }
        else if (rem < 0)
        {
            DEBUG << "received EINTR, EAGAIN, or EWOULDBLOCK; our work here is done" << std::endl;

            if (chan->recv_queue)
            {
                DEBUG << "copying received messages to global queue" << std::endl;
#ifdef BUSYBEE_MULTITHREADED
                po6::threads::mutex::hold hold(&m_recv_lock);
#endif // BUSYBEE_MULTITHREADED
                *m_recv_end = chan->recv_queue;
                m_recv_end = chan->recv_end;
                chan->recv_queue = NULL;
                chan->recv_end = &chan->recv_queue;
            }

            return;
        }
        else if (rem == 0)
        {
            DEBUG << "recv'd 0 bytes, ending connection" << std::endl;

            switch (chan->state)
            {
                case channel::NOTCONNECTED:
                    abort();
                case channel::CONNECTED:
                    *need_close = true;
                    *quiet = false;
                    return;
                case channel::IDENTIFIED:
                case channel::FIN_SENT:
                case channel::FINACK_SENT:
                    *need_close = true;
                    *quiet = false;
                    return;
                default:
                    abort();
            }
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
                    chan->flags = BBMSG_FLAGS & sz;
                    sz = BBMSG_MASK & sz;
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
                    DEBUG << "processing completed message" << std::endl;
                    assert(chan->state != channel::NOTCONNECTED);

                    if (((chan->flags & BBMSG_FIN) || (chan->flags & BBMSG_ACK)) &&
                        chan->recv_partial_msg->size() != sizeof(uint32_t))
                    {
                        DEBUG << "closing channel because of an improperly sized FIN or ACK" << std::endl;
                        *need_close = true;
                        *quiet = false;
                        return;
                    }

                    if ((chan->flags & BBMSG_IDENTIFY))
                    {
                        if (chan->state == channel::CONNECTED)
                        {
                            DEBUG << "IDENTIFY message received" << std::endl;

                            if (chan->recv_partial_msg->size() != (sizeof(uint32_t) + sizeof(uint64_t)))
                            {
                                DEBUG << "IDENTIFY message not sized correctly: " << chan->recv_partial_msg->hex() << std::endl;
                                *need_close = true;
                                *quiet = false;
                                return;
                            }

                            uint64_t id;
                            e::unpack64be(chan->recv_partial_msg->data() + sizeof(uint32_t), &id);

                            if (chan->id == 0)
                            {
                                DEBUG << "IDENTIFY message specifies server_id=" << id << std::endl;
                                chan->id = id;
                                set_mapping(id, chan->tag);
                            }
                            else if (chan->id != id)
                            {
                                DEBUG << "IDENTIFY message specifies server_id=" << id << ", but channel set to " << chan->id << std::endl;
                                *need_close = true;
                                *quiet = false;
                                return;
                            }

                            DEBUG << "IDENTIFY success" << std::endl;
                            chan->state = channel::IDENTIFIED;
                            // XXX dedupe!
                        }
                        else
                        {
                            DEBUG << "IDENTIFY message received when already identified" << std::endl;
                            *need_close = true;
                            *quiet = false;
                            return;
                        }
                    }
                    else if ((chan->flags & BBMSG_FIN))
                    {
                        switch (chan->state)
                        {
                            case channel::IDENTIFIED:
                                if (!send_fin(chan))
                                {
                                    *need_close = true;
                                    *quiet = false;
                                    return;
                                }

                                // Intentional fall-through
                            case channel::FIN_SENT:
                                if (!send_ack(chan))
                                {
                                    *need_close = true;
                                    *quiet = false;
                                    return;
                                }

                                chan->state = channel::FINACK_SENT;
                                break;
                            case channel::CONNECTED:
                            case channel::FINACK_SENT:
                                *need_close = true;
                                *quiet = false;
                                return;
                            case channel::NOTCONNECTED:
                            default:
                                abort();
                        }
                    }
                    else if ((chan->flags & BBMSG_ACK))
                    {
                        switch (chan->state)
                        {
                            case channel::FINACK_SENT:
                                // shutdown quietly to clean up all state
                                *need_close = true;
                                return;
                            case channel::CONNECTED:
                            case channel::IDENTIFIED:
                            case channel::FIN_SENT:
                                *need_close = true;
                                *quiet = false;
                                return;
                            case channel::NOTCONNECTED:
                            default:
                                abort();
                        }
                    }
                    else
                    {
                        DEBUG << "received new message " << chan->recv_partial_msg->hex() << std::endl;
                        recv_message* tmp = new recv_message(NULL, chan->id, chan->recv_partial_msg);
                        *chan->recv_end = tmp;
                        chan->recv_end = &tmp->next;
                    }

                    chan->recv_partial_msg.reset();
                    chan->flags = 0;
                }
            }
        }
    }
}

void
CLASSNAME :: work_send(channel* chan, bool* need_close, bool* quiet)
{
    if (*need_close)
    {
        return;
    }

    assert(chan->soc.get() >= 0);

    while (chan->send_queue)
    {
        assert(!chan->send_progress.empty());

        int flags = 0;
#ifdef HAVE_MSG_NOSIGNAL
        flags |= MSG_NOSIGNAL;
#endif // HAVE_MSG_NOSIGNAL
#ifdef _MSC_VER
		//XXX:  Windows is level triggered, so just send synchronously.
		u_long mode = 0;
		if(ioctlsocket(chan->soc.get(), FIONBIO, &mode))
		{
			*need_close = true;
			*quiet = false;
		}
#endif
        ssize_t ret = chan->soc.send(chan->send_progress.data(), chan->send_progress.size(), flags);
#ifdef _MSC_VER
		mode = 1;
		if(ioctlsocket(chan->soc.get(), FIONBIO, &mode))
		{
			*need_close = true;
			*quiet = false;
		}
        errno = WSAGetLastError();
        if (ret < 0 && errno != WSAEINTR && errno != WSAEWOULDBLOCK)
#else
        if (ret < 0 && errno != EINTR && errno != EWOULDBLOCK)
#endif
        {
            *need_close = true;
            *quiet = false;
            return;
        }
        else if (ret <= 0 && errno == EINTR)
        {
            continue;
        }
        else if (ret <= 0)
        {
            return;
        }
        else
        {
            chan->send_progress.advance(ret);

            if (chan->send_progress.empty())
            {
                assert(chan->send_queue);

                if (chan->send_queue->next)
                {
                    send_message* tmp = chan->send_queue;
                    chan->send_queue = chan->send_queue->next;
                    chan->send_progress = chan->send_queue->msg->as_slice();
                    delete tmp;
                }
                else
                {
                    delete chan->send_queue;
                    chan->send_queue = NULL;
                    chan->send_end = &chan->send_queue;
                    chan->send_progress = e::slice(NULL, 0);
                }

            }
        }
    }

    return;
}

bool
CLASSNAME :: send_fin(channel* chan)
{
    std::auto_ptr<e::buffer> msg(e::buffer::create(sizeof(uint32_t)));
    msg->pack_at(0) << static_cast<uint32_t>(BBMSG_FIN | sizeof(uint32_t));
    bool empty = !chan->send_queue;
    send_message* tmp = new send_message(NULL, msg);
    *chan->send_end = tmp;
    chan->send_end = &tmp->next;

    if (empty)
    {
        chan->send_progress = tmp->msg->as_slice();
    }

    bool need_close;
    bool quiet;
    work_send(chan, &need_close, &quiet);
    return need_close;
}

bool
CLASSNAME :: send_ack(channel* chan)
{
    std::auto_ptr<e::buffer> msg(e::buffer::create(sizeof(uint32_t)));
    msg->pack_at(0) << static_cast<uint32_t>(BBMSG_ACK | sizeof(uint32_t));
    bool empty = !chan->send_queue;
    send_message* tmp = new send_message(NULL, msg);
    *chan->send_end = tmp;
    chan->send_end = &tmp->next;

    if (empty)
    {
        chan->send_progress = tmp->msg->as_slice();
    }

    bool need_close;
    bool quiet;
    work_send(chan, &need_close, &quiet);
    return need_close;
}

#ifdef HAVE_EPOLL_CTL
int
CLASSNAME :: wait_event(int* fd, uint32_t* events)
{
    epoll_event ee;
    int ret = epoll_pwait(m_epoll.get(), &ee, 1, m_timeout, &m_sigmask);
    *fd = ee.data.fd;
    *events = ee.events;
    return ret;
}
#elif HAVE_KQUEUE
int
CLASSNAME :: wait_event(int* fd, uint32_t* events)
{
    struct kevent ee;
    struct timespec to = {0,0}; 
    if (m_timeout < 0) 
        to.tv_nsec = 50000;
    else
        to.tv_nsec = m_timeout * 1000;

    int ret = kevent(m_epoll.get(), NULL, 0, &ee, 1, &to);
    *fd = ee.ident;

    switch(ee.filter)
    {
        case EVFILT_READ:
            *events = EPOLLIN;
            break;
        case EVFILT_WRITE:
            *events = EPOLLOUT;
            break;
        default:
            *events = EPOLLERR;
    }

    return ret;
}
#endif
