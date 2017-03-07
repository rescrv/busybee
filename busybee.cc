// Copyright (c) 2012-2016, Cornell University
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

#if HAVE_CONFIG_H
# include "config.h"
#endif

// POSIX
#include <ifaddrs.h>
#include <poll.h>
#include <sys/types.h>

// Google SparseHash
#include <google/dense_hash_map>

// po6
#include <po6/threads/mutex.h>

// e
#include <e/array_ptr.h>
#include <e/endian.h>
#include <e/flagfd.h>
#include <e/guard.h>
#include <e/intrusive_ptr.h>
#include <e/nwf_hash_map.h>

// BusyBee
#include <busybee.h>
#include "busybee-internal.h"

#ifdef HAVE_MSG_NOSIGNAL
#define SENDRECV_FLAGS (MSG_NOSIGNAL)
#else
#define SENDRECV_FLAGS 0
#endif // HAVE_MSG_NOSIGNAL

#define IO_BLOCKSIZE 4096

#define HEADER_SIZE_MASK 0x00ffffffULL
#define HEADER_IDENTIFY  0x80000000ULL
#define HEADER_EXTENDED  0x40000000ULL

#define MAX_UNEXTENDED_MSG_SIZE (UINT32_MAX & HEADER_SIZE_MASK)

#define CHANNEL_EXTERNAL ((channel*)-1)

BEGIN_BUSYBEE_NAMESPACE

struct message
{
    message() : next(NULL), payload(NULL), id(0) {}
    message(e::buffer* _payload, uint64_t _id) : next(NULL), payload(_payload), id(_id) {}
    ~message() throw () {}

    message* next;
    e::buffer* payload;
    uint64_t id;

    private:
        message(const message&);
        message& operator = (const message&);
};

void queue_init(message** queue, message*** end);
void queue_push(message*** end, message* msg);
void queue_splice(message*** onto_end,
                  message** from_queue, message*** from_end);
void queue_remove_head(message** queue, message*** end);
void queue_cleanup(message* queue, message** end);

#define CHAN_SEND_EDGE_IN_USERSPACE (1ULL << 0)
#define CHAN_RECV_EDGE_IN_USERSPACE (1ULL << 1)
#define CHAN_CLOSE_NEEDED           (1ULL << 2)
#define CHAN_IDENTIFIED             (1ULL << 3)
#define CHAN_CLOSED                 (1ULL << 4)
#define CHAN_SENDER_HAS_IT          (1ULL << 5)
#define CHAN_RECVER_HAS_IT          (1ULL << 6)

class channel
{
    public:
        channel();
        ~channel() throw ();

    public:
        // initialize the channel (call one once)
        busybee_returncode connect(uint64_t local, uint64_t remote, const po6::net::location& where);
        busybee_returncode accept(uint64_t local, po6::net::socket* listener);
        // metadata while setting up the channel
        int fd();
        // steady state ops
        uint64_t remote();
        void deanonymize_remote(uint64_t remote);
        void enqueue(std::auto_ptr<e::buffer> msg);
        busybee_returncode address(po6::net::location* addr);
        // may return TIME_TO_CLOSE
        busybee_returncode do_work_send();
        // may return NEWLY_IDENTIFIED, TIME_TO_CLOSE
        busybee_returncode do_work_recv(message*** recv_end);
        // may return TIME_TO_CLOSE
        busybee_returncode do_work_close();

    private:
        busybee_returncode setup();
        void enqueue_identify();

    // these calls should only be occupied by one thread at a time
    // errors will be set in flags; otherwise SUCCESS should be assumed
    // CHAN_*_EDGE_IN_USERSPACE assumed cleared for each work call
    private:
        void work_send();
        void work_recv(message*** recv_end);

    private:
        // this function is basically an inner part of work_recv extracted for
        // readability; it shouldn't be called from anywhere else
        // return true if all went well, false if error flag set and work_recv
        // should return
        bool new_message(message*** recv_end);

    private:
        po6::net::socket m_sock;
        // synchronize with atomic operations
        uint64_t m_flags;
        // must synchronize with m_meta_mtx
        po6::threads::mutex m_meta_mtx;
        uint64_t m_remote;
        uint64_t m_local;
        enum { CONNECT, ACCEPT } m_id_method;
        // must synchronize with m_send_mtx
        // only work_send can manipulate the head of the queue (although it will
        // do so with the lock, so any thread can check if the queue is empty so
        // long as it holds m_send_mtx)
        po6::threads::mutex m_send_mtx;
        message* m_send_queue;
        message** m_send_end;
        // used only by thread permitted into work_send
        uint8_t* m_send_ptr;
        // used only by thread permitted into work_recv
        unsigned m_recv_partial_header_sz;
        uint8_t m_recv_partial_header[BUSYBEE_HEADER_SIZE];
        std::auto_ptr<e::buffer> m_recv_msg;
        uint32_t m_recv_head;

    private:
        channel(const channel&);
        channel& operator = (const channel&);
};

class server : public busybee_server
{
    public:
        server(busybee_controller* controller,
               uint64_t server_id,
               const po6::net::location& bind_to,
               e::garbage_collector* gc);
        virtual ~server() throw ();

    public:
        busybee_returncode init();
        virtual bool deliver(uint64_t server_id, std::auto_ptr<e::buffer> msg);
        virtual busybee_returncode send(uint64_t server_id,
                                        std::auto_ptr<e::buffer> msg);
        virtual busybee_returncode recv(e::garbage_collector::thread_state* ts,
                                        int timeout, uint64_t* server_id,
                                        std::auto_ptr<e::buffer>* msg);
        virtual busybee_returncode recv_no_msg(e::garbage_collector::thread_state* ts,
                                               int timeout, uint64_t* server_id);
        virtual busybee_returncode get_addr(uint64_t server_id, po6::net::location* addr);
        virtual busybee_returncode shutdown();

    private:
        channel* get_channel(uint64_t server_id, busybee_returncode* rc);
        int accept_connection();
        busybee_returncode work_dispatch(int fd, uint32_t events, uint64_t* server_id);
        busybee_returncode work_send(channel* chan);
        busybee_returncode work_close(channel* chan);

    private:
        const uint64_t m_server_id;
        const po6::net::location m_bind_to;
        e::garbage_collector* m_gc;
        busybee_controller* m_controller;
        poller* m_poll;
        po6::net::socket m_listen;
        channel** m_channels;
        size_t m_channels_sz;
        e::nwf_hash_map<uint64_t, channel*, e::lookup3_64_ref> m_server2channel;
        uint64_t m_anonymous;
        po6::threads::mutex m_recv_mtx;
        bool m_shutdown;
        message* m_recv_queue;
        message** m_recv_end;
        e::flagfd m_recv_flag;

    private:
        server(const server&);
        server& operator = (const server&);
};

class client : public busybee_client
{
    public:
        client(busybee_controller* controller);
        virtual ~client() throw ();

    public:
        busybee_returncode init();
        virtual bool deliver(uint64_t client_id, std::auto_ptr<e::buffer> msg);
        virtual busybee_returncode send(uint64_t server_id,
                                        std::auto_ptr<e::buffer> msg);
        virtual busybee_returncode recv(int timeout, uint64_t* server_id,
                                        std::auto_ptr<e::buffer>* msg);
        virtual busybee_returncode recv_no_msg(int timeout, uint64_t* server_id);
        virtual int poll_fd() { return m_poll->poll_fd(); }
        virtual busybee_returncode set_external_fd(int fd);
        virtual busybee_returncode reset();

    private:
        channel* get_channel(uint64_t server_id, busybee_returncode* rc);
        busybee_returncode work_dispatch(int fd, uint32_t events, uint64_t* server_id);
        busybee_returncode work_send(channel* chan);
        busybee_returncode work_close(channel* chan);

    private:
        busybee_controller* m_controller;
        poller* m_poll;
        channel** m_channels;
        size_t m_channels_sz;
        google::dense_hash_map<uint64_t, channel*> m_server2channel;
        message* m_recv_queue;
        message** m_recv_end;

    private:
        client(const client&);
        client& operator = (const client&);
};

class single : public busybee_single
{
    public:
        single(const po6::net::hostname& host);
        single(const po6::net::location& host);
        virtual ~single() throw ();

    public:
        virtual int poll_fd();
        virtual busybee_returncode send(std::auto_ptr<e::buffer> msg);
        virtual busybee_returncode recv(int timeout, std::auto_ptr<e::buffer>* msg);

    private:
        busybee_returncode setup();

    private:
        po6::net::hostname m_host;
        po6::net::location m_addr;
        enum { HOSTNAME, LOCATION } m_method;
        std::auto_ptr<channel> m_chan;
        message* m_recv_queue;
        message** m_recv_end;

    private:
        single(const single&);
        single& operator = (const single&);
};

END_BUSYBEE_NAMESPACE

using busybee::poller;
using busybee::channel;
using busybee::server;
using busybee::client;
using busybee::single;

poller :: poller()
{
}

poller :: ~poller() throw ()
{
}

void
busybee :: queue_init(message** queue, message*** end)
{
    *queue = NULL;
    *end = queue;
}

void
busybee :: queue_push(message*** end, message* msg)
{
    **end = msg;
    *end = &msg->next;
}

void
busybee :: queue_splice(message*** onto_end,
                        message** from_queue, message*** from_end)
{
#if 0
    **onto_end = *from_queue;
    *onto_end = *from_end;
    queue_init(from_queue, from_end);
#endif
    while (*from_queue)
    {
        queue_push(onto_end, *from_queue);
        *from_queue = (*from_queue)->next;
    }
    queue_init(from_queue, from_end);
}

void
busybee :: queue_remove_head(message** queue, message*** end)
{
    message* pop = *queue;

    if (pop->next)
    {
        *queue = pop->next;
    }
    else
    {
        *queue = NULL;
        *end = queue;
    }
}

void
busybee :: queue_cleanup(message* queue, message** end)
{
    while (queue)
    {
        message* head = queue;
        delete queue->payload;
        queue_remove_head(&queue, &end);
        delete head;
    }
}

channel :: channel()
    : m_sock()
    , m_flags()
    , m_meta_mtx()
    , m_remote(0)
    , m_local(0)
    , m_id_method()
    , m_send_mtx()
    , m_send_queue(NULL)
    , m_send_end(NULL)
    , m_send_ptr(NULL)
    , m_recv_partial_header_sz(0)
    , m_recv_msg()
    , m_recv_head(0)
{
    DEBUG("channel " << (void*)this << " created");
    e::atomic::store_64_release(&m_flags, 0);
    queue_init(&m_send_queue, &m_send_end);
}

channel :: ~channel() throw ()
{
    DEBUG("channel " << (void*)this << " destroyed");
    po6::threads::mutex::hold m_hold(&m_send_mtx);
    queue_cleanup(m_send_queue, m_send_end);
}

busybee_returncode
channel :: connect(uint64_t local, uint64_t remote, const po6::net::location& where)
{
    DEBUG("channel " << (void*)this << " connecting from " << local << " to " << remote << "/" << where);
    m_meta_mtx.lock();
    m_local = local;
    m_remote = remote;
    m_id_method = CONNECT;
    m_meta_mtx.unlock();

    if (!m_sock.reset(where.address.family(), SOCK_STREAM, IPPROTO_TCP) ||
        !m_sock.set_nonblocking() ||
        (!m_sock.connect(where) && errno != EINPROGRESS))
    {
        DEBUG("channel " << (void*)this << " connection failed");
        return BUSYBEE_DISRUPTED;
    }

#ifdef BUSYBEE_DEBUG
    po6::net::location loc;
    (void) m_sock.getsockname(&loc);
#endif // BUSYBEE_DEBUG
    DEBUG("channel " << (void*)this << " local address is " << loc);
    enqueue_identify();
    return setup();
}

busybee_returncode
channel :: accept(uint64_t local, po6::net::socket* listener)
{
    DEBUG("channel " << (void*)this << " accepting connection for " << local);

    if (!listener->accept(&m_sock) || m_sock.get() < 0)
    {
        DEBUG("channel " << (void*)this << " accept failed: " << po6::strerror(errno));
        return BUSYBEE_SEE_ERRNO;
    }

    m_meta_mtx.lock();
    m_local = local;
    m_id_method = ACCEPT;
    m_meta_mtx.unlock();
    return setup();
}

busybee_returncode
channel :: setup()
{
#ifdef HAVE_SO_NOSIGPIPE
    int sigpipeopt = 1;
    DEBUG("channel " << (void*)this << " configured with HAVE_SO_NOSIGPIPE");

    if (!m_sock.set_sockopt(SOL_SOCKET, SO_NOSIGPIPE, &sigpipeopt, sizeof(sigpipeopt)))
    {
        DEBUG("channel " << (void*)this << " HAVE_SO_NOSIGPIPE option failed");
        return BUSYBEE_DISRUPTED;
    }
#endif // HAVE_SO_NOSIGPIPE

    if (!m_sock.set_tcp_nodelay() ||
        !m_sock.set_nonblocking())
    {
        DEBUG("channel " << (void*)this << " set options failed");
        return BUSYBEE_DISRUPTED;
    }

    DEBUG("channel " << (void*)this << " fully setup");
    return BUSYBEE_SUCCESS;
}

void
channel :: enqueue_identify()
{
    m_meta_mtx.lock();
    uint64_t local = m_local;
    uint64_t remote = m_remote;
    m_meta_mtx.unlock();
    if (BUSYBEE_IS_ANONYMOUS(local)) local = 0;
    if (BUSYBEE_IS_ANONYMOUS(remote)) remote = 0;
    DEBUG("channel " << (void*)this << " identifying local=" << local << " remote=" << remote);
    const size_t sz = BUSYBEE_HEADER_SIZE + 2 * sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << local << remote;
    msg->pack() << uint32_t(msg->size() | HEADER_IDENTIFY);
    std::auto_ptr<message> m(new message(msg.get(), 0));
    msg.release();
    m_send_mtx.lock();
    queue_push(&m_send_end, m.get());
    m_send_mtx.unlock();
    m.release();
}

int
channel :: fd()
{
    e::atomic::load_64_acquire(&m_flags);
    return m_sock.get();
}

uint64_t
channel :: remote()
{
    po6::threads::mutex::hold hold(&m_meta_mtx);
    return m_remote;
}

void
channel :: deanonymize_remote(uint64_t remote)
{
    po6::threads::mutex::hold hold(&m_meta_mtx);
    assert(m_remote == 0);
    m_remote = remote;
}

void
channel :: enqueue(std::auto_ptr<e::buffer> msg)
{
    assert(msg->size() >= BUSYBEE_HEADER_SIZE);
    assert(msg->size() <= BUSYBEE_MAX_MSG_SIZE);
    message* msg_queue;
    message** msg_end;
    queue_init(&msg_queue, &msg_end);

    if (msg->size() > MAX_UNEXTENDED_MSG_SIZE)
    {
        DEBUG("channel " << (void*)this << " enqueuing " << msg->size() << "B extended message");
        const size_t emsg_sz = BUSYBEE_HEADER_SIZE + sizeof(uint64_t);
        std::auto_ptr<e::buffer> emsg(e::buffer::create(emsg_sz));
        e::pack32be(emsg_sz | HEADER_EXTENDED, emsg->data());
        e::pack64be(msg->size(), emsg->data() + BUSYBEE_HEADER_SIZE);
        std::auto_ptr<message> m(new message(emsg.get(), 0));
        emsg.release();
        queue_push(&msg_end, m.get());
        m.release();
        e::pack32be(0, msg->data());
    }
    else
    {
        DEBUG("channel " << (void*)this << " enqueuing " << msg->size() << "B message");
        assert((msg->size() & MAX_UNEXTENDED_MSG_SIZE) == msg->size());
        e::pack32be(msg->size(), msg->data());
    }

    std::auto_ptr<message> m(new message(msg.get(), 0));
    msg.release();
    queue_push(&msg_end, m.get());
    m.release();

    po6::threads::mutex::hold hold(&m_send_mtx);
    queue_splice(&m_send_end, &msg_queue, &msg_end);
}

busybee_returncode
channel :: address(po6::net::location* addr)
{
    if (m_sock.getpeername(addr))
    {
        return BUSYBEE_SUCCESS;
    }
    else
    {
        return BUSYBEE_DISRUPTED;
    }
}

busybee_returncode
channel :: do_work_send()
{
    DEBUG("channel " << (void*)this << " entering do_work_send");

    while (true)
    {
        DEBUG("channel " << (void*)this << " do_work_send top of loop");
        uint64_t flags = e::atomic::load_64_acquire(&m_flags);
        uint64_t new_flags = 0;

        while (true)
        {
            if ((flags & CHAN_CLOSE_NEEDED))
            {
                DEBUG("channel " << (void*)this << " do_work_send->do_work_close");
                return do_work_close();
            }
            else if ((flags & CHAN_CLOSED))
            {
                DEBUG("channel " << (void*)this << " do_work_send->BUSYBEE_DIRUPTED");
                return BUSYBEE_DISRUPTED;
            }

            new_flags = flags;

            // if a sender already has it, set CHAN_SEND_EDGE_IN_USERSPACE
            if ((flags & CHAN_SENDER_HAS_IT))
            {
                DEBUG("channel " << (void*)this << " do_work_send trying to pass work to other thread");
                new_flags |= CHAN_SEND_EDGE_IN_USERSPACE;
            }
            // else set CHAN_SENDER_HAS_IT, clear CHAN_SEND_EDGE_IN_USERSPACE
            else
            {
                DEBUG("channel " << (void*)this << " do_work_send trying to claim work_send");
                new_flags |= CHAN_SENDER_HAS_IT;
                new_flags &= ~(uint64_t)CHAN_SEND_EDGE_IN_USERSPACE;
            }

            uint64_t witnessed = e::atomic::compare_and_swap_64_acquire(&m_flags, flags, new_flags);

            if (witnessed == flags)
            {
                break;
            }

            flags = witnessed;
        }

        if ((flags & CHAN_SENDER_HAS_IT))
        {
            // another thread is working this code
            DEBUG("channel " << (void*)this << " do_work_send passing to other thread");
            return BUSYBEE_SUCCESS;
        }

        assert(new_flags & CHAN_SENDER_HAS_IT);
        work_send();
        flags = e::atomic::load_64_acquire(&m_flags);

        while (true)
        {
            new_flags = flags & ~(uint64_t)CHAN_SENDER_HAS_IT;
            uint64_t witnessed = e::atomic::compare_and_swap_64_acquire(&m_flags, flags, new_flags);

            if (witnessed == flags)
            {
                break;
            }

            flags = witnessed;
        }

        DEBUG("channel " << (void*)this << " do_work_send cleared CHAN_SENDER_HAS_IT");

        if ((flags & CHAN_CLOSE_NEEDED))
        {
            DEBUG("channel " << (void*)this << " do_work_send->do_work_close");
            return do_work_close();
        }
        else if ((flags & CHAN_CLOSED))
        {
            DEBUG("channel " << (void*)this << " do_work_send->BUSYBEE_DIRUPTED");
            return BUSYBEE_DISRUPTED;
        }
        else if (!(flags & CHAN_SEND_EDGE_IN_USERSPACE))
        {
            // The userspace buffer is completely flushed to kernel space,
            // and the sender has it flag has been cleared; time to return
            return BUSYBEE_SUCCESS;
        }
        else if ((flags & CHAN_SEND_EDGE_IN_USERSPACE))
        {
            // This could be because we hit the bottom of work_send and
            // flushed the userspace buffer before filling the kernel space
            // buffer, or it could be because another thread got an event
            // and changed the flags during this thread's execution.
            //
            // Distinguishing between the two comes down to whether or not
            // the send queue is empty.  This check needs to happen here,
            // otherwise there is a race condition between clearing the
            // userspace send queue and clearing the CHAN_SENDER_HAS_IT
            // flag.  It's not ideal, but it works.
            po6::threads::mutex::hold hold(&m_send_mtx);

            if (!m_send_queue)
            {
                return BUSYBEE_SUCCESS;
            }
        }
    }
}

busybee_returncode
channel :: do_work_recv(message*** recv_end)
{
    DEBUG("channel " << (void*)this << " entering do_work_recv");
    bool identified = true;

    while (true)
    {
        DEBUG("channel " << (void*)this << " do_work_recv top of loop");
        uint64_t flags = e::atomic::load_64_acquire(&m_flags);
        uint64_t new_flags = 0;

        while (true)
        {
            if ((flags & CHAN_CLOSE_NEEDED))
            {
                DEBUG("channel " << (void*)this << " do_work_recv->do_work_close");
                return do_work_close();
            }
            else if ((flags & CHAN_CLOSED))
            {
                DEBUG("channel " << (void*)this << " do_work_recv->BUSYBEE_DIRUPTED");
                return BUSYBEE_DISRUPTED;
            }

            new_flags = flags;

            // if a recver already has it, set CHAN_RECV_EDGE_IN_USERSPACE
            if ((flags & CHAN_RECVER_HAS_IT))
            {
                DEBUG("channel " << (void*)this << " do_work_recv trying to pass work to other thread");
                new_flags |= CHAN_RECV_EDGE_IN_USERSPACE;
            }
            // else set CHAN_RECVER_HAS_IT, clear CHAN_RECV_EDGE_IN_USERSPACE
            else
            {
                DEBUG("channel " << (void*)this << " do_work_recv trying to claim work_recv");
                new_flags |= CHAN_RECVER_HAS_IT;
                new_flags &= ~(uint64_t)CHAN_RECV_EDGE_IN_USERSPACE;
            }

            uint64_t witnessed = e::atomic::compare_and_swap_64_acquire(&m_flags, flags, new_flags);

            if (witnessed == flags)
            {
                break;
            }

            flags = witnessed;
        }

        if ((flags & CHAN_RECVER_HAS_IT))
        {
            // another thread is working this code
            DEBUG("channel " << (void*)this << " do_work_recv passing to other thread");
            return BUSYBEE_SUCCESS;
        }

        identified = identified && (flags & CHAN_IDENTIFIED);
        assert(new_flags & CHAN_RECVER_HAS_IT);
        work_recv(recv_end);
        flags = e::atomic::load_64_acquire(&m_flags);

        while (true)
        {
            new_flags = flags & ~(uint64_t)CHAN_RECVER_HAS_IT;
            uint64_t witnessed = e::atomic::compare_and_swap_64_acquire(&m_flags, flags, new_flags);

            if (witnessed == flags)
            {
                break;
            }

            flags = witnessed;
        }

        DEBUG("channel " << (void*)this << " do_work_recv cleared CHAN_RECVER_HAS_IT");

        if ((flags & CHAN_CLOSE_NEEDED))
        {
            DEBUG("channel " << (void*)this << " do_work_recv->do_work_close");
            return do_work_close();
        }
        else if ((flags & CHAN_CLOSED))
        {
            DEBUG("channel " << (void*)this << " do_work_recv->BUSYBEE_DIRUPTED");
            return BUSYBEE_DISRUPTED;
        }
        else if (!(flags & CHAN_RECV_EDGE_IN_USERSPACE))
        {
            return !identified && (flags & CHAN_IDENTIFIED)
                 ? (busybee_returncode)INTERNAL_NEWLY_IDENTIFIED
                 : BUSYBEE_SUCCESS;
        }
    }
}

busybee_returncode
channel :: do_work_close()
{
    uint64_t flags = e::atomic::load_64_acquire(&m_flags);
    uint64_t new_flags = 0;

    while (true)
    {
        flags &= ~(uint64_t)(CHAN_SENDER_HAS_IT|CHAN_RECVER_HAS_IT);
        flags |= CHAN_CLOSE_NEEDED;
        new_flags = flags | CHAN_CLOSED;
        new_flags &= ~(uint64_t)CHAN_CLOSE_NEEDED;
        uint64_t witnessed = e::atomic::compare_and_swap_64_acquire(&m_flags, flags, new_flags);

        if ((witnessed & CHAN_CLOSED))
        {
            // someone else closed
            return BUSYBEE_DISRUPTED;
        }
        else if (flags == witnessed)
        {
            shutdown(m_sock.get(), SHUT_WR);
            // this execution transitioned from CLOSE_NEEDED to CLOSED
            return (busybee_returncode)INTERNAL_TIME_TO_CLOSE;
        }
        else if ((witnessed & CHAN_SENDER_HAS_IT) || (witnessed & CHAN_RECVER_HAS_IT))
        {
            // another thread will eventually run this code
            return BUSYBEE_DISRUPTED;
        }
        else
        {
            flags = witnessed;
        }
    }
}

void
channel :: work_send()
{
    DEBUG("channel " << (void*)this << " entering work_send");
    m_send_mtx.lock();

    while (m_send_queue)
    {
        e::buffer* payload = m_send_queue->payload;
        m_send_mtx.unlock();
        DEBUG("channel " << (void*)this << " work_send top of loop");

        if (!m_send_ptr)
        {
            m_send_ptr = payload->data();
        }

        assert(m_send_ptr >= payload->data());
        assert(m_send_ptr < payload->data() + payload->size());
        const size_t amt = payload->data() + payload->size() - m_send_ptr;
        ssize_t ret = m_sock.send(m_send_ptr, amt, SENDRECV_FLAGS);

        if (ret < 0 &&
            errno != EINTR &&
            errno != EAGAIN &&
            errno != EWOULDBLOCK)
        {
            DEBUG("channel " << (void*)this << " work_send error out " << po6::strerror(errno));
            e::atomic::or_64_nobarrier(&m_flags, CHAN_CLOSE_NEEDED);
            return;
        }
        else if (ret < 0 && errno == EINTR)
        {
            DEBUG("channel " << (void*)this << " work_send interrupted");
            continue;
        }
        else if (ret < 0) // EAGAIN/EWOULDBLOCK
        {
            DEBUG("channel " << (void*)this << " work_send would block");
            // edge is clear here
            return;
        }
        else if (ret == 0)
        {
            DEBUG("channel " << (void*)this << " work_send sent 0");
            e::atomic::or_64_nobarrier(&m_flags, CHAN_CLOSE_NEEDED);
            return;
        }

        m_send_ptr += ret;

        if (m_send_ptr >= payload->data() + payload->size())
        {
            m_send_mtx.lock();
            DEBUG("channel " << (void*)this << " work_send finished sending one message");
            message* head = m_send_queue;
            queue_remove_head(&m_send_queue, &m_send_end);
            m_send_mtx.unlock();
            m_send_ptr = NULL;
            delete head->payload;
            delete head;
        }

        m_send_mtx.lock();
    }

    m_send_mtx.unlock();
    // edge is not cleared here, but there is no more work
    e::atomic::or_64_nobarrier(&m_flags, CHAN_SEND_EDGE_IN_USERSPACE);
    return;
}

void
channel :: work_recv(message*** recv_end)
{
    DEBUG("channel " << (void*)this << " entering work_recv");

    while (true)
    {
        DEBUG("channel " << (void*)this << " work_recv top of loop");
        uint8_t buf[IO_BLOCKSIZE];

        if (m_recv_partial_header_sz)
        {
            memmove(buf, m_recv_partial_header, m_recv_partial_header_sz);
        }

        const size_t amt = IO_BLOCKSIZE - m_recv_partial_header_sz;
        ssize_t ret = m_sock.recv(buf + m_recv_partial_header_sz, amt, SENDRECV_FLAGS);

        if (ret < 0 &&
            errno != EINTR &&
            errno != EAGAIN &&
            errno != EWOULDBLOCK)
        {
            DEBUG("channel " << (void*)this << " work_recv error out " << po6::strerror(errno));
            e::atomic::or_64_nobarrier(&m_flags, CHAN_CLOSE_NEEDED);
            return;
        }
        else if (ret < 0 && errno == EINTR)
        {
            DEBUG("channel " << (void*)this << " work_recv interrupted");
            continue;
        }
        else if (ret < 0) // EAGAIN/EWOULDBLOCK
        {
            DEBUG("channel " << (void*)this << " work_recv would block");
            return;
        }
        else if (ret == 0)
        {
            DEBUG("channel " << (void*)this << " work_recv recv 0");
            e::atomic::or_64_nobarrier(&m_flags, CHAN_CLOSE_NEEDED);
            return;
        }

        // adjust ret to include the data from the partial header
        ret += m_recv_partial_header_sz;
        m_recv_partial_header_sz = 0;
        uint8_t* data = buf;

        while (ret > 0)
        {
            if (!m_recv_msg.get())
            {
                if (ret < BUSYBEE_HEADER_SIZE)
                {
                    memmove(m_recv_partial_header, data, ret);
                    m_recv_partial_header_sz = ret;
                    ret = 0;
                    continue;
                }

                e::unpack32be(data, &m_recv_head);
                const uint32_t sz = m_recv_head & HEADER_SIZE_MASK;

                if (sz < BUSYBEE_HEADER_SIZE)
                {
                    e::atomic::or_64_nobarrier(&m_flags, CHAN_CLOSE_NEEDED);
                    return;
                }

                m_recv_msg.reset(e::buffer::create(sz));
                m_recv_msg->resize(BUSYBEE_HEADER_SIZE);
                memmove(m_recv_msg->data(), data, BUSYBEE_HEADER_SIZE);
                ret -= BUSYBEE_HEADER_SIZE;
                data += BUSYBEE_HEADER_SIZE;
            }

            size_t sz = m_recv_msg->capacity() - m_recv_msg->size();
            sz = std::min(static_cast<size_t>(ret), sz);
            ret -= sz;
            memmove(m_recv_msg->data() + m_recv_msg->size(), data, sz);
            m_recv_msg->resize(m_recv_msg->size() + sz);
            data += sz;

            if (m_recv_msg->size() == m_recv_msg->capacity())
            {
                if (!new_message(recv_end))
                {
                    return;
                }
            }
        }
    }
}

bool
channel :: new_message(message*** recv_end)
{
    if ((m_recv_head & HEADER_IDENTIFY))
    {
        if ((m_recv_head & HEADER_SIZE_MASK) != BUSYBEE_HEADER_SIZE + 2 * sizeof(uint64_t))
        {
            e::atomic::or_64_nobarrier(&m_flags, CHAN_CLOSE_NEEDED);
            return false;
        }

        uint64_t remote;
        uint64_t local;
        e::unpack64be(m_recv_msg->data() + BUSYBEE_HEADER_SIZE, &remote);
        e::unpack64be(m_recv_msg->data() + BUSYBEE_HEADER_SIZE + sizeof(uint64_t), &local);
        bool send_identify = false;

        if ((e::atomic::load_64_nobarrier(&m_flags) & CHAN_IDENTIFIED))
        {
            po6::threads::mutex::hold hold(&m_meta_mtx);

            if ((local != 0 && m_local != 0 && m_local != local) ||
                (remote != 0 && m_remote != 0 && m_remote != remote))
            {
                e::atomic::or_64_nobarrier(&m_flags, CHAN_CLOSE_NEEDED);
                return false;
            }
        }
        else
        {
            e::atomic::or_64_nobarrier(&m_flags, CHAN_IDENTIFIED);
            po6::threads::mutex::hold hold(&m_meta_mtx);

            if (m_id_method == CONNECT)
            {
                if ((local != 0 && m_local != 0 && m_local != local) ||
                    (remote != 0 && m_remote != 0 && m_remote != remote))
                {
                    e::atomic::or_64_nobarrier(&m_flags, CHAN_CLOSE_NEEDED);
                    return false;
                }
            }
            else if (m_id_method == ACCEPT)
            {
                if ((local != 0 && m_local != 0 && m_local != local))
                {
                    e::atomic::or_64_nobarrier(&m_flags, CHAN_CLOSE_NEEDED);
                    return false;
                }

                m_remote = remote;
                send_identify = true;
            }
            else
            {
                abort();
            }
        }

        if (send_identify)
        {
            enqueue_identify();
        }

        m_recv_msg.reset();
        m_recv_partial_header_sz = 0;
        m_recv_head = 0;
    }
    else if ((m_recv_head & HEADER_EXTENDED))
    {
        if ((m_recv_head & HEADER_SIZE_MASK) != BUSYBEE_HEADER_SIZE + sizeof(uint64_t))
        {
            e::atomic::or_64_nobarrier(&m_flags, CHAN_CLOSE_NEEDED);
            return false;
        }

        uint64_t extended_sz;
        e::unpack64be(m_recv_msg->data() + BUSYBEE_HEADER_SIZE, &extended_sz);

        if (extended_sz > BUSYBEE_MAX_MSG_SIZE)
        {
            e::atomic::or_64_nobarrier(&m_flags, CHAN_CLOSE_NEEDED);
            return false;
        }

        m_recv_msg.reset(e::buffer::create(extended_sz));
        m_recv_msg->resize(BUSYBEE_HEADER_SIZE);
        memset(m_recv_msg->data(), 0, BUSYBEE_HEADER_SIZE);
        m_recv_head = 0;
    }
    else
    {
        m_meta_mtx.lock();
        uint64_t remote = m_remote;
        m_meta_mtx.unlock();
        message* msg = new message(m_recv_msg.get(), remote);

        if ((e::atomic::load_64_nobarrier(&m_flags) & CHAN_IDENTIFIED))
        {
            queue_push(recv_end, msg);
            m_recv_msg.release();
        }
        else
        {
            delete msg;
            m_recv_msg.reset();
        }

        m_recv_partial_header_sz = 0;
        m_recv_head = 0;
    }

    return true;
}

#define str(x) #x
#define xstr(x) str(x)
#define stringify(x) case (x): lhs << xstr(x); break

std::ostream&
operator << (std::ostream& lhs, busybee_returncode rhs)
{
    switch (rhs)
    {
        stringify(BUSYBEE_SUCCESS);
        stringify(BUSYBEE_SHUTDOWN);
        stringify(BUSYBEE_DISRUPTED);
        stringify(BUSYBEE_TIMEOUT);
        stringify(BUSYBEE_EXTERNAL);
        stringify(BUSYBEE_INTERRUPTED);
        stringify(BUSYBEE_SEE_ERRNO);
        default:
            lhs << "unknown returncode";
    }

    return lhs;
}

busybee_controller :: busybee_controller()
{
}

busybee_controller :: ~busybee_controller() throw ()
{
}

busybee_server*
busybee_server :: create(busybee_controller* controller,
                         uint64_t server_id,
                         const po6::net::location& bind_to,
                         e::garbage_collector* gc)
{
    server* srv = new (std::nothrow) busybee::server(controller, server_id, bind_to, gc);

    if (!srv || srv->init() != BUSYBEE_SUCCESS)
    {
        delete srv;
        return NULL;
    }

    return srv;
}

busybee_server :: busybee_server()
{
}

busybee_server :: ~busybee_server() throw ()
{
}

server :: server(busybee_controller* controller,
                 uint64_t server_id,
                 const po6::net::location& bind_to,
                 e::garbage_collector* gc)
    : m_server_id(server_id)
    , m_bind_to(bind_to)
    , m_gc(gc)
    , m_controller(controller)
    , m_poll(NULL)
    , m_listen()
    , m_channels(NULL)
    , m_channels_sz(0)
    , m_server2channel(m_gc)
    , m_anonymous(1)
    , m_recv_mtx()
    , m_shutdown(false)
    , m_recv_queue(NULL)
    , m_recv_end(NULL)
    , m_recv_flag()
{
    DEBUG("creating server " << (void*)this);
    po6::threads::mutex::hold hold(&m_recv_mtx);
    queue_init(&m_recv_queue, &m_recv_end);
}

server :: ~server() throw ()
{
    DEBUG("destroying server " << (void*)this);
    po6::threads::mutex::hold hold(&m_recv_mtx);
    queue_cleanup(m_recv_queue, m_recv_end);
    m_gc->collect(m_poll, e::garbage_collector::free_ptr<poller>);

    for (size_t i = 0; i < m_channels_sz; ++i)
    {
        if (m_channels[i])
        {
            m_gc->collect(m_channels[i], e::garbage_collector::free_ptr<channel>);
        }
    }

    delete[] m_channels;
}

busybee_returncode
server :: init()
{
    m_poll = new_poller();

    if (!m_poll)
    {
        return BUSYBEE_SEE_ERRNO;
    }

    busybee_returncode rc = m_poll->init();

    if (rc != BUSYBEE_SUCCESS)
    {
        return rc;
    }

    m_channels_sz = sysconf(_SC_OPEN_MAX);
    const po6::net::location bind_to(m_bind_to);

    if (!m_listen.reset(bind_to.address.family(), SOCK_STREAM, IPPROTO_TCP) ||
        !m_listen.set_reuseaddr() ||
        !m_listen.bind(bind_to) ||
        !m_listen.listen(m_channels_sz) ||
        !m_listen.set_nonblocking() ||
        m_poll->add(m_listen.get(), BBPOLLIN) != BUSYBEE_SUCCESS ||
        m_poll->add(m_recv_flag.poll_fd(), BBPOLLIN) != BUSYBEE_SUCCESS)
    {
        return BUSYBEE_SEE_ERRNO;
    }

    m_channels = new (std::nothrow) channel*[m_channels_sz];

    if (!m_channels)
    {
        errno = ENOMEM;
        return BUSYBEE_SEE_ERRNO;
    }

    for (size_t i = 0; i < m_channels_sz; ++i)
    {
        e::atomic::store_ptr_release(m_channels + i, (channel*)NULL);
    }

    return BUSYBEE_SUCCESS;
}

bool
server :: deliver(uint64_t server_id, std::auto_ptr<e::buffer> msg)
{
    message* m = new message(msg.get(), server_id);
    msg.release();
    m_recv_mtx.lock();
    queue_push(&m_recv_end, m);
    m_recv_flag.set();
    m_recv_mtx.unlock();
    return true;
}

busybee_returncode
server :: send(uint64_t server_id, std::auto_ptr<e::buffer> msg)
{
    assert(msg->size() >= BUSYBEE_HEADER_SIZE);
    assert(msg->size() <= BUSYBEE_MAX_MSG_SIZE);
    busybee_returncode rc;
    channel* chan = get_channel(server_id, &rc);

    if (rc != BUSYBEE_SUCCESS)
    {
        return rc;
    }

    chan->enqueue(msg);
    return work_send(chan);
}

busybee_returncode
server :: recv(e::garbage_collector::thread_state* ts,
               int timeout, uint64_t* id,
               std::auto_ptr<e::buffer>* msg)
{
    *id = 0;

    while (true)
    {
        if (m_shutdown)
        {
            return BUSYBEE_SHUTDOWN;
        }

        if (msg)
        {
            m_recv_mtx.lock();

            if (m_shutdown)
            {
                m_recv_mtx.unlock();
                return BUSYBEE_SHUTDOWN;
            }
            else if (m_recv_queue)
            {
                message* m = m_recv_queue;
                queue_remove_head(&m_recv_queue, &m_recv_end);
                m_recv_mtx.unlock();
                *id = m->id;
                msg->reset(m->payload);
                m->payload = NULL;
                delete m;
                return BUSYBEE_SUCCESS;
            }
            else
            {
                m_recv_flag.clear();
                m_recv_mtx.unlock();
            }
        }

        busybee_returncode rc;
        int fd;
        uint32_t events;

        m_gc->offline(ts);
        rc = m_poll->poll(timeout, &fd, &events);
        DEBUG("server " << (void*)this << " rc=" << rc << " poll fd=" << fd << " events=" << events);
        m_gc->online(ts);

        if (rc != BUSYBEE_SUCCESS)
        {
            return rc;
        }

        if (fd == m_recv_flag.poll_fd())
        {
            continue;
        }

        if (fd == m_listen.get())
        {
            if ((events & BBPOLLIN))
            {
                fd = accept_connection();
            }

            if (fd < 0)
            {
                continue;
            }

            events = BBPOLLIN | BBPOLLOUT;
        }

        rc = work_dispatch(fd, events, id);

        if (rc != BUSYBEE_SUCCESS)
        {
            return rc;
        }
    }
}

busybee_returncode
server :: recv_no_msg(e::garbage_collector::thread_state* ts, int timeout, uint64_t* server_id)
{
    return recv(ts, timeout, server_id, NULL);
}

busybee_returncode
server :: get_addr(uint64_t server_id, po6::net::location* addr)
{
    channel* chan = NULL;

    if (!m_server2channel.get(server_id, &chan))
    {
        return BUSYBEE_DISRUPTED;
    }

    return chan->address(addr);
}

busybee_returncode
server :: shutdown()
{
    po6::threads::mutex::hold hold(&m_recv_mtx);
    m_shutdown = true;
    m_recv_flag.set();
    return BUSYBEE_SUCCESS;
}

channel*
server :: get_channel(uint64_t server_id, busybee_returncode* rc)
{
    channel* chan = NULL;

    if (m_server2channel.get(server_id, &chan))
    {
        *rc = BUSYBEE_SUCCESS;
        return chan;
    }

    po6::net::location remote = m_controller->lookup(server_id);

    if (remote == po6::net::location())
    {
        *rc = BUSYBEE_DISRUPTED;
        return NULL;
    }

    chan = new channel();
    *rc = chan->connect(m_server_id, server_id, remote);

    if (*rc != BUSYBEE_SUCCESS)
    {
        delete chan;
        return NULL;
    }

    *rc = m_poll->add(chan->fd(), BBPOLLIN|BBPOLLOUT|BBPOLLET);

    if (*rc != BUSYBEE_SUCCESS)
    {
        delete chan;
        return NULL;
    }

    *rc = BUSYBEE_SUCCESS;
    e::atomic::store_ptr_release(m_channels + chan->fd(), chan);
    m_server2channel.put_ine(server_id, chan);
    return chan;
}

int
server :: accept_connection()
{
    channel* chan = new channel();
    busybee_returncode rc = chan->accept(m_server_id, &m_listen);

    if (rc != BUSYBEE_SUCCESS)
    {
        delete chan;
        return - 1;
    }

    rc = m_poll->add(chan->fd(), BBPOLLIN|BBPOLLOUT|BBPOLLET);

    if (rc != BUSYBEE_SUCCESS)
    {
        delete chan;
        return -1;
    }

    e::atomic::store_ptr_release(m_channels + chan->fd(), chan);
    return chan->fd();
}

busybee_returncode
server :: work_dispatch(int fd, uint32_t events, uint64_t* server_id)
{
    assert(fd >= 0 && (unsigned)fd < m_channels_sz);
    channel* chan = e::atomic::load_ptr_acquire(m_channels + fd);
    // protect against channels passed to "collect" that haven't been freed
    if (!chan) return BUSYBEE_SUCCESS;
    busybee_returncode send_rc = BUSYBEE_SUCCESS;
    busybee_returncode recv_rc = BUSYBEE_SUCCESS;
    message* msg_queue;
    message** msg_end;
    queue_init(&msg_queue, &msg_end);

    if ((events & BBPOLLOUT))
    {
        send_rc = chan->do_work_send();
    }

    if ((events & BBPOLLIN))
    {
        recv_rc = chan->do_work_recv(&msg_end);
    }

    // do both above to clear any edges to the extent possible
    // distill two error codes into one

    if (send_rc == INTERNAL_TIME_TO_CLOSE ||
        recv_rc == INTERNAL_TIME_TO_CLOSE)
    {
        *server_id = chan->remote();
        queue_cleanup(msg_queue, msg_end);
        return work_close(chan);
    }

    if (recv_rc == INTERNAL_NEWLY_IDENTIFIED)
    {
        if (chan->remote() == 0)
        {
            uint64_t id = e::atomic::increment_64_nobarrier(&m_anonymous, 1);
            // XXX make sure this isn't in use
            chan->deanonymize_remote(id);

            for (message* m = msg_queue; m; m = m->next)
            {
                m->id = id;
            }
        }

        m_server2channel.put_ine(chan->remote(), chan);
        recv_rc = BUSYBEE_SUCCESS;
    }

    /// XXX
    m_recv_mtx.lock();
    queue_splice(&m_recv_end, &msg_queue, &msg_end);
    m_recv_flag.set();
    m_recv_mtx.unlock();

    if (send_rc == BUSYBEE_SUCCESS &&
        recv_rc == BUSYBEE_SUCCESS)
    {
        return BUSYBEE_SUCCESS;
    }
    else if (send_rc == BUSYBEE_SUCCESS)
    {
        *server_id = chan->remote();
        return recv_rc;
    }
    else if (recv_rc == BUSYBEE_SUCCESS)
    {
        *server_id = chan->remote();
        return recv_rc;
    }
    else
    {
        *server_id = chan->remote();
        return send_rc < recv_rc ? send_rc : recv_rc;
    }
}

busybee_returncode
server :: work_send(channel* chan)
{
    DEBUG("server sending channel " << (void*)chan);
    busybee_returncode rc = BUSYBEE_SUCCESS;
    rc = chan->do_work_send();

    if (rc == INTERNAL_TIME_TO_CLOSE)
    {
        DEBUG("send " << (void*)chan << " encountered error");
        return work_close(chan);
    }

    return rc;
}

busybee_returncode
server :: work_close(channel* chan)
{
    DEBUG("server closing channel " << (void*)chan);
    e::atomic::compare_and_swap_ptr_acquire(m_channels + chan->fd(), chan, (channel*)NULL);
    m_server2channel.del_if(chan->remote(), chan);
    m_gc->collect(chan, e::garbage_collector::free_ptr<channel>);
    return BUSYBEE_DISRUPTED;
}

busybee_client*
busybee_client :: create(busybee_controller* controller)
{
    client* cl = new (std::nothrow) busybee::client(controller);

    if (!cl || cl->init() != BUSYBEE_SUCCESS)
    {
        delete cl;
        return NULL;
    }

    return cl;
}

busybee_client :: busybee_client()
{
}

busybee_client :: ~busybee_client() throw ()
{
}

client :: client(busybee_controller* controller)
    : m_controller(controller)
    , m_poll(NULL)
    , m_channels(NULL)
    , m_channels_sz(0)
    , m_server2channel()
    , m_recv_queue(NULL)
    , m_recv_end(NULL)
{
    DEBUG("creating client " << (void*)this);
    m_server2channel.set_empty_key(BUSYBEE_CHOSEN_ID_MINIMUM - 1);
    m_server2channel.set_deleted_key(BUSYBEE_CHOSEN_ID_MINIMUM - 2);
    queue_init(&m_recv_queue, &m_recv_end);
}

client :: ~client() throw ()
{
    DEBUG("destroying client " << (void*)this);
    reset();
    delete m_poll;
    delete[] m_channels;
}

busybee_returncode
client :: init()
{
    m_poll = new_poller();

    if (!m_poll)
    {
        return BUSYBEE_SEE_ERRNO;
    }

    busybee_returncode rc = m_poll->init();

    if (rc != BUSYBEE_SUCCESS)
    {
        return rc;
    }

    m_channels_sz = sysconf(_SC_OPEN_MAX);
    m_channels = new (std::nothrow) channel*[m_channels_sz];

    if (!m_channels)
    {
        errno = ENOMEM;
        return BUSYBEE_SEE_ERRNO;
    }

    for (size_t i = 0; i < m_channels_sz; ++i)
    {
        e::atomic::store_ptr_release(m_channels + i, (channel*)NULL);
    }

    return BUSYBEE_SUCCESS;
}

bool
client :: deliver(uint64_t server_id, std::auto_ptr<e::buffer> msg)
{
    message* m = new message(msg.get(), server_id);
    msg.release();
    queue_push(&m_recv_end, m);
    return true;
}

busybee_returncode
client :: send(uint64_t server_id, std::auto_ptr<e::buffer> msg)
{
    assert(msg->size() >= BUSYBEE_HEADER_SIZE);
    assert(msg->size() <= BUSYBEE_MAX_MSG_SIZE);
    busybee_returncode rc;
    channel* chan = get_channel(server_id, &rc);

    if (rc != BUSYBEE_SUCCESS)
    {
        return rc;
    }

    chan->enqueue(msg);
    return work_send(chan);
}

busybee_returncode
client :: recv(int timeout, uint64_t* id,
               std::auto_ptr<e::buffer>* msg)
{
    *id = 0;

    while (true)
    {
        if (msg && m_recv_queue)
        {
            message* m = m_recv_queue;
            queue_remove_head(&m_recv_queue, &m_recv_end);
            *id = m->id;
            msg->reset(m->payload);
            m->payload = NULL;
            delete m;
            return BUSYBEE_SUCCESS;
        }

        busybee_returncode rc;
        int fd;
        uint32_t events;

        rc = m_poll->poll(timeout, &fd, &events);
        DEBUG("client " << (void*)this << " rc=" << rc << " poll fd=" << fd << " events=" << events);

        if (rc != BUSYBEE_SUCCESS)
        {
            return rc;
        }

        rc = work_dispatch(fd, events, id);

        if (rc != BUSYBEE_SUCCESS)
        {
            return rc;
        }
    }
}

busybee_returncode
client :: recv_no_msg(int timeout, uint64_t* server_id)
{
    return recv(timeout, server_id, NULL);
}

busybee_returncode
client :: set_external_fd(int fd)
{
    assert(fd >= 0 && (unsigned)fd < m_channels_sz);
    channel* chan = m_channels[fd];

    if (chan && chan != CHANNEL_EXTERNAL)
    {
        delete chan;
    }

    m_channels[fd] = CHANNEL_EXTERNAL;
    return BUSYBEE_SUCCESS;
}

#define RC2ERR(X) \
    do { \
        rc = (X); \
        if (err == BUSYBEE_SUCCESS) err = rc; \
    } while (0)

busybee_returncode
client :: reset()
{
    DEBUG("resetting client " << (void*)this);
    busybee_returncode err = BUSYBEE_SUCCESS;
    busybee_returncode rc = BUSYBEE_SUCCESS;

    for (size_t i = 0; i < m_channels_sz; ++i)
    {
        if (m_channels[i] == CHANNEL_EXTERNAL)
        {
            RC2ERR(m_poll->del(i));
        }
        else if (m_channels[i])
        {
            delete m_channels[i];
        }
    }

    m_server2channel.clear();
    queue_cleanup(m_recv_queue, m_recv_end);
    queue_init(&m_recv_queue, &m_recv_end);
    return err;
}

channel*
client :: get_channel(uint64_t server_id, busybee_returncode* rc)
{
    google::dense_hash_map<uint64_t, channel*>::iterator it;
    it = m_server2channel.find(server_id);

    if (it != m_server2channel.end())
    {
        *rc = BUSYBEE_SUCCESS;
        return it->second;
    }

    po6::net::location remote = m_controller->lookup(server_id);

    if (remote == po6::net::location())
    {
        *rc = BUSYBEE_DISRUPTED;
        return NULL;
    }

    std::auto_ptr<channel> chan(new channel());
    *rc = chan->connect(0, server_id, remote);

    if (*rc != BUSYBEE_SUCCESS)
    {
        return NULL;
    }

    *rc = m_poll->add(chan->fd(), BBPOLLIN|BBPOLLOUT|BBPOLLET);

    if (*rc != BUSYBEE_SUCCESS)
    {
        return NULL;
    }

    *rc = BUSYBEE_SUCCESS;
    assert(m_channels[chan->fd()] == NULL);
    m_server2channel[server_id] = chan.get();
    m_channels[chan->fd()] = chan.get();
    return chan.release();
}

busybee_returncode
client :: work_dispatch(int fd, uint32_t events, uint64_t* server_id)
{
    assert(fd >= 0 && (unsigned)fd < m_channels_sz);
    channel* chan = m_channels[fd];
    assert(chan);

    if (chan == CHANNEL_EXTERNAL)
    {
        return BUSYBEE_EXTERNAL;
    }

    busybee_returncode send_rc = BUSYBEE_SUCCESS;
    busybee_returncode recv_rc = BUSYBEE_SUCCESS;

    if ((events & BBPOLLOUT))
    {
        send_rc = chan->do_work_send();
    }

    if ((events & BBPOLLIN))
    {
        recv_rc = chan->do_work_recv(&m_recv_end);
    }

    // do both above to clear any edges to the extent possible
    // distill two error codes into one

    if (send_rc == INTERNAL_TIME_TO_CLOSE ||
        recv_rc == INTERNAL_TIME_TO_CLOSE)
    {
        *server_id = chan->remote();
        return work_close(chan);
    }

    if (recv_rc == INTERNAL_NEWLY_IDENTIFIED)
    {
        if (chan->remote() == 0)
        {
            *server_id = 0;
            return work_close(chan);
        }

        m_server2channel.insert(std::make_pair(chan->remote(), chan));
        recv_rc = BUSYBEE_SUCCESS;
    }

    if (send_rc == BUSYBEE_SUCCESS &&
        recv_rc == BUSYBEE_SUCCESS)
    {
        return BUSYBEE_SUCCESS;
    }
    else if (send_rc == BUSYBEE_SUCCESS)
    {
        *server_id = chan->remote();
        return recv_rc;
    }
    else if (recv_rc == BUSYBEE_SUCCESS)
    {
        *server_id = chan->remote();
        return send_rc;
    }
    else
    {
        *server_id = chan->remote();
        return send_rc < recv_rc ? send_rc : recv_rc;
    }
}

busybee_returncode
client :: work_send(channel* chan)
{
    DEBUG("client sending channel " << (void*)chan);
    busybee_returncode rc = BUSYBEE_SUCCESS;
    rc = chan->do_work_send();

    if (rc == INTERNAL_TIME_TO_CLOSE)
    {
        return work_close(chan);
    }

    return rc;
}

busybee_returncode
client :: work_close(channel* chan)
{
    DEBUG("client closing channel " << (void*)chan);
    assert(m_channels[chan->fd()] == chan);
    m_channels[chan->fd()] = NULL;
    google::dense_hash_map<uint64_t, channel*>::iterator it;
    it = m_server2channel.find(chan->remote());

    if (it->second == chan)
    {
        m_server2channel.erase(it);
    }

    delete chan;
    return BUSYBEE_DISRUPTED;
}

busybee_single*
busybee_single :: create(const po6::net::hostname& host)
{
    return new single(host);
}

busybee_single*
busybee_single :: create(const po6::net::location& host)
{
    return new single(host);
}

busybee_single :: busybee_single()
{
}

busybee_single :: ~busybee_single() throw ()
{
}

single :: single(const po6::net::hostname& host)
    : m_host(host)
    , m_addr()
    , m_method(HOSTNAME)
    , m_chan()
    , m_recv_queue()
    , m_recv_end()
{
    DEBUG("creating single connection " << (void*)this);
    queue_init(&m_recv_queue, &m_recv_end);
}

single :: single(const po6::net::location& host)
    : m_host()
    , m_addr(host)
    , m_method(LOCATION)
    , m_chan()
    , m_recv_queue()
    , m_recv_end()
{
    DEBUG("creating single connection " << (void*)this);
    queue_init(&m_recv_queue, &m_recv_end);
}

single :: ~single() throw ()
{
    DEBUG("destroying single connection " << (void*)this);
    queue_cleanup(m_recv_queue, m_recv_end);
}

int
single :: poll_fd()
{
    return m_chan.get() ? m_chan->fd() : -1;
}

busybee_returncode
single :: send(std::auto_ptr<e::buffer> msg)
{
    if (!m_chan.get())
    {
        busybee_returncode rc = setup();

        if (rc != BUSYBEE_SUCCESS)
        {
            return rc;
        }
    }

    m_chan->enqueue(msg);
    busybee_returncode rc = m_chan->do_work_send();

    if (rc == INTERNAL_TIME_TO_CLOSE)
    {
        m_chan.reset();
        return BUSYBEE_DISRUPTED;
    }

    return rc;
}

busybee_returncode
single :: recv(int timeout, std::auto_ptr<e::buffer>* msg)
{
    while (true)
    {
        if (!m_chan.get())
        {
            busybee_returncode rc = setup();

            if (rc != BUSYBEE_SUCCESS)
            {
                return rc;
            }
        }

        if (msg && m_recv_queue)
        {
            message* m = m_recv_queue;
            queue_remove_head(&m_recv_queue, &m_recv_end);
            msg->reset(m->payload);
            m->payload = NULL;
            delete m;
            return BUSYBEE_SUCCESS;
        }

        struct pollfd pfd;
        pfd.fd = m_chan->fd();
        pfd.events = POLLIN;
        int ret = poll(&pfd, 1, timeout);

        if (ret == 0)
        {
            return BUSYBEE_TIMEOUT;
        }
        else if (ret < 0)
        {
            return BUSYBEE_SEE_ERRNO;
        }

        busybee_returncode rc = m_chan->do_work_recv(&m_recv_end);

        if (rc == INTERNAL_TIME_TO_CLOSE)
        {
            m_chan.reset();
            return BUSYBEE_DISRUPTED;
        }
    }
}

busybee_returncode
single :: setup()
{
    if (m_chan.get())
    {
        return BUSYBEE_SUCCESS;
    }

    m_chan.reset(new channel());
    busybee_returncode rc;

    switch (m_method)
    {
        case HOSTNAME:
            m_addr = m_host.lookup(AF_UNSPEC, IPPROTO_TCP);
        case LOCATION:
            rc = m_chan->connect(0, 0, m_addr);
            break;
        default:
            abort();
    }

    return rc;
}

bool
busybee_discover(po6::net::ipaddr* ip)
{
    struct ifaddrs* ifa = NULL;

    if (getifaddrs(&ifa) < 0 || !ifa)
    {
        return false;
    }

    e::guard g = e::makeguard(freeifaddrs, ifa);
    g.use_variable();

    for (struct ifaddrs* ifap = ifa; ifap; ifap = ifap->ifa_next)
    {
        if (strncmp(ifap->ifa_name, "lo", 2) == 0)
        {
            continue;
        }

        if (ifap->ifa_addr->sa_family == AF_INET)
        {
            po6::net::location loc;

            if (loc.set(ifap->ifa_addr, sizeof(sockaddr_in)))
            {
                *ip = loc.address;
                return true;
            }
        }
        else if (ifap->ifa_addr->sa_family == AF_INET6)
        {
            po6::net::location loc;

            if (loc.set(ifap->ifa_addr, sizeof(sockaddr_in6)))
            {
                *ip = loc.address;
                return true;
            }
        }
    }

    errno = 0;
    return false;
}
