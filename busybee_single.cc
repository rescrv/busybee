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

#ifdef BUSYBEE_DEBUG
#define DEBUG std::cerr << __FILE__ << ":" << __LINE__ << " "
#else
#define DEBUG if (0) std::cerr << __FILE__ << ":" << __LINE__ << " "
#endif

// POSIX
#ifndef _MSC_VER
#include <poll.h>
#endif

// e
#include <e/endian.h>
#include <e/time.h>

// BusyBee
#include "busybee_single.h"

busybee_single :: busybee_single(const po6::net::hostname& host)
    : m_timeout(-1)
    , m_type(USE_HOSTNAME)
    , m_host(host)
    , m_loc()
    , m_remote()
    , m_connection()
    , m_recv_partial_header_sz()
    , m_recv_partial_msg()
    , m_flags(0)
    , m_token(0)
{
}

busybee_single :: busybee_single(const po6::net::location& loc)
    : m_timeout(-1)
    , m_type(USE_LOCATION)
    , m_host()
    , m_loc(loc)
    , m_remote()
    , m_connection()
    , m_recv_partial_header_sz()
    , m_recv_partial_msg()
    , m_flags(0)
    , m_token(0)
{
}

busybee_single :: ~busybee_single() throw ()
{
}

void
busybee_single :: set_timeout(int timeout)
{
    m_timeout = timeout;
}

busybee_returncode
#ifdef _MSC_VER
busybee_single :: send(std::shared_ptr<e::buffer> msg)
#else
busybee_single :: send(std::auto_ptr<e::buffer> msg)
#endif
{
    // Pack the size into the header
    *msg << static_cast<uint32_t>(msg->size());

    if (m_connection.get() < 0)
    {
        if (m_type == USE_HOSTNAME)
        {
            m_remote = m_host.connect(AF_UNSPEC, SOCK_STREAM, IPPROTO_TCP, &m_connection);
        }
        else if (m_type == USE_LOCATION)
        {
            m_connection.reset(m_loc.address.family(), SOCK_STREAM, IPPROTO_TCP);
            m_connection.connect(m_loc);
            m_remote = m_connection.getpeername();
        }
        else
        {
            abort();
        }

#ifdef HAVE_SO_NOSIGPIPE
        int sigpipeopt = 1;
        soc.set_sockopt(SOL_SOCKET, SO_NOSIGPIPE, &sigpipeopt, sizeof(sigpipeopt));
#endif // HAVE_SO_NOSIGPIPE
        uint32_t sz = sizeof(uint32_t) + sizeof(uint64_t);
        char buf[sizeof(uint32_t) + sizeof(uint64_t)];
        sz |= 0x80000000ULL;
        char* ptr = buf;
        ptr = e::pack32be(sz, ptr);
        ptr = e::pack64be(0, ptr);

        if (m_connection.xwrite(buf, ptr - buf) != ptr - buf)
        {
            reset();
            return BUSYBEE_DISRUPTED;
        }
    }

    if (m_connection.xwrite(msg->data(), msg->size()) != msg->size())
    {
        reset();
        return BUSYBEE_DISRUPTED;
    }

    return BUSYBEE_SUCCESS;
}

busybee_returncode
#ifdef _MSC_VER
busybee_single :: recv(std::shared_ptr<e::buffer>* msg)
#else
busybee_single :: recv(std::auto_ptr<e::buffer>* msg)
#endif
{
    while (true)
    {
        if (m_connection.get() < 0)
        {
            reset();
            return BUSYBEE_DISRUPTED;
        }

        size_t to_read = 0;
        uint8_t* ptr = NULL;

        if (m_recv_partial_msg.get())
        {
            to_read = m_recv_partial_msg->capacity() - m_recv_partial_msg->size();
            ptr = m_recv_partial_msg->data() + m_recv_partial_msg->size();
        }
        else
        {
            to_read = sizeof(uint32_t) - m_recv_partial_header_sz;
            ptr = m_recv_partial_header + m_recv_partial_header_sz;
        }

        pollfd pfd;
        pfd.fd = m_connection.get();
        pfd.events = POLLIN;
        pfd.revents = 0;
#ifdef _MSC_VER
        int status = WSAPoll(&pfd, 1, m_timeout);
#else
        int status = poll(&pfd, 1, m_timeout);
#endif

        if (status < 0 && EINTR)
        {
            continue;
        }
        else if (status < 0)
        {
            return BUSYBEE_POLLFAILED;
        }
        else if (status == 0)
        {
            return BUSYBEE_TIMEOUT;
        }

        assert(status == 1);
        int flags = 0;
#ifdef HAVE_MSG_NOSIGNAL
        flags |= MSG_NOSIGNAL;
#endif // HAVE_MSG_NOSIGNAL

        ssize_t amt = m_connection.xrecv(ptr, to_read, flags);

        if (amt < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
        {
            reset();
            return BUSYBEE_DISRUPTED;
        }
        else if (amt < 0)
        {
            continue;
        }
        else if (amt == 0)
        {
            reset();
            return BUSYBEE_DISRUPTED;
        }

        if (m_recv_partial_msg.get())
        {
            m_recv_partial_msg->resize(m_recv_partial_msg->size() + amt);
        }
        else
        {
            m_recv_partial_header_sz += amt;

            if (m_recv_partial_header_sz == sizeof(uint32_t))
            {
                m_recv_partial_header_sz = 0;
                uint32_t sz;
                e::unpack32be(m_recv_partial_header, &sz);

                m_flags = sz & 0xe0000000UL;
                sz &= 0x1fffffffUL;

                if (sz < sizeof(uint32_t))
                {
                    reset();
                    return BUSYBEE_DISRUPTED;
                }

                m_recv_partial_msg.reset(e::buffer::create(sz));
                memmove(m_recv_partial_msg->data(), m_recv_partial_header, sizeof(uint32_t));
                m_recv_partial_msg->resize(sizeof(uint32_t));
            }
        }

        // Handle newly-created, 0-length messages (otherwise we could nest
        // above)
        if (m_recv_partial_msg.get() && m_recv_partial_msg->size() == m_recv_partial_msg->capacity())
        {
            if ((m_flags & 0x80000000ULL))
            {
                if (m_recv_partial_msg->size() != sizeof(uint32_t) + sizeof(uint64_t))
                {
                    reset();
                    return BUSYBEE_DISRUPTED;
                }

                e::unpack64be(m_recv_partial_msg->data() + sizeof(uint32_t), &m_token);
            }

            if (m_flags)
            {
                m_recv_partial_msg.reset();
            }
            else
            {
                *msg = m_recv_partial_msg;
                return BUSYBEE_SUCCESS;
            }
        }
    }
}

void
busybee_single :: reset()
{
    try
    {
        m_connection.shutdown(SHUT_RDWR);
    }
    catch (po6::error& e)
    {
    }

    try
    {
        m_connection.close();
    }
    catch (po6::error& e)
    {
    }

    m_remote = po6::net::location();
    m_recv_partial_header_sz = 0;
    m_recv_partial_msg.reset();
    m_flags = 0;
    m_token = 0;
}
