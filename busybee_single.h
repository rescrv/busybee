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

#ifndef busybee_single_h_
#define busybee_single_h_

// STL
#include <memory>

// po6
#include <po6/net/hostname.h>

// e
#include <e/buffer.h>

// BusyBee
#include <busybee_returncode.h>

class busybee_single
{
    public:
        busybee_single(const po6::net::hostname& host);
        busybee_single(const po6::net::location& host);
        ~busybee_single() throw ();

    public:
        void set_timeout(int timeout);

    public:
        // This is valid so long as send has been called, and the most recent
        // call to send or recv returned SUCCESS
        const po6::net::location& remote() { return m_remote; }
        // This is valid so long as recv has been called, and the most recent call
        // to send or recv returned SUCCESS
        uint64_t token() { return m_token; }

    public:
#ifdef _MSC_VER
        busybee_returncode send(std::shared_ptr<e::buffer> msg);
        busybee_returncode recv(std::shared_ptr<e::buffer>* msg);
#else
        busybee_returncode send(std::auto_ptr<e::buffer> msg);
        busybee_returncode recv(std::auto_ptr<e::buffer>* msg);
#endif

    private:
        void reset();

    private:
        int m_timeout;
        enum { USE_HOSTNAME, USE_LOCATION } m_type;
        po6::net::hostname m_host;
        po6::net::location m_loc;
        po6::net::location m_remote;
        po6::net::socket m_connection;
        uint32_t m_recv_partial_header_sz;
        uint8_t m_recv_partial_header[sizeof(uint32_t)];
#ifdef _MSC_VER
        std::shared_ptr<e::buffer> m_recv_partial_msg;
#else
        std::auto_ptr<e::buffer> m_recv_partial_msg;
#endif
        uint32_t m_flags;
        uint64_t m_token;

    private:
        busybee_single(const busybee_single&);
        busybee_single& operator = (const busybee_single&);
};

#endif // busybee_single_h_
