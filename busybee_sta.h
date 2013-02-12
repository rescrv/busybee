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

#ifndef busybee_sta_h_
#define busybee_sta_h_

// STL
#include <memory>

// po6
#include <po6/net/location.h>
#include <po6/net/socket.h>

// e
#include <e/array_ptr.h>
#include <e/buffer.h>
#include <e/lockfree_hash_map.h>

// BusyBee
#include <busybee_mapper.h>
#include <busybee_returncode.h>

class busybee_sta
{
    public:
        busybee_sta(busybee_mapper* mapper,
                    const po6::net::location& bind_to,
                    uint64_t server_id);
        ~busybee_sta() throw ();

    public:
        void set_id(uint64_t server_id);
        void set_timeout(int timeout);
        void set_ignore_signals();
        void unset_ignore_signals();
        void add_signals();

    public:
        int poll_fd();
        busybee_returncode drop(uint64_t server_id);
        busybee_returncode send(uint64_t server_id,
                                std::auto_ptr<e::buffer> msg);
        busybee_returncode recv(uint64_t* server_id,
                                std::auto_ptr<e::buffer>* msg);

    private:
        class channel;
        class recv_message;
        class send_message;

    private:
        busybee_returncode get_channel(uint64_t server_id, channel** chan, uint64_t* chan_tag);
        bool setup_channel(po6::net::socket* soc, channel* chan, uint64_t new_tag);
        void set_mapping(uint64_t server_id, uint64_t chan_tag);
        void work_accept();
        int add_event(int fd, uint32_t events);
        int wait_event(int* fd, uint32_t* events);
        void work_close(channel* chan);
        void work_recv(channel* chan, bool* need_close, bool* quiet);
        void work_send(channel* chan, bool* need_close, bool* quiet);
        bool send_fin(channel* chan);
        bool send_ack(channel* chan);

    private:
        po6::io::fd m_epoll;
        po6::net::socket m_listen;
        size_t m_channels_sz;
        e::array_ptr<channel> m_channels;
        e::lockfree_hash_map<uint64_t, uint64_t, e::hash_map_id> m_server2channel;
        busybee_mapper* m_mapper;
        uint64_t m_server_id;
        int m_timeout;
        recv_message* m_recv_queue;
        recv_message** m_recv_end;
        sigset_t m_sigmask;

    private:
        busybee_sta(const busybee_sta&);
        busybee_sta& operator = (const busybee_sta&);
};

#endif // busybee_sta_h_
