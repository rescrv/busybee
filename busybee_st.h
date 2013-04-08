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

#ifndef busybee_st_h_
#define busybee_st_h_

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

#define BUSYBEE_HIDDEN __attribute__((visibility("hidden")))

class busybee_st
{
    public:
        busybee_st(busybee_mapper* mapper,
                   uint64_t server_id);
        ~busybee_st() throw ();

    public:
        void set_id(uint64_t server_id);
        void set_timeout(int timeout);
        void set_ignore_signals();
        void unset_ignore_signals();
        void add_signals();

    public:
        busybee_returncode set_external_fd(int fd);

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
        int BUSYBEE_HIDDEN add_event(int fd, uint32_t events);
        int BUSYBEE_HIDDEN wait_event(int* fd, uint32_t* events);
        busybee_returncode BUSYBEE_HIDDEN get_channel(uint64_t server_id, channel** chan, uint64_t* chan_tag);
        busybee_returncode BUSYBEE_HIDDEN setup_channel(po6::net::socket* soc, channel* chan);
        busybee_returncode BUSYBEE_HIDDEN possibly_work_recv(channel* chan);
        bool BUSYBEE_HIDDEN work_close(channel* chan, busybee_returncode* rc);
        bool BUSYBEE_HIDDEN work_send(channel* chan, busybee_returncode* rc);
        bool BUSYBEE_HIDDEN work_recv(channel* chan, busybee_returncode* rc);
        bool BUSYBEE_HIDDEN state_transition(channel* chan, busybee_returncode* rc);
        void BUSYBEE_HIDDEN handle_identify(channel* chan, bool* need_close, bool* clean_close);
        void BUSYBEE_HIDDEN handle_fin(channel* chan, bool* need_close, bool* clean_close);
        void BUSYBEE_HIDDEN handle_ack(channel* chan, bool* need_close, bool* clean_close);
        bool BUSYBEE_HIDDEN send_finack(channel* chan);

    private:
        po6::io::fd m_epoll;
        size_t m_channels_sz;
        e::array_ptr<channel> m_channels;
        static uint64_t hash(const uint64_t& r)
        {
            return r;
        }
        e::lockfree_hash_map<uint64_t, uint64_t, hash> m_server2channel;
        busybee_mapper* m_mapper;
        uint64_t m_server_id;
        uint32_t m_anon_id;
        int m_timeout;
        int m_external;
        recv_message* m_recv_queue;
        recv_message** m_recv_end;
        sigset_t m_sigmask;

    private:
        busybee_st(const busybee_st&);
        busybee_st& operator = (const busybee_st&);
};

#undef BUSYBEE_HIDDEN

#endif // busybee_st_h_
