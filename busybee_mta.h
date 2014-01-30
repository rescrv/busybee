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

#ifndef busybee_mta_h_
#define busybee_mta_h_

// STL
#include <memory>

// po6
#include <po6/net/location.h>
#include <po6/net/socket.h>
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>

// e
#include <e/array_ptr.h>
#include <e/buffer.h>
#include <e/lockfree_hash_map.h>

// BusyBee
#include <busybee_mapper.h>
#include <busybee_returncode.h>

#define BUSYBEE_HIDDEN __attribute__((visibility("hidden")))

class busybee_mta
{
    public:
        busybee_mta(busybee_mapper* mapper,
                    const po6::net::location& bind_to,
                    uint64_t server_id,
                    size_t num_threads);
        ~busybee_mta() throw ();

    public:
        void shutdown();
        // Enter a barrier.  All "num_threads" threads must enter the barrier
        // before the caller will leave.  All other threads will remain in the
        // barrier until "unpause" is called.  No locks will be held while
        // threads are paused, so deadlock cannot occur.
        void pause();
        // Cause all threads to leave the barrier initiated by "pause".
        void unpause();
        // Wakeup threads.  Use when "deliver" is called by a thread that will
        // never call "recv".
        void wake_one();

    public:
        void set_id(uint64_t server_id);
        void set_timeout(int timeout); // call while paused
        void set_ignore_signals();
        void unset_ignore_signals();
        void add_signals();

    public:
        busybee_returncode get_addr(uint64_t server_id, po6::net::location* addr);
        bool deliver(uint64_t server_id, std::auto_ptr<e::buffer> msg);
        int poll_fd();
        busybee_returncode drop(uint64_t server_id);
        busybee_returncode send(uint64_t server_id,
                                std::auto_ptr<e::buffer> msg);
        busybee_returncode recv(uint64_t* server_id,
                                std::auto_ptr<e::buffer>* msg);

    private:
        class channel;
        struct recv_message;
        struct send_message;

    private:
        int BUSYBEE_HIDDEN add_event(int fd, uint32_t events);
        int BUSYBEE_HIDDEN wait_event(int* fd, uint32_t* events);
        // Alert the threads to wake them up with eventfd
        void BUSYBEE_HIDDEN up_the_semaphore();
        busybee_returncode BUSYBEE_HIDDEN get_channel(uint64_t server_id, channel** chan, uint64_t* chan_tag);
        busybee_returncode BUSYBEE_HIDDEN setup_channel(po6::net::socket* soc, channel* chan);
        busybee_returncode BUSYBEE_HIDDEN possibly_work_send_or_recv(channel* chan);
        bool BUSYBEE_HIDDEN work_dispatch(channel* chan, uint32_t events, busybee_returncode* rc);
        void BUSYBEE_HIDDEN work_accept();
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
        po6::net::socket m_listen;
        size_t m_channels_sz;
        e::array_ptr<channel> m_channels;
        e::lockfree_hash_map<uint64_t, uint64_t, e::hash_map_id> m_server2channel;
        busybee_mapper* m_mapper;
        uint64_t m_server_id;
        po6::threads::mutex m_anon_lock;
        uint32_t m_anon_id;
        int m_timeout;
        po6::threads::mutex m_recv_lock;
        recv_message* m_recv_queue;
        recv_message** m_recv_end;
        sigset_t m_sigmask;
        char* m_pipebuf;
        po6::io::fd m_eventfdread;
        po6::io::fd m_eventfdwrite;
        po6::threads::mutex m_pause_lock;
        po6::threads::cond m_pause_all_paused;
        po6::threads::cond m_pause_may_unpause;
        bool m_shutdown;
        size_t m_pause_count;
        bool m_pause_paused;
        size_t m_pause_num;

    private:
        busybee_mta(const busybee_mta&);
        busybee_mta& operator = (const busybee_mta&);
};

#undef BUSYBEE_HIDDEN

#endif // busybee_mta_h_
