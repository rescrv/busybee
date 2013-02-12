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
        bool deliver(uint64_t server_id, std::auto_ptr<e::buffer> msg);
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
        int add_event(int fd, uint32_t events);
        int wait_event(int* fd, uint32_t* events);
        // Alert the threads to wake them up with eventfd
        void up_the_semaphore();

    private:
        busybee_returncode get_channel(uint64_t server_id, channel** chan, uint64_t* chan_tag);
        bool setup_channel(po6::net::socket* soc, channel* chan, uint64_t new_tag);
        void set_mapping(uint64_t server_id, uint64_t chan_tag);
        void work_accept();
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

#endif // busybee_mta_h_
