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

// po6
#include <po6/io/fd.h>

// STL
#include <tr1/memory>

// po6
#include <po6/net/ipaddr.h>
#include <po6/net/location.h>
#include <po6/net/socket.h>
#include <po6/threads/mutex.h>

// e
#include <e/buffer.h>
#include <e/lockfree_hash_map.h>
#include <e/nonblocking_bounded_fifo.h>
#include <e/striped_lock.h>
#include <e/worker_barrier.h>

// BusyBee
#include <busybee_returncode.h>

class busybee_mta
{
    public:
        busybee_mta(const po6::net::ipaddr& ip,
                    in_port_t incoming,
                    in_port_t outgoing,
                    size_t num_threads);
        ~busybee_mta() throw ();

    public:
        void shutdown();

    public:
        // Enter a barrier.  All "num_threads" threads must enter the barrier
        // before the caller will leave.  All other threads will remain in the
        // barrier until "unpause" is called.  No locks will be held while
        // threads are paused, so deadlock cannot occur.
        void pause();
        // Cause all threads to leave the barrier initiated by "pause".
        void unpause();

    public:
        void set_timeout(int timeout);

    public:
        int add_external_fd(int fd, uint32_t events);
        void get_last_external(int* fd, uint32_t* events);

    public:
        busybee_returncode drop(const po6::net::location& to);
        busybee_returncode send(const po6::net::location& to,
                                std::auto_ptr<e::buffer> msg);
        busybee_returncode recv(po6::net::location* from,
                                std::auto_ptr<e::buffer>* msg);
        // Deliver a message (put it on the queue) as if it came from "from".
        // This will *not* wake up threads.  This is intentional so the thread
        // calling deliver will possibly pull the delivered item from the queue.
        bool deliver(const po6::net::location& from, std::auto_ptr<e::buffer> msg);

    public:
        po6::net::location inbound();
        po6::net::location outbound();

    private:
        class channel;
        class message;
        class pending;

    private:
        busybee_returncode get_channel(const po6::net::location& to,
                                       channel** chan,
                                       uint32_t* chantag);
        busybee_returncode get_channel(po6::net::socket* to,
                                       channel** chan,
                                       uint32_t* chantag);
        int add_descriptor(int fd);
        void postpone_event(channel* chan);
        int receive_event(int*fd, uint32_t* events);
        // Accept a new socket, and return the file descriptor number.
        int work_accept();
        // Remove the channel and set the resources to be freed.
        // This must be called with chan->mtx held.
        void work_close(channel* chan);
        // Read from the socket, and set it up for further reading if necessary.
        //
        // It will return true if "from", "msg", "res" should be returned to the
        // user, and false otherwise.
        //
        // This must be called with chan->mtx held It is the caller's
        // responsibility to check chan->events for postponed events after
        // releasing the mutex protecting this call.  This may call work_close.
        bool work_read(channel* chan,
                       po6::net::location* from,
                       std::auto_ptr<e::buffer>* msg,
                       busybee_returncode* res);
        // Write to the socket, and set it up for further writing if necessary.
        // If no other messages are buffered, write msg, otherwise buffer msg,
        // and write the buffered messages.
        //
        // It will return true if progress was made, and false if there is an
        // error to report (using *res).
        //
        // This must be called with chan->mtx held.  It is the caller's
        // responsibility to check chan for postponed events after releasing the
        // mutex protecting this call.  This may call work_close.
        bool work_write(channel* chan,
                        busybee_returncode* res);

    private:
        busybee_mta(const busybee_mta& other);

    private:
        busybee_mta& operator = (const busybee_mta& rhs);

    private:
        po6::io::fd m_epoll;
        po6::net::socket m_listen;
        po6::net::location m_bindto;
        e::striped_lock<po6::threads::mutex> m_connectlocks;
        e::lockfree_hash_map<po6::net::location, std::pair<int, uint32_t>, po6::net::location::hash> m_locations;
        e::nonblocking_bounded_fifo<message> m_incoming;
        std::vector<std::tr1::shared_ptr<channel> > m_channels;
        e::nonblocking_bounded_fifo<pending> m_postponed;
        e::worker_barrier m_pause_barrier;
        int m_timeout;
        int m_external_fd;
        int m_external_events;
        bool m_shutdown;
};

#endif // busybee_mta_h_
