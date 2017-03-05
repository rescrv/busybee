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

#ifndef busybee_h_
#define busybee_h_

// C++
#include <iostream>

// STL
#include <memory>

// e
#include <e/buffer.h>
#include <e/garbage_collector.h>

#define BUSYBEE_HEADER_SIZE 4
#define BUSYBEE_MAX_MSG_SIZE (1ULL << 48)

// If IDs are not guaranteed to be above the minimum, there will likely be
// conflicts internal to the system.  This is an application defect.
#define BUSYBEE_CHOSEN_ID_MINIMUM_LOG2 60
#define BUSYBEE_CHOSEN_ID_MINIMUM (1ULL << BUSYBEE_CHOSEN_ID_MINIMUM_LOG2)
#define BUSYBEE_IS_ANONYMOUS(X) ((X) < BUSYBEE_CHOSEN_ID_MINIMUM)

// busybee_returncode occupies [4608, 4864)
enum busybee_returncode
{
    BUSYBEE_SUCCESS     = 4608,
    BUSYBEE_SHUTDOWN    = 4609,
    BUSYBEE_DISRUPTED   = 4611,
    BUSYBEE_TIMEOUT     = 4613,
    BUSYBEE_EXTERNAL    = 4614,
    BUSYBEE_INTERRUPTED = 4615,
    BUSYBEE_SEE_ERRNO   = 4616
};

std::ostream& operator << (std::ostream& lhs, busybee_returncode rhs);

class busybee_controller
{
    public:
        busybee_controller();
        virtual ~busybee_controller() throw ();

    public:
        virtual po6::net::location lookup(uint64_t server_id) = 0;
};

class busybee_server
{
    public:
        static busybee_server* create(busybee_controller* controller,
                                      uint64_t server_id,
                                      const po6::net::location& bind_to,
                                      e::garbage_collector* gc);

    public:
        busybee_server();
        virtual ~busybee_server() throw ();

    public:
        // Enqueue a message in the "recv" queue.  The server_id and msg passed
        // in here will be returned via the server_id and msg arguments to recv.
        virtual bool deliver(uint64_t server_id, std::auto_ptr<e::buffer> msg) = 0;
        // Send a message.  A BUSYBEE_SUCCESS returncode guarantees at most that
        // the message is enqueued in userspace.
        virtual busybee_returncode send(uint64_t server_id,
                                        std::auto_ptr<e::buffer> msg) = 0;
        // Receive a message or notification of another event.  server_id is
        // filled in on BUSYBEE_SUCCESS or BUSYBEE_DISRUPTED.  msg is filled in
        // on BUSYBEE_SUCCESS.  Otherwise, id/msg left unchanged.
        //
        // The thread state may be taken offline
        virtual busybee_returncode recv(e::garbage_collector::thread_state* ts,
                                        int timeout, uint64_t* server_id,
                                        std::auto_ptr<e::buffer>* msg) = 0;
        // Receive a notification of a non-message event.  This will never
        // return a message, and thus never return BUSYBEE_SUCCESS.  The primary
        // use for this function is to receive notification of transport layer
        // failures without taking delivery of a message a time when the message
        // could not be processed.
        //
        // The thread state may be taken offline
        virtual busybee_returncode recv_no_msg(e::garbage_collector::thread_state* ts,
                                               int timeout, uint64_t* server_id) = 0;
        // Return the address used for the channel
        virtual busybee_returncode get_addr(uint64_t server_id, po6::net::location* addr) = 0;
        // Shutdown the server, forcing all subsequent recv to return BUSYBEE_SHUTDOWN
        virtual busybee_returncode shutdown() = 0;
};

class busybee_client
{
    public:
        static busybee_client* create(busybee_controller* controller);

    public:
        busybee_client();
        virtual ~busybee_client() throw ();

    public:
        // Enqueue a message in the "recv" queue.  The server_id and msg passed
        // in here will be returned via the server_id and msg arguments to recv.
        virtual bool deliver(uint64_t server_id, std::auto_ptr<e::buffer> msg) = 0;
        // Send a message.  A BUSYBEE_SUCCESS returncode guarantees at most that
        // the message is enqueued in userspace.
        virtual busybee_returncode send(uint64_t server_id,
                                        std::auto_ptr<e::buffer> msg) = 0;
        // Receive a message or notification of another event.  server_id is
        // filled in on BUSYBEE_SUCCESS or BUSYBEE_DISRUPTED.  msg is filled in
        // on BUSYBEE_SUCCESS.  Otherwise, id/msg left unchanged.
        virtual busybee_returncode recv(int timeout, uint64_t* server_id,
                                        std::auto_ptr<e::buffer>* msg) = 0;
        // Receive a notification of a non-message event.  This will never
        // return a message, and thus never return BUSYBEE_SUCCESS.  The primary
        // use for this function is to receive notification of transport layer
        // failures without taking delivery of a message a time when the message
        // could not be processed.
        virtual busybee_returncode recv_no_msg(int timeout, uint64_t* server_id) = 0;
        // The file descriptor to poll for activity on this busybee client
        virtual int poll_fd() = 0;
        // Set an external fd to be monitored by the internal poll
        virtual busybee_returncode set_external_fd(int fd) = 0;
        // Reset the client's internal data structures.  Deleting then
        // recreating a busybee instance can change the poll_fd().  This method
        // accomplishes the same result as delete/recreate, but guarantees the
        // poll_fd stays the same.
        virtual busybee_returncode reset() = 0;
};

class busybee_single
{
    public:
        static busybee_single* create(const po6::net::hostname& host);
        static busybee_single* create(const po6::net::location& host);

    public:
        busybee_single();
        virtual ~busybee_single() throw ();

    public:
        // File descriptor to poll for activity
        virtual int poll_fd() = 0;
        // Send a message.  A BUSYBEE_SUCCESS returncode guarantees at most that
        // the message is enqueued in userspace.
        virtual busybee_returncode send(std::auto_ptr<e::buffer> msg) = 0;
        // Receive a message or notification of another event.  msg is filled in
        // on BUSYBEE_SUCCESS.  Otherwise, msg left unchanged.
        virtual busybee_returncode recv(int timeout, std::auto_ptr<e::buffer>* msg) = 0;
};

bool busybee_discover(po6::net::ipaddr* ip);

#endif // busybee_h_
