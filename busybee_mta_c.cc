// C++
#include <sstream>

// po6
#include <po6/net/location.h>

// busybee
#include "busybee_constants.h"
#include "busybee_mta.h"
#include "busybee_mta_c.h"

extern "C" {

busybee_mta*
busybee_mta_create(busybee_mapper* mapper,
                   const char* address,
                   uint16_t port,
                   uint64_t server_id,
                   size_t num_threads)
{
    return new busybee_mta(mapper, po6::net::location(address, port),
                           server_id, num_threads);
}

void
busybee_mta_delete(busybee_mta* bb)
{
    delete bb;
}

#define WRAP(FUNC_NAME) \
    busybee_mta_ ## FUNC_NAME(busybee_mta* bb) \
    { \
        return bb->FUNC_NAME(); \
    }

void WRAP(shutdown);

void WRAP(pause);

void WRAP(unpause);

void WRAP(wake_one);

void WRAP(set_ignore_signals);

void WRAP(unset_ignore_signals);

void WRAP(add_signals);

int WRAP(poll_fd);

void
busybee_mta_set_id(busybee_mta* bb, uint64_t server_id)
{
    return bb->set_id(server_id);
}

void
busybee_mta_set_timeout(busybee_mta* bb, int timeout)
{
    return bb->set_timeout(timeout);
}

busybee_returncode
busybee_mta_get_addr(busybee_mta* bb,
                     uint64_t server_id,
                     const char** address,
                     uint16_t* port)
{
    std::ostringstream addr;

    po6::net::location location;
    busybee_returncode rc = bb->get_addr(server_id, &location);
    if (rc != BUSYBEE_SUCCESS) return rc;
    
    addr << location.address;
    *address = addr.str().c_str();
    *port = location.port;

    return BUSYBEE_SUCCESS;
}

busybee_returncode
busybee_mta_drop(busybee_mta* bb,
                 uint64_t server_id)
{
    return bb->drop(server_id);
}

// Create a buffer with the data given, and also with extra space
// at the beginning for the busybee header.
inline e::buffer*
cstr_to_buffer(const char* msg,
               size_t msg_sz)
{
   e::buffer* buf = e::buffer::create(msg_sz + BUSYBEE_HEADER_SIZE);
   memcpy(buf->data() + BUSYBEE_HEADER_SIZE, msg, msg_sz);
   buf->extend(BUSYBEE_HEADER_SIZE + msg_sz);
   return buf;
}

int
busybee_mta_deliver(busybee_mta* bb,
                    uint64_t server_id,
                    const char* msg,
                    size_t msg_sz)
{
    std::auto_ptr<e::buffer> buf(cstr_to_buffer(msg, msg_sz));
    return bb->deliver(server_id, buf);
}

busybee_returncode
busybee_mta_send(busybee_mta* bb,
                 uint64_t server_id,
                 const char* msg,
                 size_t msg_sz)
{
    std::auto_ptr<e::buffer> buf(cstr_to_buffer(msg, msg_sz));
    return bb->send(server_id, buf);
}

busybee_returncode busybee_mta_recv(busybee_mta* bb,
                                    uint64_t* server_id,
                                    const char** msg,
                                    size_t* msg_sz)
{
    std::auto_ptr<e::buffer> buf;
    busybee_returncode rc = bb->recv(server_id, &buf);
    if (rc != BUSYBEE_SUCCESS) return rc;

    // We don't want the busybee header
    *msg_sz = buf->size() - BUSYBEE_HEADER_SIZE;
    void* msg_cp = malloc(*msg_sz);
    memcpy(msg_cp,
           buf->data() + BUSYBEE_HEADER_SIZE,
           *msg_sz);
    *msg = (char*) msg_cp;

    return BUSYBEE_SUCCESS;
}

}