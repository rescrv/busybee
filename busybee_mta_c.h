#ifndef busybee_mta_c_h_
#define busybee_mta_c_h_

// C
#include <stdint.h>
#include <stddef.h>

// busybee
#include "busybee_mapper_c.h"
#include "busybee_returncode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct busybee_mta busybee_mta;
typedef enum busybee_returncode busybee_returncode;

busybee_mta* busybee_mta_create(busybee_mapper* mapper,
                                const char* address,
                                uint16_t port,
                                uint64_t server_id,
                                size_t num_threads);

void busybee_mta_delete(busybee_mta* bb);

void busybee_mta_shutdown(busybee_mta* bb);

void busybee_mta_pause(busybee_mta* bb);

void busybee_mta_unpause(busybee_mta* bb);

void busybee_mta_wake_one(busybee_mta* bb);

void busybee_mta_set_id(busybee_mta* bb, uint64_t server_id);

void busybee_mta_set_timeout(busybee_mta* bb, int timeout);

void busybee_mta_set_ignore_signals(busybee_mta* bb);

void busybee_mta_unset_ignore_signals(busybee_mta* bb);

void busybee_mta_add_signals(busybee_mta* bb);

busybee_returncode busybee_mta_get_addr(busybee_mta* bb,
                                        uint64_t server_id,
                                        const char** address,
                                        uint16_t* port);

int busybee_mta_deliver(busybee_mta* bb,
                        uint64_t server_id,
                        const char* msg,
                        size_t msg_sz);

int busybee_mta_poll_fd(busybee_mta* bb);

busybee_returncode busybee_mta_drop(busybee_mta* bb,
                                    uint64_t server_id);

busybee_returncode busybee_mta_send(busybee_mta* bb,
                                    uint64_t server_id,
                                    const char* msg,
                                    size_t msg_sz);

busybee_returncode busybee_mta_recv(busybee_mta* bb,
                                    uint64_t* server_id,
                                    const char** msg,
                                    size_t* msg_sz);

#ifdef __cplusplus
}
#endif

#endif // busybee_mta_c_h_