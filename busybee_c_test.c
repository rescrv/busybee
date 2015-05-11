// C
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// busybee
#include "busybee_mapper_c.h"
#include "busybee_mta_c.h"

static char const *const server_addrs[] = {"127.0.0.1", "127.0.0.1"};
static uint16_t const server_ports[] = {5555, 5556};
static int const num_threads = 2;


int
lookup(void* user_data,
       uint64_t server_id,
       const char** address,
       uint16_t *port)
{
    size_t index = (server_id >> 32) - 1;
    *address = server_addrs[index];
    *port = server_ports[index];
    return 1;
}

int main()
{
    busybee_mapper* mapper = busybee_mapper_create(NULL, &lookup);
    
    uint64_t sid_one = (1ULL << 32);
    busybee_mta* bb_one = busybee_mta_create(mapper, server_addrs[0],
                                             server_ports[0], sid_one,
                                             num_threads);


    uint64_t sid_two = (2ULL << 32);
    busybee_mta* bb_two = busybee_mta_create(mapper, server_addrs[1],
                                             server_ports[1], sid_two,
                                             num_threads);

    #define check(condition, print) \
        if (!(condition)) { \
            print; \
            busybee_mta_delete(bb_one); \
            busybee_mta_delete(bb_two); \
            return EXIT_FAILURE; \
        }

    busybee_returncode rc;

    const char* msg = "Hello Derek what's up";
    rc = busybee_mta_send(bb_one, sid_two, msg, strlen(msg));
    check(rc == BUSYBEE_SUCCESS, printf("send() failed: %d\n", rc));

    uint64_t from;
    const char* reply;
    size_t reply_sz;
    rc = busybee_mta_recv(bb_two, &from, &reply, &reply_sz);
    check(rc == BUSYBEE_SUCCESS, printf("recv() failed: %d\n", rc));
    check(reply_sz == strlen(msg),
          printf("reply's size doesn't match the size of the sent message\n"));
    check(memcmp(msg, reply, reply_sz) == 0,
          printf("reply (%*.s) doesn't match the sent message (%s)\n",
                 (int) reply_sz, reply, msg));
    check(from == sid_one, printf("sender's server id isn't the correct value\n"));

    busybee_mta_delete(bb_one);
    busybee_mta_delete(bb_two);

    printf("All tests passed.\n");
    
    return EXIT_SUCCESS;
}