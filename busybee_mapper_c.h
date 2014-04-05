#ifndef busybee_mapper_c_h_
#define busybee_mapper_c_h_

// C
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct busybee_mapper busybee_mapper;

typedef int (*lookup_func_t)(void* user_data,
                             uint64_t server_id,
                             const char **address,
                             uint16_t *port);

busybee_mapper* busybee_mapper_create(void* user_data,
                                      lookup_func_t lookup);

#ifdef __cplusplus
}
#endif

#endif // busybee_mapper_c_h_