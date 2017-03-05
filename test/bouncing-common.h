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

// STL
#include <vector>

// BusyBee
#include <busybee.h>

class controller : public busybee_controller
{
    public:
        controller(const char** args, size_t args_sz)
            : m_servers()
        {
            if (args_sz % 2 != 0)
            {
                abort();
            }

            for (size_t i = 0; i < args_sz; i += 2)
            {
                po6::net::location server;

                if (!server.set(args[i], atoi(args[i + 1])))
                {
                    std::cerr << "could not parse address " << i << std::endl;
                    abort();
                }

                m_servers.push_back(server);
            }
        }
        ~controller() throw () {}

    public:
        uint64_t server_from_index(unsigned idx)
        {
            return BUSYBEE_CHOSEN_ID_MINIMUM + idx % m_servers.size();
        }
        virtual po6::net::location lookup(uint64_t server_id)
        {
            if (server_id >= BUSYBEE_CHOSEN_ID_MINIMUM &&
                server_id < BUSYBEE_CHOSEN_ID_MINIMUM + m_servers.size())
            {
                return m_servers[server_id - BUSYBEE_CHOSEN_ID_MINIMUM];
            }

            return po6::net::location();
        }
        uint64_t reverse_lookup(const po6::net::location& host)
        {
            for (size_t i = 0; i < m_servers.size(); ++i)
            {
                if (m_servers[i] == host)
                {
                    return i + BUSYBEE_CHOSEN_ID_MINIMUM;
                }
            }

            return 0;
        }

    private:
        std::vector<po6::net::location> m_servers;
};
