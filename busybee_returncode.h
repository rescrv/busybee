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

#ifndef busybee_returncode_h_
#define busybee_returncode_h_

// C++
#include <iostream>

// busybee_returncode occupies [4608, 4864)
enum busybee_returncode
{
    BUSYBEE_SUCCESS     = 4608,
    BUSYBEE_SHUTDOWN    = 4609,
    BUSYBEE_QUEUED      = 4610,
    BUSYBEE_POLLFAILED  = 4611,
    BUSYBEE_DISCONNECT  = 4612,
    BUSYBEE_CONNECTFAIL = 4613,
    BUSYBEE_ADDFDFAIL   = 4614,
    BUSYBEE_BUFFERFULL  = 4615,
    BUSYBEE_TIMEOUT     = 4616,
    BUSYBEE_EXTERNAL    = 4617
};

#define str(x) #x
#define xstr(x) str(x)
#define stringify(x) case (x): lhs << xstr(x); break

inline std::ostream&
operator << (std::ostream& lhs, busybee_returncode rhs)
{
    switch (rhs)
    {
        stringify(BUSYBEE_SUCCESS);
        stringify(BUSYBEE_SHUTDOWN);
        stringify(BUSYBEE_QUEUED);
        stringify(BUSYBEE_POLLFAILED);
        stringify(BUSYBEE_DISCONNECT);
        stringify(BUSYBEE_CONNECTFAIL);
        stringify(BUSYBEE_ADDFDFAIL);
        stringify(BUSYBEE_BUFFERFULL);
        stringify(BUSYBEE_TIMEOUT);
        stringify(BUSYBEE_EXTERNAL);
        default:
            lhs << "unknown returncode";
            break;
    }

    return lhs;
}

#undef stringify
#undef xstr
#undef str

#endif // busybee_returncode_h_
