# Busybee

BusyBee provides a messaging abstraction on top of TCP sockets.

BusyBee is a refined version of the HyperDex event loop.  It exposes a
"messaging" abstraction on top of TCP and automatically packs/unpacks messages
on the wire.  At the core of BusyBee is a thread-safe event loop that enables
multiple threads to send and receive messages concurrently.

## Build

Busybee depends on [po6](https://github.com/rescrv/po6) and [e](https://github.com/rescrv/e).
They can be built with the exact commands described below.

On Linux:

```
autoreconf -i
./configure
make
```

Then, if you want to install BusyBee:

```
sudo make install
```

## Bindings

Busybee itself comes with API for both C and C++.

There also exists a binding for Rust: https://github.com/derekchiang/rust-busybee
