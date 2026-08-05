# SemSwitch Silo

This repository is an unofficial SemSwitch-maintained fork of [Loophole Labs Silo](https://github.com/loopholelabs/silo).

The initial maintenance baseline is upstream Silo v0.2.20. The first correction fixes NBD dispatcher handling of requests whose payload plus framing exceeds the original fixed receive buffer. The original defect could leave the dispatcher reading into a zero-length slice, fill the NBD socket, and block filesystem synchronization.

The correction has been tested with repeated local Firecracker live migration and bidirectional two-host Firecracker live migration.

SemSwitch intends to keep changes narrow and compatible with the original `github.com/loopholelabs/silo` module imports.
