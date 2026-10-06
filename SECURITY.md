# Security policy

## Reporting a vulnerability

Please do not open a public issue for a security problem. Report it privately
through GitHub instead:
**[Report a vulnerability](https://github.com/JohanLindvall/diskqueue/security/advisories/new)**
(also under the repository's **Security** tab). Only you and the repository's
maintainers can see the report until an advisory is published.

A report is most useful with the version or commit you tested, the platform and
filesystem, and a way to reproduce it: a short program, or a queue directory
(tarred up) that triggers the problem on open or on read.

Fixes ship as a new tag and are disclosed in a GitHub security advisory that
credits the reporter, unless you would rather not be named.

## Supported versions

`diskqueue` is pre-1.0, and every green build of `main` is tagged `v0.0.N`. Fixes
land on `main` and ship in the next tag; older tags are not patched, so the fix
for any version is to upgrade to the latest.

## What counts

The queue directory is trusted storage. The checksums are `xxhash64`: they catch
accidental damage (torn writes, bit rot, truncation), not tampering, and anyone
who can write to the directory can forge a record that verifies. That is expected
behaviour, not a vulnerability.

What the library does promise, whatever the bytes on disk say, and therefore what
is in scope:

- they never crash the process: a panic on any on-disk input is a vulnerability;
- they never wedge the queue: an open or a read that can make no progress is one
  too;
- a record that fails its checksum is never delivered as data.

Durability bugs that need no attacker, such as a record lost or miscounted after a
crash, are ordinary bugs: please open an issue for those.
