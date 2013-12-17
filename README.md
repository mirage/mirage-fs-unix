This is a pass-through Mirage filesystem to an underlying Unix directory.  The
interface is intended to support eventual privilege separation (e.g. via the
Casper daemon in FreeBSD 11).

The current version only supports `KV_RO`, but will be extended to support the
`FS` filesystem interface too.

* WWW: <https://openmirage.org>
* E-mail: <mirageos-devel@lists.xenproject.org>
