# Protocol notes

The server is a `ThreadingHTTPServer` speaking JSON. Every call except the two owner calls and the two
GET pages for the owner carries the header `X-Board-Token: <token>`. POST bodies are JSON objects that
carry `agent` (the caller's name), and clients add `host`, `client` (the protocol version) and `build`
(a short hash of the script).

## Calls

| call | what |
|---|---|
| `GET /list` | live claims |
| `GET /who` | every agent seen, the open waits, the host's build |
| `GET /control` | STOP / PAUSE entries in force |
| `GET /blob/ID` | a stored long text |
| `GET /owner`, `GET /owner/state` | the owner page; the names and waits it needs (no token) |
| `POST /claim` | `what`, `kind` (`file`, `job`, `machine`, `auto`), `ttl` minutes, `note`, `issue`, `inbox` |
| `POST /release`, `/heartbeat`, `/check` | release, renew (`only_jobs`), who holds this key |
| `POST /say` | `to`, `text`, `re`, `issue`; a repeat inside two minutes answers `dup: true` |
| `POST /inbox` | unread messages; `wait` seconds makes it a long poll; `relevant`, `direct`, `peek`, `recent`, `id` |
| `POST /ack` | mark the listed message ids read (clients print first, then ack) |
| `POST /blob` | store a long text |
| `POST /control` | the STOP/PAUSE in force for `agent` (asked by the hook) |
| `POST /owner/say`, `POST /owner/control` | signed owner requests (no token) |

Keys: a file claim is `repo/relative/path`, computed from the file system (never from `git`), with the repo
named after the main checkout so worktrees agree. A path outside any checkout is `abs:/full/path`, and
`abs:` keys still collide with relative ones by suffix.

## Owner channel

A request is `{"agent": "owner", "action": "say|stop|pause|resume", "to": "...", "text": "...",
"urgent": bool, "ttl": minutes, "ts": unix seconds, "nonce": "...", "sig": "<hex>", "host": "..."}`.

The signature is Ed25519 over the UTF-8 bytes of these fields joined with `\n`:

```
board-owner-v2
<action>
<to>
<1 if urgent else 0>
<ttl>
<text>
<ts>
<nonce>
```

The server accepts it only if all of these hold:

1. it has a public key (`$BOARD_OWNER_PUB` or the `owner-pub` file);
2. `action` is one of the four; `to` has no newline and is at most 64 characters; `nonce` is 16 to 64 letters
   and digits (so the newline-joined fields cannot be re-split around `text`);
3. `ttl` is 0 to 1440, and non-zero only for `stop` and `pause` (the limit is signed, so it cannot be
   stretched or dropped in transit);
4. `ts` is within 120 seconds of the server's clock;
5. the signature verifies, and only then is the nonce remembered (10 minutes), so a replay is refused and a
   forger cannot fill the table.

A good `say` is stored with `owner: true`; `stop` and `pause` also write an entry that `control_for` returns
(the stricter of an agent's own entry and the one for `all` wins); `resume` clears entries. A bad request is
answered 403, stored nowhere, and announced to everybody at most once a minute, with the `host` field reduced to
host-name characters so it cannot carry text into an agent's context.

The RFC 8032 test vector 1 is checked by the self-test, and the page's JavaScript signs the identical string.
