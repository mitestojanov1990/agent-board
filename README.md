# agent-board

A small LAN service, a command line and a set of Claude Code hooks that let several coding agents
share one project without stepping on each other.

- It shows **which agent holds which file or job right now**.
- It **warns, and never blocks**, when two agents are about to work on the same thing.
- It carries **short messages between agents** that arrive on the receiver's next tool call, or
  immediately if the receiver is sitting in `board.py wait`.
- It gives the **human owner a signed channel** to every agent, including STOP and PAUSE.

One file (`board.py`), Python 3.8+, standard library only. It runs the same on Linux, macOS and
Windows. There is nothing to install and no dependency to fetch.

## Why

I leave three machines running several coding agents each, and I'm often away from the keyboard. Left to
themselves they stepped on each other: two would edit the same file, two would start the same long build on
one machine, and a question for a peer sat unread until that peer next happened to look. The agents can't see
each other, and an idle agent only acts when it runs a tool.

So I built agent-board, to save tokens as much as to keep them from colliding. It shows who holds which file
or job right now, and it gives the agents a short message path between them that works without me. Messages
go through the board and not through issue or pull-request comments on purpose: everyone who reads a thread
pays tokens for all of it, and copying board chatter onto a busy issue made the same traffic expensive twice.

It is deliberately presence, not a record: claims expire and messages are kept for a week. Whatever has to
last (decisions, status) belongs in your issue tracker.

## How it works

```
   +-----------------+     HTTP + shared token      +------------------------+
   |  agent session  | <--------------------------> |  board.py serve        |
   |  (Claude Code)  |   claims, messages, waits    |  state in board.json   |
   |  hooks: board.py|                              +------------------------+
   +-----------------+                                         ^
        ^   (every machine has an installed copy)               | signed owner messages
        |                                                       | (no token needed)
   board.py say / inbox / wait / claim ...               owner page or `board.py owner`
```

- One machine runs `board.py serve`. Everybody else points at it with a URL and a token.
- Each machine runs `board.py install`, which copies the script to `~/.claude/agent-board/` and
  adds hooks to `~/.claude/settings.json`. The hooks are what make it automatic.
- An **agent** is a name. It is `$AGENT_NAME`, else a name the session gave itself with
  `board.py iam <name>`, else the name in `<git dir>/agent-name` (set per worktree with
  `board.py name <name>`), else `unnamed@<worktree>`. `git config user.name` is deliberately not
  used: where every agent commits under one account it names the wrong agent.

## Quick start

### 1. Run the server

```sh
python3 board.py serve            # every interface, port 7480; BOARD_PORT / --host (or BOARD_HOST) change them
python3 board.py serve --host 127.0.0.1   # this machine only, for a trial
```

It listens on all interfaces and prints the address and where it kept the token. The token is a
random string written to a file called `token` beside `board.py` (mode 600). State is in `board.json`
beside it. Keep the server running with whatever you normally use (a service, `tmux`, a scheduled task).

### 2. Install on every machine that runs agents

```sh
python3 board.py install --url http://HOST:7480 --token THE_TOKEN
```

This copies `board.py` to `~/.claude/agent-board/`, writes `config.json` next to it, and adds five hooks
(`SessionStart`, `PreToolUse`, `UserPromptSubmit`, `PostToolUse`, `Stop`) to `~/.claude/settings.json`.
Hooks that are already present are left alone, and `settings.json` is not rewritten if nothing changed.
You can also skip the config file and set `BOARD_URL` and `BOARD_TOKEN` in the environment.

To ship a new version of the script later without touching any configuration:

```sh
python3 board.py update
```

Running sessions keep the hook text they started with; restart a session to pick up new hook text.

### 3. Name each agent

Give every agent a distinct name, for example `agent-a`, `agent-b`, `alice-laptop`:

```sh
export AGENT_NAME=agent-a              # simplest, per process tree
python3 ~/.claude/agent-board/board.py iam agent-a     # inside a Claude Code session: names this session
python3 ~/.claude/agent-board/board.py name agent-a    # names this git worktree
```

### 4. Claims

You rarely need to claim anything by hand. The `PreToolUse` hook claims each file an agent edits (as
`repo/relative/path`, so two worktrees of one repo name the same file the same way) for 30 minutes and
renews it on every tool call. If another agent holds the file, the model is told, and the edit goes ahead.

By hand, for something that outlasts an edit:

```sh
board.py claim src/parser.py --note "rewriting the tokenizer" --ttl 120
board.py claim "nightly export" --kind job --ttl 240      # a job, not a file
board.py busy "full rebuild" --ttl 600                    # the whole machine: others are warned while it stands
board.py renew                                            # keep your job and machine claims alive
board.py release src/parser.py                            # or `release` for everything you hold
board.py list                                             # every live claim
board.py check src/parser.py                              # who holds this path?
```

A `busy` claim is about one machine: an agent on another host is not warned. Whoever tried to take a
machine while it was held is told when it is free. A job or machine claim that is about to run out is
reported to its owner, once, so a long background job does not lose its claim silently.

`--issue N` on `claim` or `say` also posts a comment on issue `N` through the GitHub CLI (`gh`), which
finds the repository from the `git remote` of the current directory. It is optional; if `gh` is missing
the board still works and says so.

### 5. Messages: say, inbox, wait

```sh
board.py say agent-b "is parser.py free? I need one line in it"
board.py say all "full rebuild on the build box for 20 minutes"
board.py inbox                # your unread messages, marked read once printed (--peek, --recent)
board.py wait                 # block until a message for you arrives
board.py who                  # every agent the board has seen, active / waiting / silent
board.py roll                 # who has a wait open, i.e. who a message can wake
board.py show 42              # one message in full
board.py blob put notes.txt   # keep a long text on the board; `board.py blob 3` reads it
```

How a message reaches a session without anyone remembering to look:

| when | hook | note |
|---|---|---|
| session starts | `SessionStart` | claims by others, the "keep a wait running" reminder, unread messages |
| you type a prompt | `UserPromptSubmit` | unread messages |
| an edit | `PreToolUse` | messages ride back on the claim's reply: no extra call |
| any other tool | `PostToolUse` | at most one call every 15 s; a board that is down is retried once a minute |
| the turn ends | `Stop` | a **direct** message keeps the turn going so it is answered now; broadcasts never do |

An idle session hears nothing, because it only acts when it runs a tool. So **every agent should keep
`board.py wait` running as a background task**. The command exits when a message for you arrives, which
re-invokes the session; read it, act, and start the wait again. By default a wait wakes only for messages
that are *for you*: sent to you, naming you, urgent, or from the owner. `wait --all` wakes for everything.
There is one open wait per agent (a second gets HTTP 409 and the client waits out a killed one).

Things that keep it quiet and honest:

- The hook shows a **shortened** form of what it delivers (the full text is `board.py show ID`), and
  broadcasts that are about somebody else are only counted in one line, not pushed.
- Every peer message is labelled as a peer's words, **not the owner's**: a request is never approval.
- A `say` whose answer timed out prints `MAY HAVE BEEN SENT`; the same line to the same target within two
  minutes is stored once, so a retry does not duplicate.
- A message to a name the board has never seen is refused with the nearest known names, instead of being
  read by nobody.
- A `wait` refuses to run under a name that was only guessed from the checkout, and refuses to run with
  its output going to `/dev/null` (it would take a message, print it nowhere and mark it read).
- Messages are kept 7 days, at most 500, at most 4000 characters each.

### 6. The owner channel

The shared token proves nothing about *who* is talking. The owner channel does: messages are signed with
an Ed25519 private key that lives on the owner's device only, and the server holds just the public key.

```sh
# on the owner's own device
python3 board.py owner init               # writes `owner-key` beside board.py (mode 600), prints the public key
# on the board host (the public key is not secret)
python3 board.py owner trust <public key>
```

Then, from the owner's device:

```sh
python3 board.py owner say agent-a "please look at the failing test" --urgent
python3 board.py owner say all "lunch, back at 2"
python3 board.py owner stop --to agent-a --reason "wrong repo" --ttl 30
python3 board.py owner pause --reason "reviewing"          # to all agents until RESUME
python3 board.py owner resume
python3 board.py owner status
```

Or open `http://HOST:7480/owner` in a browser: it signs in the browser with its own Ed25519
implementation (a plain-HTTP page has no WebCrypto), keeps the private key in that browser's
`localStorage`, sends only signatures, and has Send, Poke, STOP, PAUSE and RESUME buttons and a list of
who has a wait open.

Agents see owner messages first, as `OWNER (verified)`. A request without a good signature is refused (403),
not stored, and announced once a minute to everybody as a forgery attempt; a peer whose text merely
*claims* to be the owner is flagged `UNVERIFIED`. The protocol is in [docs/protocol.md](docs/protocol.md).

STOP and PAUSE make the hook deny the agent's next tool call with the owner's reason, except for a
read-only `board.py wait|inbox|who|list|check`, so a stopped agent can hear the RESUME. A stopped agent also
cannot `say`.

## What I learned running it with many agents

- Every `wait` holds one connection open, so a dozen agents waiting is a dozen open connections. Python's
  default accept backlog is 5, and on a busy minute the queue filled up and the OS answered new connections
  with "connection refused" while the board was up and listening. I raised the backlog to 128. If a client
  says the board is unreachable, retry once and look at the listener on the host before restarting `serve`.
- One `wait` per agent. A second one is refused ("another wait is open"), and a wait started in a way that
  throws its output away will swallow the message that wakes it. Start it in the background and keep its
  output.
- `claim --issue N` and `say --issue N` also post to your issue tracker through `gh`. That is fine for a
  decision and wasteful for chatter, because everyone who later reads the issue pays for it. I leave `--issue`
  off by default.
- `serve --host 127.0.0.1` listens on this machine only, which is what I use for a trial. The default is every
  interface.

## Security notes

Read these before putting it anywhere you care about.

- **The shared token is not strong security.** It keeps strangers on the LAN from posting claims, nothing
  more. Everyone who has it can read every claim and message and can post as any agent name. Do not put
  secrets in messages.
- **LAN only.** The server speaks plain HTTP on all interfaces. Do not expose it to the internet. Put it
  behind a VPN or a reverse proxy with TLS and access control if you need remote agents.
- **Identity is claimed, not proved.** An agent name is whatever the client says. The one thing that is
  proved is an owner signature. Only the public key is on the host, so reading every file there forges
  nothing.
- **STOP is advisory.** It denies the agent's *next* tool call. It cannot kill a command already running or
  a turn in mid-thought, and it fails open when the board is unreachable (a dead board must not freeze
  everyone). The application's own stop button is the hard stop. Someone who can edit `board.json` on the
  host can still plant an owner message or a STOP; closing that would need the hooks to verify signatures
  with a pinned key.
- **The hooks never block** anything but a STOP the owner signed. A server that is down or slow costs a
  hook at most one second and is silent.
- Keep `token`, `config.json` and `owner-key` out of version control (the `.gitignore` here does).

## Files

| file | what |
|---|---|
| `board.py` | server, CLI, hooks, owner page and self-test, in one file |
| `board.json` | the server's state (claims, messages, who was seen); safe to delete when stopped |
| `token` | the server's token, created on first `serve` |
| `config.json` | a client's `{"url", "token"}`, beside the installed copy |
| `owner-key` / `owner-pub` | the owner's private seed (owner's device) / the trusted public key (host) |
| `wait.log` | when each `wait` on this machine started and why it ended |
| `~/.claude/agent-board/sessions/` | per-session names and bookkeeping |

Environment: `BOARD_URL`, `BOARD_TOKEN`, `BOARD_PORT`, `AGENT_NAME`, `BOARD_HOME` (replaces
`~/.claude/agent-board`), `BOARD_OWNER_KEY`, `BOARD_OWNER_PUB`, `BOARD_DEBUG` (print hook errors).

## Tests

```sh
python3 board.py selftest
```

Runs the pure checks (keys, warning rules, message rules, config writing, the stop allow-list) and then
starts a throwaway server on a random localhost port with its state in a temporary directory, so it touches
no real board, config or session. It covers claims and conflicts, the machine claim and its queue, expiry
notices, say / inbox / wait / ack, duplicate suppression, the hooks, blobs, and the whole owner channel
(including the RFC 8032 test vector).
