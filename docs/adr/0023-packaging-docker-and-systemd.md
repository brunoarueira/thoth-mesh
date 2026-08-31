# 23. Packaging: a multi-stage Docker image and a hardened systemd unit

## Status

Accepted

## Context

Issue #50 (Phase 9): today the only way to run `thoth-mesh-node` or
`thoth-mesh` (the CLI) is `cargo build`/`cargo run` from a checkout of
this repo, or `cargo install` from the crates.io releases issue #37/#38
already set up. There's no container image and no service-manager
integration - "someone can actually run this somewhere real" isn't
true yet. The issue's "Known shape" named three deliverables: a
Dockerfile, a systemd unit, and install docs - explicitly flagging
that the shape of each is worth refining via ADR before building.

## Decision

### Docker: one multi-stage Dockerfile, both binaries in one image

A single `Dockerfile` at the repo root, two stages:

- **Builder**: `rust:1-bookworm` (the full image, not `-slim`) -
  `thoth-mesh-tls` depends on `ring` (see ADR-0016), which compiles
  C/assembly and needs a C toolchain; the full `bookworm` image
  already has one (`build-essential`), avoiding an extra `apt-get
  install` layer for a builder stage that's discarded anyway. `1`
  (latest 1.x stable), not a pinned patch version, and deliberately
  *not* the workspace's declared `rust-version = "1.85"` either -
  building against that pin during this ADR's work turned up that it's
  already stale (the code uses a `let`-chain, which needs a newer
  compiler than 1.85 actually has), undetected until now because CI
  itself floats on `dtolnay/rust-toolchain@stable` rather than pinning
  a version. Tracking stable here matches what CI already effectively
  verifies, rather than hard-coding a version this repo isn't actually
  tested against. The `rust-version` field's own staleness is a
  pre-existing, separate issue - flagged, not fixed, here. Runs `cargo
  build --release --workspace` once - this crate's `[profile.release]`
  already sets `lto = true`/`codegen-units = 1` (slower build,
  smaller/faster binary), unchanged by packaging.
- **Runtime**: `gcr.io/distroless/cc-debian12:nonroot` - no shell, no
  package manager, no libc headers, nothing beyond the C/C++ runtime
  libraries the dynamically-linked binaries actually need (`libc`,
  `libgcc_s`), and it already runs as a non-root `nonroot` user (UID
  `65532`) with no extra user-creation step required. Chosen over a
  fully static `musl`/`scratch` build: `ring`'s build script targeting
  `musl` is a meaningfully bigger lift (cross toolchain, verifying
  every transitive dependency actually supports it) for a size/attack-
  surface win `distroless/cc` already gets most of the way to. Worth
  revisiting if this ever needs to run somewhere `glibc` genuinely
  isn't available.

Both `thoth-mesh-node` and `thoth-mesh` (the CLI) are copied into the
runtime image, not just the former. `distroless/cc` has no shell, so
there's no interactive `docker exec -it <container> sh` either way -
but `docker exec <container> /usr/local/bin/thoth-mesh ...` (naming
the binary directly, no shell needed to invoke it) still works, and
having the CLI available in the same image/network namespace is a
genuinely useful debugging affordance for a federated system, at
near-zero extra image size (it shares the builder's dependency
compilation with the node binary).

`ENTRYPOINT` is the node binary; `CMD` defaults to `["--addr",
"0.0.0.0:49500"]` - **not** `thoth_mesh_core::DEFAULT_ADDR`
(`127.0.0.1:49500`), which would be unreachable through Docker's port
publishing (`-p`) from outside the container network namespace
entirely, a easy-to-hit and confusing-to-debug footgun worth defaulting
away from in the image itself rather than only documenting. `CMD` is
fully overridable - `docker run <image> --addr 0.0.0.0:49500 --peer
other:49500 --metrics-addr 0.0.0.0:9090` replaces it outright, same as
any other Docker image's default `CMD`.

### systemd: a hardened unit using `DynamicUser=yes`

An example unit file (`packaging/thoth-mesh-node.service`) rather than
anything installed automatically - operators' systems differ too much
(binary location, config layout) for one unit file to just work
everywhere unedited; it's a documented starting point, same spirit as
the TLS/ACL examples already in `docs/OPERATIONS.md`.

- **`DynamicUser=yes`** instead of a manually-created service account:
  systemd allocates and tears down an ephemeral, unprivileged user for
  the unit's lifetime, with no `useradd`/`groupadd` install step
  needed and no leftover account if the service is later removed -
  less for install docs to get right, less to clean up.
- **Argument delivery via `EnvironmentFile=` + unquoted `$NODE_ARGS`
  in `ExecStart=`**: `thoth-mesh-node` takes CLI flags, not
  environment variables, for all of its configuration (`--peer`,
  `--tls-cert`, `--topic-acl`, ...) - there's no env-var config surface
  to bind `Environment=` directives to directly. systemd word-splits
  an *unquoted* `$VAR` reference in `ExecStart=` the same way a shell
  would (a documented systemd.service behavior, distinct from `${VAR}`
  which it does not word-split) - so `EnvironmentFile=/etc/thoth-mesh/
  node.env` setting `NODE_ARGS="--peer a:49500 --peer b:49500 ..."`
  and `ExecStart=/usr/local/bin/thoth-mesh-node $NODE_ARGS` lets an
  operator's actual flags live in one editable file outside the unit
  itself, without needing a wrapper shell script just to expand
  arguments.
- **Standard hardening bundle**: `NoNewPrivileges=yes`,
  `ProtectSystem=strict`, `ProtectHome=yes`, `PrivateTmp=yes`,
  `RestrictSUIDSGID=yes` - all no-cost for a network daemon that reads
  a handful of cert/key files and opens listening sockets, nothing
  filesystem-mutating beyond what those need.
- **`Restart=on-failure`**, a modest `RestartSec` - the daemon itself
  already has connection-level reconnect/backoff (ADR-0012) for peer
  links; this is the outer layer for the process itself dying (panic,
  OOM-killed, etc.), not a replacement for that logic.

### Docs: extend `docs/OPERATIONS.md`, not a new file

New `## Docker` and `## systemd` sections alongside the existing
`## Build / install` section, rather than a separate `DEPLOYMENT.md` -
`OPERATIONS.md` is already this project's one "how to actually run
this" document (build, quickstart, TLS, ACLs, metrics); packaging is
one more way to run the same daemon, not a different concern needing
its own doc. `cargo install thoth-mesh-node`/`cargo install
thoth-mesh-cli` (the crates.io releases from issue #37/#38) get their
own line in `## Build / install` too, alongside building from source.

## Consequences

The Dockerfile and unit file are new, unenforced-by-CI artifacts - a
future flag rename in `main.rs` doesn't fail a build the way a Rust
API change would; keeping the unit file's `ExecStart=` comment and the
Docker `CMD` in sync with actual flags is a manual-review concern from
here on, not a compiler-checked one. Not addressed here - worth
revisiting (a smoke-test job building the image and running `--help`,
say) if this drifts in practice.

`gcr.io/distroless/cc-debian12` is an external, Google-maintained base
image this project now has a build-time (and runtime, since it's
`FROM` in the shipped Dockerfile) dependency on - a new kind of
dependency this project hasn't taken on before (everything else is a
`crates.io` crate). Acceptable for a v1 packaging story; if that base
image's maintenance posture or supply chain ever becomes a concern,
switching to a from-scratch `musl` static build is the fallback this
ADR already named and declined for now.

This closes #50.
