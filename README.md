# deckr-driver-mirabox-rust

Rust MiraBox hardware manager for Deckr over Core NATS and JetStream KV.

This repo keeps the existing `deckr-mirabox-manager` binary identity and embeds its own
layout assets from `layouts/built-in`, so it no longer depends on files living in the
Python MiraBox repo at build time.

## Contract

This repo stays a pure Cargo project. It is tested against the current Deckr hardware
transport contract rather than expressed as a Python package dependency.

## Runtime

The manager participates as `hardware_manager:<manager-id>` on the
`hardware_messages` lane. By default it uses `mirabox-rust-<hostname>`.

```sh
deckr-mirabox-manager \
  --nats-url nats://127.0.0.1:4222 \
  --state-bucket deckr_state_v1
```

Set `--manager-id` only when you want a stable deployment/location name, such as
when controller device config pins a manager to a room:

```sh
deckr-mirabox-manager \
  --manager-id kitchen \
  --nats-url nats://127.0.0.1:4222 \
  --state-bucket deckr_state_v1
```

Environment variables:

- `DECKR_MANAGER_ID` (optional; overrides the `mirabox-rust-<hostname>` default)
- `DECKR_NATS_URL`
- `DECKR_STATE_BUCKET`

## Build

For normal local development:

```sh
cargo build
cargo test
```

If you use `just`, the same commands are available as:

```sh
just build
just test
```

## Cross-platform builds

The repo includes the cross toolchain setup that used to live in `sidepanel`:

- `Cross.toml`
- `docker/rust-cross/*.Dockerfile`
- `just cross-images`
- `just release`

Build the custom `cross` images first:

```sh
just cross-images
```

Then build release binaries for the supported Linux targets:

```sh
just release
```

This currently produces release binaries for:

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `armv7-unknown-linux-gnueabihf`
- `arm-unknown-linux-gnueabihf`

The `aarch64-unknown-linux-gnu` target covers 64-bit Raspberry Pi OS on newer boards,
`armv7-unknown-linux-gnueabihf` covers 32-bit ARMv7 boards, and
`arm-unknown-linux-gnueabihf` keeps the ARMv6/Raspberry Pi build flags from the previous
cross setup.

## GitHub Actions

The build workflow runs formatting, clippy, tests, coverage, and release builds for
Linux, macOS, and Windows. Pull requests, merge queue runs, and pushes to `main` verify
the full build matrix. Pushing a tag that starts with `v`, such as `v0.1.0`, packages
the binaries and creates or updates the matching GitHub Release.
