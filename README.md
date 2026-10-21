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
Published lane messages include the current Deckr envelope session fields
(`senderSessionId` and, for direct controller traffic, `recipientSessionId`) and
the matching NATS headers. Hardware discovery candidates are advertised through
Beacon. After a controller claim is negotiated, device command/input routing is
fenced only by valid Concord hardware-claim contracts and participant tokens.

```sh
deckr-mirabox-manager \
  --nats-url nats://127.0.0.1:4222
```

Set `--manager-id` only when you want a stable deployment/location name, such as
when controller device config pins a manager to a room:

```sh
deckr-mirabox-manager \
  --manager-id kitchen \
  --nats-url nats://127.0.0.1:4222
```

Environment variables:

- `DECKR_MANAGER_ID` (optional; overrides the `mirabox-rust-<hostname>` default)
- `DECKR_NATS_URL`
- `DECKR_CONCORD_TOKEN_REFRESH_SECONDS` (optional requested cadence; defaults to `60`)
- `DECKR_STATE_RECONCILE_SECONDS` (optional; defaults to `300`)

## Build

This crate is built and tested with Rust 1.88.

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

### OpenWrt on Raspberry Pi 1

The Raspberry Pi 1 OpenWrt build targets the tested device profile:

```text
OpenWrt: 24.10.4
Target:  bcm27xx/bcm2708
Arch:    arm_arm1176jzf-s_vfp
SDK:     openwrt-sdk-24.10.4-bcm27xx-bcm2708_gcc-13.3.0_musl_eabi.Linux-x86_64.tar.zst
Rust:    arm-unknown-linux-musleabihf
```

Build the OpenWrt package and a raw binary tarball with:

```sh
just openwrt-pi1
```

On macOS this command runs the Linux OpenWrt SDK inside Docker. On Linux x86_64
it runs the SDK directly. The command writes:

- `dist/deckr-mirabox-manager_<version>-1_arm_arm1176jzf-s_vfp.ipk`
- `dist/deckr-mirabox-manager-openwrt-24.10.4-bcm2708-pi1.tar.gz`
- `dist/deckr-mirabox-manager-openwrt-24.10.4-bcm2708-pi1.*.txt`

Install the package on the Pi:

```sh
opkg install ./deckr-mirabox-manager_<version>-1_arm_arm1176jzf-s_vfp.ipk
```

For a quick raw-binary test instead:

```sh
tar -xzf deckr-mirabox-manager-openwrt-24.10.4-bcm2708-pi1.tar.gz
./deckr-mirabox-manager-openwrt-24.10.4-bcm2708-pi1/deckr-mirabox-manager --help
```

The build stages a static `libusb` archive with the OpenWrt toolchain and uses
the `hidapi` static libusb backend. It fails if the produced binary references
glibc, such as `/lib/ld-linux-armhf.so.3` or `libc.so.6`. The expected Pi 1
artifact is a statically linked ARM EABI5 binary.

## Layout YAML

The manager uses `serde_yaml` only for repository-owned layout files under
`layouts/built-in`. Those files are linted during the build and embedded in the
binary; runtime or user-supplied YAML is not accepted.

## GitHub Actions

The build workflow runs formatting, clippy, tests, coverage, and release builds for
Linux, macOS, Windows, and OpenWrt Raspberry Pi 1. Pull requests, merge queue
runs, and pushes to `main` verify the full build matrix. Pushing a tag that
starts with `v`, such as `v0.1.0`, packages the binaries and creates or updates
the matching GitHub Release.

CI checks out `kws/deckr-core-rust` at `.deckr-core-revision` for every build.
Private repository access uses `DECKR_REPO_TOKEN` with read access to that core.

Ordinary alpha development updates Git commits and dependency pins without
changing package versions or creating release tags. Releases require a separate
explicit release request.

## Shared core dependency

Build with the sibling `deckr-core-rust` checkout at the exact Git commit in
`.deckr-core-revision`. CI uses that same commit for every target. After cloning
`kws/deckr-core-rust` alongside this repository, select it with:

```bash
git -C ../deckr-core-rust checkout --detach "$(cat .deckr-core-revision)"
cargo test --locked
```

The core owns Beacon/Concord lifetimes, descriptor validation, standard capability
schemas, and command authorization. Queued output retains `HardwareCommandContext`
and checks its authority before hardware writes. Hardware-specific protocol
encoding, discovery, and safe reset behavior remain in this repository.
