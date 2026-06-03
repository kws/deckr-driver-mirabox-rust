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
- `DECKR_CONCORD_TOKEN_REFRESH_SECONDS` (optional; defaults to `15`)
- `DECKR_STATE_RECONCILE_SECONDS` (optional; defaults to `300`)

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

## GitHub Actions

The build workflow runs formatting, clippy, tests, coverage, and release builds for
Linux, macOS, Windows, and OpenWrt Raspberry Pi 1. Pull requests, merge queue
runs, and pushes to `main` verify the full build matrix. Pushing a tag that
starts with `v`, such as `v0.1.0`, packages the binaries and creates or updates
the matching GitHub Release.

The workflow checks out the sibling `kws/deckr` repository because this crate uses the
local Deckr Rust core path dependency. If the current branch or tag exists in
`kws/deckr`, the workflow uses it; otherwise it uses Deckr's default branch. Set the
repository variable `DECKR_REF` to force a specific Deckr branch, tag, or
40-character commit SHA. For private repository access, add a `DECKR_REPO_TOKEN`
secret with read access to `kws/deckr`.

## Release Flow

Driver releases are made from a release commit that pins the workflow to the
exact Deckr commit used for the build. This keeps the release reproducible even
when the driver tag does not exist in the upstream `kws/deckr` repository.

1. Choose the Deckr commit SHA to release against.
2. Update `version` in `Cargo.toml` and `Cargo.lock`.
3. Replace every workflow `DECKR_REF` expression with the chosen 40-character
   Deckr SHA.
4. Run formatting, clippy, and tests.
5. Commit the change as `Release vX.Y.Z`.
6. Tag that commit as `vX.Y.Z` and push the branch and tag.
7. Wait for GitHub Actions to publish the GitHub Release artifacts.
8. Restore every workflow `DECKR_REF` value to
   `${{ vars.DECKR_REF || github.head_ref || github.ref_name }}`.
9. Commit the restore as `Restore unpinned Deckr workflow ref` and push it.
