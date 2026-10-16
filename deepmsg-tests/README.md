# `deepmsg-tests` — the C and Java that tests deepmsg

Everything in this directory is **deepmsg's**, not Aeron's. It lives in this fork because
it is C and Java, and the `deepmsg` repository is a Rust rewrite that is not supposed to
carry either: a `.c` or a `.java` file in that tree is a file whose build, classpath and
lifetime belong to a different toolchain, and the two checkouts that build them are the
reference's own.

The branch this directory is on is based on
**`fix/the-drop-wait-races-the-flood`**, which carries the two **test-side holds** deepmsg
runs the reference's C archive suite with (three `sleep`s and an `#include <thread>` in
`aeron_archive_persistent_subscription_test.cpp`, and one in
`PersistentSubscriptionWrapperTest.cpp`). They are on one branch together on purpose: a
checkout of this branch is the whole of what `deepmsg` needs from a *modified* reference.

## What is here, and who reads it

### `probes/` — C programs linked against the reference's own client

| file | what it measures |
|---|---|
| `destination_probe.c` | a reference client adding a destination to our driver (`ADD_DESTINATION` decoded and answered by code that shares nothing with ours) |
| `ipc_response_probe.c` | the `control-mode=response` shape over IPC |
| `spy_probe.c` | a spy subscription at the reference's client, against our driver's counters |

They are compiled by the test that needs them:

    cc -std=gnu11 -Wall -Werror -I <reference>/aeron-client/src/main/c \
       -o <out> probes/<name>.c -L <reference>/cppbuild/Release/lib -laeron \
       -Wl,-rpath,<reference>/cppbuild/Release/lib

★ **`<reference>` is the `aeron` checkout, not this one.** The headers and `libaeron` are
the reference's build; this repository is where the *sources* of our tests live. The two
are deliberately separate checkouts.

### `java/` — programs compiled against `aeron-all-1.53.2.jar`

| file | what it measures |
|---|---|
| `ArchiveConnectProbe.java` | the archive connect over a `control-mode=response` IPC response channel |
| `RecordingEventsProbe.java` | the three recording events, decoded by the reference's own `RecordingEventsAdapter` |
| `ReferenceArchiveClientProbe.java` | ★ the reference's own `AeronArchive` client against **our** archive, on the **reference's** Java media driver — the mixed gate |

    javac -proc:none -cp <jar> -d <classes> java/<name>.java

Run with `<jar>` on the classpath and the two `--add-opens` flags the reference's own
build passes (`tests/src/driver.rs` in `deepmsg` has them as `AGRONA_JVM_ARGS`).

### `generators/` — programs that produce committed fixtures

| file | produces |
|---|---|
| `catalog/GenerateCatalog.java` | `deepmsg/tests/fixtures/catalog/{archive.catalog,catalog.tsv}` |
| `catalog/RecoverCatalog.java` | the stop position the reference's own `Catalog` recovers |
| `mark-file/GenerateMarkFile.java` | `deepmsg/tests/fixtures/mark-file/{archive-mark.dat,mark-file.tsv}` |
| `sbe/Generate.java` | `deepmsg/tests/fixtures/sbe/*.bin` and `golden.tsv` |

★ **The outputs stay in `deepmsg`** — they are committed fixtures the Rust tests read
(`include_bytes!`, `golden.tsv`). What moved here is the program that makes them, and
each fixture's own `README.md` in `deepmsg` says how to run it from here.

## How `deepmsg` finds this

One environment variable, and a default path relative to `deepmsg/tests/`:

    DEEPMSG_AERON_TESTS   default: ../../aeron-deepmsg/deepmsg-tests

`deepmsg/tests/src/aeron_tests.rs` resolves it. **A missing directory is a skip, not a
failure**: a workspace that has not cloned this fork should not turn the interop suite
red, exactly as a missing reference build does not.

## What is deliberately not here

* **`archive-shim`** — the thing that stands in for the reference's archive in its C
  suite. It is a **Rust** binary in `deepmsg` (`crates/tools/src/bin/archive-shim.rs`),
  and the C suite is built with `-DJava_JAVA_EXECUTABLE=<that binary>`.
* **`crates/dpdk/shim/shim.c`** — product FFI, not a test.
* **`bench/tools/HdrQuantiles.java`** — a bench read-out tool that belongs beside the
  bench scripts that call it.
