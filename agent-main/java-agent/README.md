# Nezha Java Agent

This module is an embeddable Java implementation of the Nezha agent client protocol.
It is intentionally scoped to the client side only: it connects to an official Nezha
Dashboard, authenticates, reports host/state/GeoIP data, and processes dashboard tasks.

## Current Scope

- gRPC protocol generated from `src/main/proto/nezha.proto`
- metadata auth with `client_secret` and `client_uuid`
- YAML config compatible with the Go agent's main keys
- host and state collection via OSHI, including optional temperature and GPU reporting
- task support for HTTP GET, ICMP reachability, TCP ping, command execution, report config, and apply config
- query tasks honor `disable_send_query` and use configured `dns` servers as fallback resolvers
- GeoIP reporting supports `custom_ip_api`, dashboard restart re-reporting, and the official agent's empty-IP fallback after repeated lookup failures
- streaming task support for terminal, NAT traversal, and file manager over `IOStream`
- unsupported for now: in-place self-update and the Go agent's optional uTLS transport behavior

The streaming implementation follows the Go agent wire formats, including the `ff 05 ff 05`
stream id handshake and the `NZFN`/`NZTD`/`NERR`/`NZUP` file manager protocol. This build has
local protocol and lifecycle coverage; final production confidence still needs an end-to-end
run against a real official dashboard with a real `server` and `client_secret`.

## Build

```bash
mvn -f java-agent/pom.xml package
```

The shaded executable jar is generated under `java-agent/target/`.
If Maven cannot write to the default user repository in a sandbox, use a workspace-local repository:

```bash
mvn -Dmaven.repo.local=.m2/repository -f java-agent/pom.xml package
```

The test suite includes unit coverage for config, task handling, GeoIP fallback, GPU parsing, and
IOStream/file-manager wire formats, plus local loopback checks for HTTP tasks and the gRPC
dashboard lifecycle. Loopback checks are skipped automatically when the current environment
cannot create local servers.

## Run

```bash
java -jar java-agent/target/nezha-java-agent-0.1.0-SNAPSHOT.jar --config java-agent/config.yml
```

Start from `config.example.yml`, then set `server` and `client_secret`. When `--config` is not
provided, the executable jar looks for `config.yml` next to the jar, matching the official agent's
default layout.
If `uuid` is empty, the agent generates and saves one on first start.

## Embed

All handwritten client code is consolidated in `com.nezhahq.agent.NezhaJavaAgent` for easy embedding.
The protobuf/gRPC classes are still generated from `src/main/proto/nezha.proto`; when integrating
into another Java project, keep that proto generation step or copy the generated `com.nezhahq.agent.proto`
classes alongside the single handwritten file.

```java
import com.nezhahq.agent.NezhaJavaAgent;

NezhaJavaAgent.RunningAgent agent = NezhaJavaAgent.start(Path.of("config.yml"));
```

Call `agent.close()` or `agent.stop()` during your application's shutdown path. If you only need
the worker future, use `startAsync(Path)`:

```java
CompletableFuture<Void> worker = NezhaJavaAgent.startAsync(Path.of("config.yml"));
```

Keep a client reference when your application needs lower-level lifecycle control:

```java
import com.nezhahq.agent.NezhaJavaAgent;

NezhaJavaAgent.NezhaAgentClient client = NezhaJavaAgent.createClient(Path.of("config.yml"));
CompletableFuture<Void> worker = client.startAsync();
```

Use `client.stop()` to shut down that lower-level client. Use `runForever(Path)` instead when your
process is dedicated to the agent and should block in the foreground.
Remote config changes are saved locally and trigger a reconnect after a short delay so report
timers and gRPC metadata pick up the new settings.
