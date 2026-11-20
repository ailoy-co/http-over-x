# http-over-x

One HTTP client for a server that is a child process or a URL.

A program that talks to a server often needs it in two places: on the same machine, started when the program starts, and somewhere else, reached over the network. `http-over-x` gives both one client. The server speaks HTTP either way: over its own stdin and stdout when it is spawned, over TCP when it is at a URL. Only the line that opens the client differs.

```rust
let client = Client::over  // a stream already open
let client = Client::spawn(Command::new("my-server"))?;  // a child process
let client = Client::connect("https://example.com/api", auth, headers)?;  // a URL
```

Both ends are here: the client, and what a server needs to answer it over either.

## The client

A request is a path under the server's base, with JSON in and JSON out. Any type that `serde` reads or writes works as a body.

```rust
use http_over_x::Client;
use serde_json::{Value, json};

let client = Client::spawn(std::process::Command::new("my-server"))?;

let job: Value = client.post("/jobs", &json!({"cmd": ["ls"]})).await?;
let job: Value = client.get("/jobs/j1").await?;
client.delete("/jobs/j1").await?;
```

What the client does for every request:

- **Retries.** A `429` or a `503` is sent again after its `Retry-After`, for thirty seconds at most, and only where a second sending is safe.
- **Credentials.** Over TCP it sends what an `Auth` says: a bearer token, given or printed by a command that is run again when the token runs out or the server refuses it; a username and password, as `Authorization: Basic`; or a client certificate, in mutual TLS. A token or a password is never sent in the clear to another machine.
- **Refusals.** A status outside `2xx` is an `Error::Status` with the server's `code` and `message`.

`Client::over` opens a client on a stream the program already holds, with no process started and no credentials sent, as over stdio. A test reaches a server in its own process this way, over an in-memory pipe that `serve_stdio` answers at the other end:

```rust
let (pipe, server) = tokio::io::duplex(64 * 1024);
let (input, output) = tokio::io::split(server);
tokio::spawn(serve_stdio(router, input, output));
let client = Client::over(pipe);
```

## The server

The server is an ordinary [axum](https://docs.rs/axum) app. Write the router as usual, then hand it to `serve_stdio` in place of `axum::serve`:

```rust
use axum::{Json, Router, routing::post};
use http_over_x::serve_stdio;
use serde_json::{Value, json};

async fn start(Json(args): Json<Value>) -> Json<Value> {
    Json(json!({"id": "j1", "cmd": args["cmd"]}))
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let router = Router::new().route("/jobs", post(start));
    serve_stdio(router, tokio::io::stdin(), tokio::io::stdout()).await
}
```

The same router serves TCP, so one binary can do both:

```rust
let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
serve_tcp(router, listener).await   // or axum::serve(listener, router).await
```

Extractors, state, layers and middleware work as they do in any axum app. Over stdio the server's stdout is the connection, so logs go to stderr.

axum is not required: `serve_stdio` and `serve_tcp` take any `tower::Service` from a request to a response, and a plain function serves through `tower::service_fn`. The requests, responses and headers are the `http` crate's, re-exported as `http_over_x::http`.

`from_body` reads a JSON body as a handler's args, and its error names the member that was wrong in plain words, such as `cmd: a list, not "ls"`.

Over TCP, `serve_tcp`'s connections are in the clear, which a client sends its token or its password over only to a server on its own machine. For clients elsewhere, `serve_tls` (the `tls` feature) serves the same service over TLS, with the server's certificate and its key in PEM files:

```rust
use http_over_x::{Tls, serve_tls};

// Any client:
serve_tls(router, listener, &Tls::new("server.pem", "server.key")).await
// Only those showing a certificate this authority signed, in mutual TLS:
serve_tls(router, listener, &Tls::new("server.pem", "server.key").client_ca("clients-ca.pem")).await
```

Neither checks a token or a password: that is the service's, as a layer of its router.

## Streaming (SSE)

A server streams to a client over server-sent events. What is streamed is a log: numbered entries, such as a long-running job's output, which a client can read while the job runs.

The server keeps each job's entries in an `sse::Log` and answers `GET {job}/events` with its stream:

```rust
use std::sync::Arc;

use axum::{body::Body, extract::State, http::{HeaderMap, header::CONTENT_TYPE}, response::IntoResponse};
use http_over_x::sse;
use serde_json::{Value, json};

async fn events(State(log): State<Arc<sse::Log<Value>>>, headers: HeaderMap) -> impl IntoResponse {
    let stream = Body::from_stream(log.events(&headers));
    ([(CONTENT_TYPE, "text/event-stream")], stream)
}

// Keeps 1000 entries, then one `truncated` in place of the rest.
let log = sse::Log::new(1000, json!({"truncated": {}}));
let router = Router::new().route("/jobs/j1/events", get(events)).with_state(log.clone());

// As the job runs:
log.push(json!({"stdout": "hello"}));
log.end(json!({"end": {"code": 0}}), || ());
```

The client reads the entries as they come, until the one with an `end` member:

```rust
use futures_util::StreamExt;

let mut log = std::pin::pin!(sse::log::<Value>(&client, "/jobs/j1/events".into(), None));
while let Some(entry) = log.next().await {
    let entry = entry?;
    println!("{}: {}", entry.id, entry.entry);
}
```

Entries are any type `serde` reads and writes, an enum of your own in place of `Value`. Over TCP, a log whose connection breaks is read on from after the last entry the client got. The third argument starts a read after a given entry.

## The protocol

The two ends share a few conventions, and a server on this crate follows them:

| | |
| --- | --- |
| Bodies | JSON, in and out. |
| Refusals | The status, with `{"error": {"code": "...", "message": "..."}}`. Other members of `error` reach the client as they are. |
| Refused for now | `429` and `503`, with `Retry-After` in seconds. A `429` whose `code` is `quota` with a `reason` other than `requests` is not sent again. A `503` whose `code` is `capacity` says nothing of the request was done. |
| Authentication | Over TCP, `Authorization: Bearer <token>`, `Authorization: Basic <credentials>`, or the client's certificate in mutual TLS. Over stdio, none: the client started the server. |
| Logs | A `text/event-stream` at `{job}/events`. Each entry has an `id`, counted from `1`, and the last has an `end` member. A reader sends `Last-Event-ID` to read on. `GET {job}` answers whether the job is `running`. |

## Status

The crate is new, and its API may change.

## License

Apache-2.0. See [LICENSE](LICENSE).
