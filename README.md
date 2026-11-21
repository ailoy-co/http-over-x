# http-over-x

One HTTP client for a server that is in the same process, a child process, or at a URL.

You write one [axum](https://docs.rs/axum) router and one set of client calls. Where the server runs changes one line at each end:

| Where the server is | Server | Client |
| --- | --- | --- |
| [In the same process](#in-the-same-process) | nothing to run | `Client::embed(router)` |
| [A child process](#a-child-process) | `serve_stdio(router, stdin, stdout)` | `Client::spawn(command)` |
| [At a URL](#at-a-url) | `serve_tcp(router, listener)` | `Client::connect(url, auth, headers)` |

The chapters below all use this router and this request:

```rust
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};

async fn start(Json(args): Json<Value>) -> Json<Value> {
    Json(json!({"id": "j1", "cmd": args["cmd"]}))
}

fn router() -> Router {
    Router::new().route("/jobs", post(start))
}
```

```rust
let job: Value = client.post("/jobs", &json!({"cmd": ["ls"]})).await?;
```

## In the same process

There is no server to run. The client serves the router itself, over an in-memory pipe. This is how a test reaches its server.

```rust
use http_over_x::Client;

let client = Client::embed(router());
```

`embed` needs the `server` feature. No credentials are sent.

## A child process

The server is its own binary, and it answers over its stdin and stdout:

```rust
use http_over_x::serve_stdio;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    serve_stdio(router(), tokio::io::stdin(), tokio::io::stdout()).await
}
```

The client starts that binary and talks to it:

```rust
use http_over_x::Client;
use std::process::Command;

let client = Client::spawn(Command::new("my-server"))?;
```

The server's stdout is the connection, so its logs go to stderr. No credentials are sent, since the client started the server. Dropping the client kills the server.

## At a URL

The server listens on a socket:

```rust
use http_over_x::serve_tcp;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    serve_tcp(router(), listener).await // or axum::serve(listener, router()).await
}
```

The client connects to its base URL, with the credentials and headers to send on every request:

```rust
use http_over_x::{Auth, Client, http::HeaderMap};

let client = Client::connect("http://127.0.0.1:8080", Auth::None, HeaderMap::new())?;
```

An `Auth` is one of:

- `Auth::bearer(token)`: a bearer token.
- `Auth::command(program, args)`: a bearer token that a command prints. The command runs again when the token runs out or the server refuses it.
- `Auth::basic(username, password)`: `Authorization: Basic`.
- `Auth::mtls(cert, key)`: a client certificate, in mutual TLS.

`serve_tcp` serves in the clear, and the client refuses to send a token or a password in the clear to another machine. For clients elsewhere, `serve_tls` (the `tls` feature) serves the same router over TLS, with the server's certificate and key in PEM files:

```rust
use http_over_x::{Tls, serve_tls};

// Any client:
serve_tls(router(), listener, &Tls::new("server.pem", "server.key")).await
// Only clients showing a certificate this authority signed, in mutual TLS:
serve_tls(router(), listener, &Tls::new("server.pem", "server.key").client_ca("clients-ca.pem")).await
```

Neither `serve_tcp` nor `serve_tls` checks a token or a password. That is the router's job, as a layer.

## Requests

A request is a path under the server's base, with JSON in and JSON out. Any type that `serde` reads or writes works as a body. These calls are the same over all three:

```rust
let job: Value = client.post("/jobs", &json!({"cmd": ["ls"]})).await?;
let job: Value = client.get("/jobs/j1").await?;
client.delete("/jobs/j1").await?;
```

- **Retries.** A `429` or a `503` is sent again after its `Retry-After`, for thirty seconds at most, and only where a second sending is safe.
- **Refusals.** A status outside `2xx` is an `Error::Status` with the server's `code` and `message`.

On the server, extractors, state, layers and middleware work as they do in any axum app. `from_body` reads a JSON body as a handler's args, and its error names the member that was wrong in plain words, such as `cmd: a list, not "ls"`.

axum is not required. `embed`, `serve_stdio` and `serve_tcp` take any `tower::Service` from a request to a response, and a plain function serves through `tower::service_fn`. The requests, responses and headers are the `http` crate's, re-exported as `http_over_x::http`.

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
