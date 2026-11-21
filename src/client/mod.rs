//! The client's end: the server's endpoints over either transport, which only opening and
//! closing tell apart. It knows no endpoint's protocol; each is built on [`Client::send`].

mod credentials;
mod stdio;
mod tcp;

use std::io;
use std::sync::{PoisonError, RwLock};
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Serialize, de::DeserializeOwned};

pub use self::credentials::TOKEN_REFUSED;
use self::{stdio::Stdio, tcp::Tcp};
use crate::{Auth, Error, error::refused};

/// A request's or a response's body, as either transport carries it.
pub type Body = BoxBody<Bytes, io::Error>;

/// `bytes`, as a whole [`Body`].
pub fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).map_err(|e| match e {}).boxed()
}

/// The most a request's body may be to be kept, to send it again ([`Client::send`]).
const REPLAYABLE: u64 = 1 << 20;

/// The longest a request is held back from the server, all told -- its answers' time and the
/// waits between them -- while it turns it away for now.
const PATIENCE: Duration = Duration::from_secs(30);

/// The first wait before a request turned away without a `Retry-After` is sent again, each
/// after twice the one before; and the least wait, whatever `Retry-After` asks.
const FIRST_WAIT: Duration = Duration::from_millis(500);

/// The wait `response` asks for, in seconds (`Retry-After`; never a date, from these servers).
fn retry_after(response: &http::Response<Body>) -> Option<Duration> {
    let seconds = response.headers().get(http::header::RETRY_AFTER)?;
    Some(Duration::from_secs(
        seconds.to_str().ok()?.trim().parse().ok()?,
    ))
}

/// What a `429` or a `503` says of the request it answers, as the protocol's Refused for now has
/// it.
struct ForNow {
    /// Whether to send the request again after a wait: a `429` unless it is a `quota` whose
    /// `reason` is other than `requests`, which frees up only as the caller closes or deletes
    /// something; a `503` where it is `capacity`, or where the request is `repeatable`.
    again: bool,
    /// Whether nothing of the request was done: a `429`, or a `503` that is `capacity`. Any
    /// other `503` may have come after it was carried out.
    undone: bool,
}

/// `response`, a `429` or a `503`, and what it says of a request that is `repeatable` or not
/// ([`Client::send`]). Its body read, and given back whole.
async fn for_now(
    response: http::Response<Body>,
    repeatable: bool,
) -> (http::Response<Body>, ForNow) {
    let status = response.status();
    let (parts, body) = response.into_parts();
    let bytes = body
        .collect()
        .await
        .map(|b| b.to_bytes())
        .unwrap_or_default();
    let said: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    let error = &said["error"];
    let said = match status {
        StatusCode::TOO_MANY_REQUESTS => ForNow {
            again: error["code"] != "quota" || error["reason"] == "requests",
            undone: true,
        },
        _ => {
            let undone = error["code"] == "capacity";
            ForNow {
                again: undone || repeatable,
                undone,
            }
        }
    };
    (http::Response::from_parts(parts, full(bytes)), said)
}

/// How a client reaches the server's endpoints.
enum Connection {
    Tcp(Tcp),
    Stdio(Stdio),
}

impl Connection {
    /// Send `request`, whose URI is a path under the server's base. An `Err` says why no
    /// response came.
    async fn send(&self, request: http::Request<Body>) -> Result<http::Response<Body>, Error> {
        match self {
            Connection::Tcp(tcp) => tcp.send(request).await,
            Connection::Stdio(stdio) => stdio.send(request).await,
        }
    }
}

/// A client of one server, over stdio or HTTP: the connection every endpoint's requests go by,
/// and all they share.
pub struct Client {
    connection: Connection,
    /// What every request carries over either transport, each in place of a header of its
    /// name among those given ([`set_header`](Self::set_header)).
    own: RwLock<HeaderMap>,
}

impl Client {
    fn new(connection: Connection) -> Self {
        Client {
            connection,
            own: RwLock::new(HeaderMap::new()),
        }
    }

    /// Start `command`, a [`std::process::Command`] or a tokio one, as the server, and talk to
    /// it over its stdin and stdout: whatever `command` set for those two is replaced, and it
    /// runs in a process group of its own, out of reach of the terminal's Ctrl-C. The rest of it
    /// (arguments, environment, directory, stderr) is the caller's. Must be called inside a
    /// tokio runtime.
    pub fn spawn(command: impl Into<tokio::process::Command>) -> io::Result<Self> {
        Stdio::spawn(command).map(|stdio| Self::new(Connection::Stdio(stdio)))
    }

    /// Reach the server at `url`, its base `http` or `https` URL, sending `auth`, the
    /// credentials it wants, and `headers`, whatever else it asks of each request, with every
    /// request. `Authorization` is `auth`'s, over one given as a header, where `auth` is a
    /// token or a password; a certificate ([`Auth::Mtls`]) leaves the header to `headers`.
    /// Nothing is sent until the first request. Refused when a token or a password would go in
    /// the clear, over `http` to any host but this machine; and when a certificate would not be
    /// shown, over `http` at all, or cannot be read.
    pub fn connect(url: &str, auth: Auth, headers: HeaderMap) -> Result<Self, Error> {
        Tcp::connect(url, auth, headers).map(|tcp| Self::new(Connection::Tcp(tcp)))
    }

    /// Serve `service` in this process and talk to it over an in-memory pipe, one HTTP/2
    /// connection as over stdio: what [`serve_stdio`](crate::serve_stdio) is given, where
    /// [`spawn`](Self::spawn) would start a server of it as a child. No credentials are sent,
    /// as over stdio, and the service is dropped once the client is. Must be called inside a
    /// tokio runtime.
    #[cfg(feature = "server")]
    pub fn embed<S, B>(service: S) -> Self
    where
        S: tower_service::Service<
                http::Request<hyper::body::Incoming>,
                Response = http::Response<B>,
            > + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let (pipe, server) = tokio::io::duplex(64 * 1024);
        let (input, output) = tokio::io::split(server);
        tokio::spawn(crate::serve_stdio(service, input, output));
        Self::new(Connection::Stdio(Stdio::over(pipe, None)))
    }

    /// Send `auth` with every request from now on, in place of what was given at
    /// [`connect`](Self::connect): a new token, when the server's are short-lived. A forwarder
    /// already open sends it with its next connection. Over stdio, where none is sent, nothing
    /// changes. Refused as at `connect`, the auth unchanged.
    pub fn set_auth(&self, auth: Auth) -> Result<(), Error> {
        match &self.connection {
            Connection::Tcp(tcp) => tcp.set_auth(auth),
            Connection::Stdio(_) => Ok(()),
        }
    }

    /// Send `headers` with every request from now on, in place of those given at
    /// [`connect`](Self::connect). A forwarder already open sends them with its next
    /// connection. Over stdio, where none are sent, nothing changes: a header that goes over
    /// either transport is [`set_header`](Self::set_header)'s.
    pub fn set_headers(&self, headers: HeaderMap) {
        if let Connection::Tcp(tcp) = &self.connection {
            tcp.set_headers(headers);
        }
    }

    /// Send the header `name` as `value` with every request from now on, over either
    /// transport, in place of one of that name among the headers given; with `None`, no
    /// longer, and the one among those headers goes again where there is one.
    pub fn set_header(&self, name: HeaderName, value: Option<HeaderValue>) {
        let mut own = self.own.write().unwrap_or_else(PoisonError::into_inner);
        match value {
            Some(value) => own.insert(name, value),
            None => own.remove(name),
        };
    }

    /// Send `request` to whichever endpoint its URI, a path under the server's base, names. An
    /// `Err` says why no response came, or that the server refused the headers with a `401` or
    /// a `403`, which every endpoint's are alike. A path, or a
    /// `Destination`, with a `.` or `..` segment still in it, a file's path that goes above its
    /// root or a name that is one, is refused unsent: over HTTP it would be resolved before it
    /// went, to another endpoint's path even, and the server refuses one that arrives.
    ///
    /// A request whose body is known and small (a megabyte) -- every resource's, and a
    /// small file's -- is sent again when the server turns it away for now: a `429` or a `503`,
    /// after its `Retry-After` or else a wait that doubles, for thirty seconds at most, its
    /// answers' time counted with the waits; and a `401`, at once and once, when the token is a
    /// command's, which prints a new one. A refusal that waiting does not change is answered at
    /// once, and one that may have come after the request was carried out is sent again only
    /// where a second sending leaves what the first did: a `GET`, `HEAD`, `OPTIONS`, `PUT`,
    /// `DELETE` or `PROPFIND` with no `If-Match` or `If-None-Match`. Such a
    /// `DELETE` that then finds nothing, `404`, is answered `204`: what it names is gone.
    pub async fn send(&self, request: http::Request<Body>) -> Result<http::Response<Body>, Error> {
        if let Some(path) = dot_segment(&request) {
            return Err(Error::Client(format!(
                "{path:?}: a path cannot go above its root, nor a name be \".\" or \"..\""
            )));
        }
        let (parts, body) = request.into_parts();
        let small = hyper::body::Body::size_hint(&body)
            .exact()
            .is_some_and(|size| size <= REPLAYABLE);
        if !small {
            return self.send_once(http::Request::from_parts(parts, body)).await;
        }
        let bytes = body
            .collect()
            .await
            .map_err(|e| Error::Transport(e.to_string()))?
            .to_bytes();
        let started = tokio::time::Instant::now();
        let conditional = [http::header::IF_MATCH, http::header::IF_NONE_MATCH]
            .iter()
            .any(|condition| parts.headers.contains_key(condition));
        let repeatable = !conditional
            && matches!(
                parts.method.as_str(),
                "GET" | "HEAD" | "OPTIONS" | "PUT" | "DELETE" | "PROPFIND"
            );
        let mut backoff = FIRST_WAIT;
        let mut renewed = false;
        // Whether a sending before may have been carried out.
        let mut maybe_done = false;
        loop {
            let mut request = http::Request::new(full(bytes.clone()));
            *request.method_mut() = parts.method.clone();
            *request.uri_mut() = parts.uri.clone();
            *request.version_mut() = parts.version;
            *request.headers_mut() = parts.headers.clone();
            let response = match self.send_once(request).await {
                Err(refused) if refused.status() == Some(401) && !renewed && self.renews() => {
                    renewed = true;
                    continue;
                }
                answered => answered?,
            };
            let status = response.status();
            if status == StatusCode::NOT_FOUND && maybe_done && parts.method == http::Method::DELETE
            {
                return Ok(http::Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .body(full(Bytes::new()))
                    .expect("a valid response"));
            }
            if status != StatusCode::TOO_MANY_REQUESTS && status != StatusCode::SERVICE_UNAVAILABLE
            {
                return Ok(response);
            }
            let (response, said) = for_now(response, repeatable).await;
            let wait = retry_after(&response)
                .unwrap_or(backoff)
                .clamp(FIRST_WAIT, PATIENCE);
            if !said.again || started.elapsed() + wait > PATIENCE {
                return Ok(response);
            }
            maybe_done |= !said.undone;
            tokio::time::sleep(wait).await;
            backoff *= 2;
        }
    }

    async fn send_once(
        &self,
        mut request: http::Request<Body>,
    ) -> Result<http::Response<Body>, Error> {
        let own = self
            .own
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        for (name, value) in &own {
            request.headers_mut().insert(name, value.clone());
        }
        let response = self.connection.send(request).await?;
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(refused(response).await);
        }
        Ok(response)
    }

    /// Whether a connection that breaks is made anew by the next request: over TCP. Over stdio,
    /// the one connection is the server's process, and gone with it.
    pub fn reconnects(&self) -> bool {
        matches!(self.connection, Connection::Tcp(_))
    }

    /// Whether a token refused is printed anew: a [`Token::Command`](crate::Token::Command)'s.
    fn renews(&self) -> bool {
        match &self.connection {
            Connection::Tcp(tcp) => tcp.credentials.renews(),
            Connection::Stdio(_) => false,
        }
    }

    /// End the session. Over stdio, close the server's stdin and kill it if it has not exited
    /// within five seconds; over HTTP there is nothing to end.
    pub async fn close(self) {
        if let Connection::Stdio(stdio) = self.connection {
            stdio.close().await;
        }
    }
}

/// `bytes` read as a `T`.
fn read<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(bytes).map_err(|e| Error::Client(e.to_string()))
}

/// The body of `response`, a success, read as a `T`.
pub async fn json<T: DeserializeOwned>(response: http::Response<Body>) -> Result<T, Error> {
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|e| Error::Transport(format!("reading the response: {e}")))?;
    read(&body.to_bytes())
}

impl Client {
    /// Send `method` to `path` with `body` as JSON, if any, and answer the body of a success. A
    /// status outside `2xx` is an [`Error::Status`].
    async fn send_json(
        &self,
        method: Method,
        path: &str,
        body: Option<&impl Serialize>,
    ) -> Result<Bytes, Error> {
        let mut request = http::Request::builder().method(method).uri(path);
        let body = match body {
            Some(body) => {
                request = request.header(header::CONTENT_TYPE, "application/json");
                serde_json::to_vec(body).map_err(|e| Error::Client(e.to_string()))?
            }
            None => Vec::new(),
        };
        let request = request
            .header(header::ACCEPT, "application/json")
            .body(full(body))
            .map_err(|e| Error::Client(format!("not a request: {e}")))?;
        let response = self.send(request).await?;
        if !response.status().is_success() {
            return Err(refused(response).await);
        }
        response
            .into_body()
            .collect()
            .await
            .map(|body| body.to_bytes())
            .map_err(|e| Error::Transport(format!("reading the response: {e}")))
    }

    /// What `GET path` answers.
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, Error> {
        read(&self.send_json(Method::GET, path, None::<&()>).await?)
    }

    /// What `POST path` with `body` answers.
    pub async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &impl Serialize,
    ) -> Result<T, Error> {
        read(&self.send_json(Method::POST, path, Some(body)).await?)
    }

    /// `POST path` with `body`, answered with nothing.
    pub async fn post_empty(&self, path: &str, body: &impl Serialize) -> Result<(), Error> {
        self.send_json(Method::POST, path, Some(body))
            .await
            .map(drop)
    }

    /// What `PUT path` with `body` answers.
    pub async fn put<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &impl Serialize,
    ) -> Result<T, Error> {
        read(&self.send_json(Method::PUT, path, Some(body)).await?)
    }

    /// What `PATCH path` with `body` answers.
    pub async fn patch<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &impl Serialize,
    ) -> Result<T, Error> {
        read(&self.send_json(Method::PATCH, path, Some(body)).await?)
    }

    /// `DELETE path`, answered with nothing.
    pub async fn delete(&self, path: &str) -> Result<(), Error> {
        self.send_json(Method::DELETE, path, None::<&()>)
            .await
            .map(drop)
    }
}

/// What a path segment keeps as it is: the unreserved characters of RFC 3986.
const SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// `segment`, percent-encoded as one path segment.
pub fn segment(segment: &str) -> String {
    utf8_percent_encode(segment, SEGMENT).to_string()
}

/// `path`, `/`-separated, each segment percent-encoded, [`resolved`] first.
pub fn segments(path: &str) -> String {
    resolved(path)
        .into_iter()
        .map(segment)
        .collect::<Vec<_>>()
        .join("/")
}

/// The segments of `path`, `/`-separated, as a file's path is read: empty and `.` segments
/// dropped, and each `..` taking the segment before it. A `..` above where `path` starts is
/// kept, for [`Client::send`] to refuse unsent.
pub fn resolved(path: &str) -> Vec<&str> {
    let mut kept: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." if kept.last().is_some_and(|s| *s != "..") => {
                kept.pop();
            }
            segment => kept.push(segment),
        }
    }
    kept
}

/// The path of `request`, or its `Destination`, if either has a `.` or `..` segment, plain or
/// percent-encoded, with `\` separating segments too, as an `http` URL reads one.
fn dot_segment(request: &http::Request<Body>) -> Option<&str> {
    let is_dot = |segment: &str| {
        matches!(
            segment.to_ascii_lowercase().replace("%2e", ".").as_str(),
            "." | ".."
        )
    };
    let destination = request
        .headers()
        .get("Destination")
        .and_then(|to| to.to_str().ok());
    [Some(request.uri().path()), destination]
        .into_iter()
        .flatten()
        .find(|path| path.split(['/', '\\']).any(is_dot))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::{Json, Router, routing::get};
    use http::header::AUTHORIZATION;

    use super::*;

    #[test]
    fn segments_percent_encoded() {
        assert_eq!(segment("my vol"), "my%20vol");
        assert_eq!(segment("a/b"), "a%2Fb");
        assert_eq!(segments("/dir//a b.txt/"), "dir/a%20b.txt");
        assert_eq!(segments("한글/#?"), "%ED%95%9C%EA%B8%80/%23%3F");
        assert_eq!(segments(""), "");
    }

    #[test]
    fn segments_dot_resolved() {
        assert_eq!(segments("."), "");
        assert_eq!(segments("./a/./b/."), "a/b");
        assert_eq!(segments("a/.."), "");
        assert_eq!(segments("a/b/../c"), "a/c");
        assert_eq!(segments("a/../../b"), "../b");
        assert_eq!(segments("../../a"), "../../a");
        // A dot inside a segment is part of its name.
        assert_eq!(segments("a.b/..c/.d/..."), "a.b/..c/.d/...");
        // What a URL would read as a dot segment goes as a name: `%`, `\` and a newline encoded.
        assert_eq!(segments("%2e/%2E%2e"), "%252e/%252E%252e");
        assert_eq!(segments("..\\x/.\\"), "..%5Cx/.%5C");
        assert_eq!(segments(".\n./.\t"), ".%0A./.%09");
    }

    /// The bearer token and the headers given at `connect` go with every request, until
    /// `set_auth`, `set_headers` or `set_header` replaces them.
    #[tokio::test]
    async fn auth_and_headers() {
        // Answers whom the bearer token names, `alice` or `bob`: 401 without one, 403 for
        // anyone else.
        let who = |headers: HeaderMap| async move {
            let token = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok());
            match token.and_then(|t| t.strip_prefix("Bearer ")) {
                None => Err(StatusCode::UNAUTHORIZED),
                Some(user @ ("alice" | "bob")) => Ok(Json(user.to_owned())),
                Some(_) => Err(StatusCode::FORBIDDEN),
            }
        };
        let router = Router::new().route("/user", get(who));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await });

        let client = Client::connect(&url, Auth::bearer("alice"), HeaderMap::new()).unwrap();
        for _ in 0..2 {
            assert_eq!(client.get::<String>("/user").await.unwrap(), "alice");
        }
        client.set_auth(Auth::bearer("bob")).unwrap();
        assert_eq!(client.get::<String>("/user").await.unwrap(), "bob");
        // The old one is gone with the new, not kept under it.
        client.set_auth(Auth::None).unwrap();
        let error = client.get::<String>("/user").await.unwrap_err();
        assert_eq!(error.status(), Some(401), "{error:?}");
        client.set_auth(Auth::bearer("carol")).unwrap();
        let error = client.get::<String>("/user").await.unwrap_err();
        assert_eq!(error.status(), Some(403), "{error:?}");

        // The token in a header of the caller's own, as to a server that reads it from one.
        let alice = HeaderMap::from_iter([(AUTHORIZATION, "Bearer alice".parse().unwrap())]);
        let bob = HeaderMap::from_iter([(AUTHORIZATION, "Bearer bob".parse().unwrap())]);
        let client = Client::connect(&url, Auth::None, alice).unwrap();
        assert_eq!(client.get::<String>("/user").await.unwrap(), "alice");
        // `set_header` overrides the one given, until it is unset.
        client.set_header(AUTHORIZATION, Some("Bearer bob".parse().unwrap()));
        assert_eq!(client.get::<String>("/user").await.unwrap(), "bob");
        client.set_header(AUTHORIZATION, None);
        assert_eq!(client.get::<String>("/user").await.unwrap(), "alice");
        client.set_headers(bob.clone());
        assert_eq!(client.get::<String>("/user").await.unwrap(), "bob");
        client.set_headers(HeaderMap::new());
        let error = client.get::<String>("/user").await.unwrap_err();
        assert_eq!(error.status(), Some(401), "{error:?}");

        // The auth's `Authorization` wins over one given as a header.
        let client = Client::connect(&url, Auth::bearer("alice"), bob).unwrap();
        assert_eq!(client.get::<String>("/user").await.unwrap(), "alice");
    }

    #[tokio::test]
    async fn basic_auth() {
        let who = |headers: HeaderMap| async move {
            // `alice:secret`, in base64.
            match headers.get(AUTHORIZATION).map(HeaderValue::as_bytes) {
                Some(b"Basic YWxpY2U6c2VjcmV0") => Ok(Json("alice".to_owned())),
                _ => Err(StatusCode::UNAUTHORIZED),
            }
        };
        let router = Router::new().route("/user", get(who));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await });
        let client =
            Client::connect(&url, Auth::basic("alice", "secret"), HeaderMap::new()).unwrap();
        assert_eq!(client.get::<String>("/user").await.unwrap(), "alice");

        client.set_auth(Auth::basic("alice", "guess")).unwrap();
        let error = client.get::<String>("/user").await.unwrap_err();
        assert!(matches!(error, Error::Status(401, _, _)), "{error:?}");
    }

    #[test]
    fn no_credentials_over_plain_http() {
        for auth in [Auth::bearer("t"), Auth::basic("alice", "secret")] {
            let with = |url| Client::connect(url, auth.clone(), HeaderMap::new());
            assert!(with("http://example.com/").is_err());
            for url in [
                "https://example.com/",
                "http://127.0.0.1:9/",
                "http://localhost:9/",
                "http://[::1]:9/",
            ] {
                assert!(with(url).is_ok(), "{url}");
            }
            let client = Client::connect("http://example.com/", Auth::None, HeaderMap::new());
            assert!(client.unwrap().set_auth(auth).is_err());
        }
    }

    #[test]
    fn connect_rejects_non_http_url() {
        for url in ["not a url", "file:///tmp/x"] {
            assert!(
                Client::connect(url, Auth::None, HeaderMap::new()).is_err(),
                "{url}"
            );
        }
    }

    #[tokio::test]
    async fn base_url_trailing_slash() {
        let router = Router::new().route("/base/ping", get(|| async { Json("pong") }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await });
        for base in [format!("{url}base"), format!("{url}base/")] {
            let client = Client::connect(&base, Auth::None, HeaderMap::new()).unwrap();
            assert_eq!(client.get::<String>("/ping").await.unwrap(), "pong");
        }
    }

    #[tokio::test]
    async fn no_server_is_transport_error() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        drop(listener);
        let client = Client::connect(&url, Auth::None, HeaderMap::new()).unwrap();
        for _ in 0..2 {
            let error = client.get::<String>("/ping").await.unwrap_err();
            assert!(matches!(error, Error::Transport(_)), "{error:?}");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn spawned_server_exits() {
        let client = Client::spawn(tokio::process::Command::new("true")).unwrap();
        for _ in 0..2 {
            assert!(client.get::<String>("/ping").await.is_err());
        }
        client.close().await;
    }

    /// The time a request is held back counts the server's answers as well as the waits
    /// between them: a server that holds each refusal 12 s is asked three times in 30 s, not
    /// once a second for 30 s of waits. On a clock the test moves, and so over a pipe, whose
    /// answers wait on nothing else.
    #[tokio::test(start_paused = true)]
    async fn retry_budget_counts_response_time() {
        let asked = Arc::new(AtomicUsize::new(0));
        let held = asked.clone();
        let router = Router::new().route(
            "/slow",
            get(async move || {
                held.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(12)).await;
                let error = serde_json::json!({"code": "capacity", "message": "no room yet"});
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    [(header::RETRY_AFTER, "1")],
                    Json(serde_json::json!({ "error": error })),
                )
            }),
        );
        let error = Client::embed(router)
            .get::<String>("/slow")
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Status(503, _, _)), "{error:?}");
        assert_eq!(asked.load(Ordering::SeqCst), 3);
    }

    /// The server's end of a pipe is done, and without an error, once the client closes its
    /// own: here, where the pipe is the test's to open.
    #[tokio::test]
    async fn serve_stdio_returns_on_close() {
        let router = Router::new().route("/ping", get(|| async { Json("pong") }));
        let (pipe, server) = tokio::io::duplex(64 * 1024);
        let (input, output) = tokio::io::split(server);
        let served = tokio::spawn(crate::serve_stdio(router, input, output));
        let client = Client::new(Connection::Stdio(Stdio::over(pipe, None)));
        assert_eq!(client.get::<String>("/ping").await.unwrap(), "pong");
        client.close().await;
        served.await.unwrap().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_token_renewed_on_401() {
        let requests = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&requests);
        let router = Router::new().fallback(move |headers: HeaderMap| {
            counted.fetch_add(1, Ordering::Relaxed);
            let token = headers.get(http::header::AUTHORIZATION).cloned();
            async move {
                match token.is_some_and(|token| token == "Bearer new") {
                    true => StatusCode::NO_CONTENT,
                    false => StatusCode::UNAUTHORIZED,
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await });

        let script = r#"if [ -n "$TOKEN_REFUSED" ]; then echo new; else echo old; fi"#;
        let auth = Auth::command("sh", ["-c", script]);
        let client = Client::connect(&url, auth, HeaderMap::new()).unwrap();
        let request = || http::Request::get("/volumes/").body(full("")).unwrap();
        assert_eq!(client.send(request()).await.unwrap().status(), 204);
        assert_eq!(requests.load(Ordering::Relaxed), 2);
        assert_eq!(client.send(request()).await.unwrap().status(), 204);
        assert_eq!(requests.load(Ordering::Relaxed), 3);

        // A token given is the caller's to replace: refused, the request is not sent again.
        let given = Auth::bearer("old");
        let client = Client::connect(&url, given, HeaderMap::new()).unwrap();
        assert_eq!(
            client.send(request()).await.unwrap_err().status(),
            Some(401)
        );
        assert_eq!(requests.load(Ordering::Relaxed), 4);
    }
}
