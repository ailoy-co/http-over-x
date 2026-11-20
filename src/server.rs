//! The server's end of either transport: one HTTP/2 connection over its own stdin and stdout,
//! or every connection a TCP listener takes, in the clear or over TLS.

#[cfg(feature = "tls")]
use std::path::{Path, PathBuf};
use std::{future::Future, io, time::Duration};

use bytes::Bytes;
use http::{Request, Response};
use http_body::Body;
use hyper::body::Incoming;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
    service::TowerToHyperService,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tower_service::Service;

/// Serve one HTTP/2 connection over `input` and `output` (a server's stdin and stdout), each
/// request answered by `service`, until the client closes it or `input` ends. What is in flight
/// then is cancelled, not waited for.
///
/// Every endpoint is `service`'s, as a server over HTTP routes its own: the router a TCP
/// listener is served with is served here as it is, and a function from a request to a
/// response is one through `tower::service_fn`.
pub async fn serve_stdio<S, B, I, O>(service: S, input: I, output: O) -> io::Result<()>
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    I: AsyncRead + Send + Unpin + 'static,
    O: AsyncWrite + Send + Unpin + 'static,
{
    let io = TokioIo::new(tokio::io::join(input, output));
    let served = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
        .serve_connection(io, TowerToHyperService::new(service))
        .await;
    match served {
        Ok(()) => Ok(()),
        // The client went without saying goodbye, as it may.
        Err(e) if e.is_incomplete_message() || is_eof(&e) => Ok(()),
        Err(e) => Err(io::Error::other(e)),
    }
}

/// Serve every connection `listener` takes, over HTTP/1 or HTTP/2 as each client speaks, each
/// request answered by `service`: the one [`serve_stdio`] is given, over TCP. It does not
/// return on its own, and dropped, it takes no more connections and cancels what is in flight
/// on those it has.
///
/// The connections are in the clear, as a client on this machine may send its token or its
/// password: one on another does not, and is served over TLS (`serve_tls`, the `tls` feature).
pub async fn serve_tcp<S, B>(service: S, listener: TcpListener) -> io::Result<()>
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    serve(service, listener, |stream| std::future::ready(Ok(stream))).await
}

/// The certificate a server shows over TLS, and whose certificates it wants of its clients, if
/// any: [`serve_tls`]'s.
#[cfg(feature = "tls")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tls {
    /// The path of a PEM file of the server's certificate, with the chain that leads to it.
    pub cert: PathBuf,
    /// The path of a PEM file of the certificate's private key, which may be the file `cert`
    /// is, holding both.
    pub key: PathBuf,
    /// Where given, the path of a PEM file of the certificate authorities whose clients are
    /// served, and no others: each shows a certificate one of them signed as its connection
    /// opens, in mutual TLS. Without it, no client is asked for one.
    pub client_ca: Option<PathBuf>,
}

#[cfg(feature = "tls")]
impl Tls {
    /// The certificate in the PEM file at `cert`, whose private key is in the one at `key`,
    /// shown to any client: no `client_ca`.
    pub fn new(cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        Tls {
            cert: cert.into(),
            key: key.into(),
            client_ca: None,
        }
    }

    /// The same, serving only the clients that show a certificate signed by an authority in
    /// the PEM file at `ca`.
    pub fn client_ca(mut self, ca: impl Into<PathBuf>) -> Self {
        self.client_ca = Some(ca.into());
        self
    }

    /// What opens each connection, the files read: an `Err` names the one that is not there
    /// or holds nothing of use.
    fn acceptor(&self) -> io::Result<tokio_rustls::TlsAcceptor> {
        use tokio_rustls::rustls::{
            self,
            pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
        };

        let unusable = |path: &Path, why: &dyn std::fmt::Display| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: {why}", path.display()),
            )
        };
        let read = |path: &Path| {
            std::fs::read(path)
                .map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))
        };
        let certificates = |path: &Path| -> io::Result<Vec<CertificateDer<'static>>> {
            match CertificateDer::pem_slice_iter(&read(path)?).collect::<Result<Vec<_>, _>>() {
                Ok(found) if !found.is_empty() => Ok(found),
                Ok(_) => Err(unusable(path, &"no certificate, in PEM")),
                Err(e) => Err(unusable(path, &e)),
            }
        };

        let provider = std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?;
        let builder = match &self.client_ca {
            None => builder.with_no_client_auth(),
            Some(ca) => {
                let mut authorities = rustls::RootCertStore::empty();
                for authority in certificates(ca)? {
                    authorities.add(authority).map_err(|e| unusable(ca, &e))?;
                }
                let clients = rustls::server::WebPkiClientVerifier::builder_with_provider(
                    authorities.into(),
                    provider,
                )
                .build()
                .map_err(|e| unusable(ca, &e))?;
                builder.with_client_cert_verifier(clients)
            }
        };
        let chain = certificates(&self.cert)?;
        let key = PrivateKeyDer::from_pem_slice(&read(&self.key)?)
            .map_err(|e| unusable(&self.key, &format_args!("no private key, in PEM: {e}")))?;
        let mut config = builder
            .with_single_cert(chain, key)
            .map_err(|e| unusable(&self.cert, &e))?;
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config)))
    }
}

/// How long a client has to open TLS on its connection before it is dropped.
#[cfg(feature = "tls")]
const OPENS_WITHIN: Duration = Duration::from_secs(10);

/// Serve every connection `listener` takes as [`serve_tcp`] does, over TLS as `tls` has it: a
/// client that does not open it, or shows no certificate where one is wanted, is not answered.
/// An `Err` is of `tls`, read as serving starts: a file of it not there, or holding nothing of
/// use.
#[cfg(feature = "tls")]
pub async fn serve_tls<S, B>(service: S, listener: TcpListener, tls: &Tls) -> io::Result<()>
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let acceptor = tls.acceptor()?;
    serve(service, listener, move |stream| {
        let opening = acceptor.accept(stream);
        async move {
            tokio::time::timeout(OPENS_WITHIN, opening)
                .await
                .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
        }
    })
    .await
}

/// Serve every connection `listener` takes, each as `open` makes of its stream, or not at all
/// where that fails.
async fn serve<S, B, Open, Opening, Io>(
    service: S,
    listener: TcpListener,
    open: Open,
) -> io::Result<()>
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    Open: Fn(TcpStream) -> Opening,
    Opening: Future<Output = io::Result<Io>> + Send + 'static,
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    // Dropped with this future, which is what cancels them.
    let mut connections = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            // One has ended, however it did: its client's to notice, not the server's.
            Some(_) = connections.join_next() => continue,
        };
        let stream = match accepted {
            Ok((stream, _)) => stream,
            // A client that left before it was taken.
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::ConnectionRefused
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset
                ) =>
            {
                continue;
            }
            // Out of something, files most likely, which those in flight free as they end.
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let service = TowerToHyperService::new(service.clone());
        let opening = open(stream);
        connections.spawn(async move {
            let Ok(io) = opening.await else {
                return;
            };
            let _ = auto::Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(TokioIo::new(io), service)
                .await;
        });
    }
}

/// Whether `error` is the input ending.
fn is_eof(error: &hyper::Error) -> bool {
    let mut source = std::error::Error::source(error);
    while let Some(error) = source {
        if let Some(error) = error.downcast_ref::<io::Error>() {
            return matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionReset
            );
        }
        source = error.source();
    }
    false
}

#[cfg(all(test, feature = "client"))]
mod tests {
    use axum::{Json, Router, routing::get};
    use http::HeaderMap;

    use super::*;
    use crate::{Auth, Client};

    #[tokio::test]
    async fn serve_stdio_returns_on_close() {
        let router = Router::new().route("/ping", get(|| async { Json("pong") }));
        let (pipe, server) = tokio::io::duplex(64 * 1024);
        let (input, output) = tokio::io::split(server);
        let served = tokio::spawn(serve_stdio(router, input, output));
        let client = Client::over(pipe);
        assert_eq!(client.get::<String>("/ping").await.unwrap(), "pong");
        client.close().await;
        served.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn serve_tcp_drop_cancels() {
        let router = Router::new()
            .route("/ping", get(|| async { Json("pong") }))
            .route("/never", get(std::future::pending::<Json<&str>>));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let served = tokio::spawn(serve_tcp(router, listener));
        let client =
            std::sync::Arc::new(Client::connect(&url, Auth::None, HeaderMap::new()).unwrap());
        assert_eq!(client.get::<String>("/ping").await.unwrap(), "pong");

        let waiting = tokio::spawn({
            let client = client.clone();
            async move { client.get::<String>("/never").await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        served.abort();
        assert!(waiting.await.unwrap().is_err());
        assert!(client.get::<String>("/ping").await.is_err());
    }

    /// What `serve_tls` does with a certificate is with the client's, which shows one
    /// (`client::tcp`).
    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn tls_bad_file_named_in_error() {
        let dir = std::env::temp_dir();
        let empty = dir.join(format!("http-over-x-{}-tls-empty.pem", std::process::id()));
        std::fs::write(&empty, "").unwrap();
        let missing = dir.join("http-over-x-tls-no-such.pem");
        for (tls, named) in [
            (Tls::new(&missing, &missing), "no-such"),
            (Tls::new(&empty, &missing), "empty"),
            (Tls::new(&missing, &missing).client_ca(&empty), "empty"),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let why = serve_tls(Router::new(), listener, &tls).await.unwrap_err();
            assert!(why.to_string().contains(named), "{why}");
        }
        let _ = std::fs::remove_file(empty);
    }
}
