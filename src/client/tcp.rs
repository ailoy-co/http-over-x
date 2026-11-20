//! The client's end of HTTP over TCP: the server at a base URL, each endpoint a path under it.

use std::net::IpAddr;
use std::path::Path;
use std::sync::{Arc, PoisonError, RwLock};

use http::{HeaderMap, header};
use http_body_util::BodyExt;
use reqwest::{RequestBuilder, Url};

use super::{
    Body,
    credentials::{Authorization, Credentials},
};
use crate::{Auth, Error};

/// What is sent with every request, read as each is sent, so that a change reaches the next
/// one: a forwarder opened before keeps a hold on it too.
pub(super) type Shared<T> = Arc<RwLock<T>>;

/// What `shared` holds, as sent now.
pub(super) fn current<T: Clone>(shared: &Shared<T>) -> T {
    shared
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
}

/// The server at a base URL. Nothing to close: the server keeps nothing between requests.
pub(super) struct Tcp {
    /// Made anew when a certificate is given or taken away ([`Auth::Mtls`]): its connections
    /// are opened with one.
    pub(super) http: Shared<reqwest::Client>,
    /// Ends in `/`, so a path joins under it.
    pub(super) base: Url,
    pub(super) credentials: Arc<Credentials>,
    pub(super) headers: Shared<HeaderMap>,
}

impl Tcp {
    /// The server whose base URL is `url`, an `http` or `https` URL, with `auth` and `headers`
    /// sent on every request to it. Nothing is sent until the first request.
    pub(super) fn connect(url: &str, auth: Auth, headers: HeaderMap) -> Result<Tcp, Error> {
        let mut base = Url::parse(url).map_err(|e| Error::Client(format!("{url}: {e}")))?;
        if !matches!(base.scheme(), "http" | "https") {
            return Err(Error::Client(format!("not an http or https URL: {base}")));
        }
        let http = http(&base, &auth)?;
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        Ok(Tcp {
            http: Arc::new(RwLock::new(http)),
            base,
            credentials: Arc::new(Credentials::new(auth)),
            headers: Arc::new(RwLock::new(headers)),
        })
    }

    /// Send `auth` with every request from now on, in place of what was sent until now.
    pub(super) fn set_auth(&self, auth: Auth) -> Result<(), Error> {
        let http = http(&self.base, &auth)?;
        // The connections kept open go on being used unless a certificate comes or goes.
        if self.credentials.mtls() || matches!(auth, Auth::Mtls { .. }) {
            *self.http.write().unwrap_or_else(PoisonError::into_inner) = http;
        }
        self.credentials.set(auth);
        Ok(())
    }

    /// Send `headers` with every request from now on, in place of those sent until now.
    pub(super) fn set_headers(&self, headers: HeaderMap) {
        *self.headers.write().unwrap_or_else(PoisonError::into_inner) = headers;
    }

    /// Send `request`, whose URI is a path under the base.
    pub(super) async fn send(
        &self,
        request: http::Request<Body>,
    ) -> Result<http::Response<Body>, Error> {
        let (parts, body) = request.into_parts();
        let url = self
            .base
            .join(parts.uri.to_string().trim_start_matches('/'))
            .map_err(|e| Error::Client(format!("{}: {e}", parts.uri)))?;
        // The request's own headers win over the ones sent with every request.
        let mut headers = current(&self.headers);
        headers.extend(parts.headers);
        let request = current(&self.http)
            .request(parts.method, url)
            .body(reqwest::Body::wrap(body));
        let authorization = self
            .credentials
            .authorization()
            .await
            .map_err(Error::Client)?;
        let response = authorized(request, headers, authorization)
            .send()
            .await
            .map_err(|e| Error::Transport(format!("{e:#}")))?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            self.credentials.refused();
        }
        let response: http::Response<reqwest::Body> = response.into();
        Ok(response.map(|body| body.map_err(std::io::Error::other).boxed()))
    }
}

/// `request` with `headers`, and `authorization` as its `Authorization` in place of any among
/// them.
pub(super) fn authorized(
    request: RequestBuilder,
    mut headers: HeaderMap,
    authorization: Option<Authorization>,
) -> RequestBuilder {
    if authorization.is_some() {
        headers.remove(header::AUTHORIZATION);
    }
    let request = request.headers(headers);
    match authorization {
        None => request,
        Some(Authorization::Bearer(token)) => request.bearer_auth(token),
        Some(Authorization::Basic { username, password }) => {
            request.basic_auth(username, Some(password))
        }
    }
}

/// What requests to `base` with `auth` are sent by: its connections opened with the
/// certificate of an [`Auth::Mtls`], read now. Refused where `auth` cannot go to `base`
/// ([`not_in_the_clear`]), or its certificate cannot be read.
fn http(base: &Url, auth: &Auth) -> Result<reqwest::Client, Error> {
    builder(base, auth)?
        .build()
        .map_err(|e| Error::Client(format!("{e:#}")))
}

/// [`http`], yet to be built.
fn builder(base: &Url, auth: &Auth) -> Result<reqwest::ClientBuilder, Error> {
    not_in_the_clear(base, auth)?;
    let Auth::Mtls { cert, key, ca } = auth else {
        return Ok(reqwest::Client::builder());
    };
    let read = |path: &Path| {
        std::fs::read(path).map_err(|e| Error::Client(format!("{}: {e}", path.display())))
    };
    let mut pem = read(cert)?;
    if key != cert {
        pem.push(b'\n');
        pem.extend(read(key)?);
    }
    let unusable = |e: reqwest::Error| {
        Error::Client(format!(
            "no certificate and its key, in PEM, in {} and {}: {e:#}",
            cert.display(),
            key.display()
        ))
    };
    let identity = reqwest::Identity::from_pem(&pem).map_err(unusable)?;
    let builder = reqwest::Client::builder().identity(identity);
    let Some(ca) = ca else {
        return Ok(builder);
    };
    let authorities = reqwest::Certificate::from_pem_bundle(&read(ca)?)
        .ok()
        .filter(|authorities| !authorities.is_empty())
        .ok_or_else(|| Error::Client(format!("no certificate, in PEM, in {}", ca.display())))?;
    Ok(builder.tls_certs_merge(authorities))
}

/// Refuses `auth` to `base` when its token or password would go in the clear: over `http` to
/// any host but this machine's loopback, where a local server is (the protocol) and no wire.
/// A certificate is refused over `http` to any host: none is shown but as TLS opens.
fn not_in_the_clear(base: &Url, auth: &Auth) -> Result<(), Error> {
    if matches!(auth, Auth::None) || base.scheme() == "https" {
        return Ok(());
    }
    if matches!(auth, Auth::Mtls { .. }) {
        return Err(Error::Client(format!(
            "a certificate is not shown to {base}: reach the server over https"
        )));
    }
    let loopback = base.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !loopback {
        return Err(Error::Client(format!(
            "the credentials would go in the clear to {base}: reach the server over https"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sent(headers: &[(&str, &str)], authorization: Option<Authorization>) -> HeaderMap {
        let mut given = HeaderMap::new();
        for (name, value) in headers {
            let name = header::HeaderName::from_bytes(name.as_bytes()).unwrap();
            given.append(name, value.parse().unwrap());
        }
        let request = reqwest::Client::new().get("http://127.0.0.1:9/");
        authorized(request, given, authorization)
            .build()
            .unwrap()
            .headers()
            .clone()
    }

    #[test]
    fn a_token_takes_the_place_of_an_authorization_header_and_leaves_the_rest() {
        let given = [("authorization", "Bearer mine"), ("x-project", "p")];
        let headers = sent(&given, Some(Authorization::Bearer("t".into())));
        let authorization: Vec<_> = headers.get_all(header::AUTHORIZATION).iter().collect();
        assert_eq!(authorization, ["Bearer t"]);
        assert_eq!(headers["x-project"], "p");
    }

    #[test]
    fn a_password_goes_with_its_username_in_base64() {
        let basic = Authorization::Basic {
            username: "alice".into(),
            password: "secret".into(),
        };
        let headers = sent(&[("authorization", "Bearer mine")], Some(basic));
        let authorization: Vec<_> = headers.get_all(header::AUTHORIZATION).iter().collect();
        assert_eq!(authorization, ["Basic YWxpY2U6c2VjcmV0"]);
    }

    /// A certificate authority's certificate, and one it signed for `name` with its key, each
    /// in PEM.
    struct Signed {
        cert: String,
        key: String,
    }

    fn authority() -> (rcgen::Certificate, rcgen::KeyPair) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        (params.self_signed(&key).unwrap(), key)
    }

    fn signed(name: &str, (ca, ca_key): &(rcgen::Certificate, rcgen::KeyPair)) -> Signed {
        let key = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(vec![name.to_owned()]).unwrap();
        let cert = params.signed_by(&key, ca, ca_key).unwrap();
        Signed {
            cert: cert.pem(),
            key: key.serialize_pem(),
        }
    }

    /// A [`Tcp`] to the server at `url`, as [`Tcp::connect`] makes it but trusting only the
    /// server certificates `ca` signed.
    fn trusting(url: &str, auth: Auth, ca: &rcgen::Certificate) -> Tcp {
        let base = Url::parse(url).unwrap();
        let ca = reqwest::Certificate::from_pem(ca.pem().as_bytes()).unwrap();
        let http = builder(&base, &auth)
            .unwrap()
            .tls_certs_only([ca])
            .build()
            .unwrap();
        Tcp {
            http: Arc::new(RwLock::new(http)),
            base,
            credentials: Arc::new(Credentials::new(auth)),
            headers: Arc::default(),
        }
    }

    /// The PEM `contents` in a file of this test's, `name`.
    fn file(name: &str, contents: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("http-over-x-{}-{name}", std::process::id()));
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[tokio::test]
    async fn mtls() {
        let ca = authority();
        let trusted = file("ca.pem", &ca.0.pem());
        let server = signed("127.0.0.1", &ca);
        let server = file("server.pem", &format!("{}{}", server.cert, server.key));
        // Two servers answering `204` over TLS: one that wants a client certificate `ca`
        // signed, and one that wants none.
        let mut urls = Vec::new();
        for tls in [
            crate::Tls::new(&server, &server).client_ca(&trusted),
            crate::Tls::new(&server, &server),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            urls.push(format!("https://{}/", listener.local_addr().unwrap()));
            let router = axum::Router::new().fallback(|| async { http::StatusCode::NO_CONTENT });
            tokio::spawn(async move { crate::serve_tls(router, listener, &tls).await });
        }
        let [url, open]: [String; 2] = urls.try_into().unwrap();
        let request = || http::Request::get("/").body(crate::full("")).unwrap();

        let alice = signed("alice", &ca);
        let cert = file("alice.pem", &alice.cert);
        let key = file("alice.key", &alice.key);
        let tcp = trusting(&url, Auth::mtls(&cert, &key), &ca.0);
        assert_eq!(tcp.send(request()).await.unwrap().status(), 204);

        // The server's authority given as a file, as a caller gives it.
        let auth = Auth::Mtls {
            cert: cert.clone(),
            key: key.clone(),
            ca: Some(trusted.clone()),
        };
        let tcp = Tcp::connect(&url, auth, HeaderMap::new()).unwrap();
        assert_eq!(tcp.send(request()).await.unwrap().status(), 204);
        // Without it, the server is not one to show a certificate to.
        let tcp = Tcp::connect(&url, Auth::mtls(&cert, &key), HeaderMap::new()).unwrap();
        assert!(matches!(
            tcp.send(request()).await,
            Err(Error::Transport(_))
        ));

        // The certificate and its key in one file.
        let both = file("alice-both.pem", &format!("{}{}", alice.cert, alice.key));
        let tcp = trusting(&url, Auth::mtls(&both, &both), &ca.0);
        assert_eq!(tcp.send(request()).await.unwrap().status(), 204);

        // Without one, and with one another authority signed, no request is answered.
        let tcp = trusting(&url, Auth::None, &ca.0);
        assert!(matches!(
            tcp.send(request()).await,
            Err(Error::Transport(_))
        ));
        let mallory = signed("mallory", &authority());
        let theirs = file("mallory.pem", &format!("{}{}", mallory.cert, mallory.key));
        let tcp = trusting(&url, Auth::mtls(&theirs, &theirs), &ca.0);
        assert!(matches!(
            tcp.send(request()).await,
            Err(Error::Transport(_))
        ));

        // A server that wants no certificate answers a client that shows none.
        let tcp = trusting(&open, Auth::None, &ca.0);
        assert_eq!(tcp.send(request()).await.unwrap().status(), 204);

        for path in [cert, key, both, theirs, trusted, server] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn a_certificate_is_given_over_https_and_in_files_that_hold_one() {
        let https = Url::parse("https://example.com/").unwrap();
        let ca = authority();
        let alice = signed("alice", &ca);
        let cert = file("only.pem", &alice.cert);
        let key = file("only.key", &alice.key);
        assert!(http(&https, &Auth::mtls(&cert, &key)).is_ok());

        // Not over http, to this machine even: no certificate would be shown.
        for url in ["http://example.com/", "http://127.0.0.1:9/"] {
            let refused = http(&Url::parse(url).unwrap(), &Auth::mtls(&cert, &key));
            let Err(Error::Client(why)) = refused else {
                panic!("{url}: not refused");
            };
            assert!(why.contains("https"), "{why}");
        }
        // A file that is not there is named, and so is a pair with no key between them.
        let missing = std::env::temp_dir().join("http-over-x-no-such.pem");
        let Err(Error::Client(why)) = http(&https, &Auth::mtls(&missing, &key)) else {
            panic!("a missing file: not refused");
        };
        assert!(why.contains("http-over-x-no-such.pem"), "{why}");
        let Err(Error::Client(why)) = http(&https, &Auth::mtls(&cert, &cert)) else {
            panic!("no key: not refused");
        };
        assert!(why.contains("only.pem"), "{why}");
        // And so is a `ca` with no certificate in it.
        let auth = Auth::Mtls {
            cert: cert.clone(),
            key: key.clone(),
            ca: Some(key.clone()),
        };
        let Err(Error::Client(why)) = http(&https, &auth) else {
            panic!("no authority: not refused");
        };
        assert!(why.contains("only.key"), "{why}");

        for path in [cert, key] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn without_a_token_the_headers_go_as_given() {
        let headers = sent(&[("authorization", "Bearer mine")], None);
        assert_eq!(headers[header::AUTHORIZATION], "Bearer mine");
    }
}
