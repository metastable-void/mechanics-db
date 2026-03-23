mod spawn;

use std::collections::HashSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use sqlx::{Arguments, Column, Connection, Row};
use parking_lot::RwLock;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header::{AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use sqlx::AnyConnection;
use serde::{Deserialize, Serialize};
use sqlx::any::AnyArguments;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

type HttpResponse = Response<Full<Bytes>>;
type DbConn = Arc<Mutex<AnyConnection>>;

enum ApiError {
    NotFound,
    Unauthorized,
    InvalidType,
    InvalidRequest,
    Db(String),
}

impl ApiError {
    fn to_response(&self) -> HttpResponse {
        let (status, message) = match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "Not found".to_string()),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            Self::InvalidType => (StatusCode::BAD_REQUEST, "Invalid type".to_string()),
            Self::InvalidRequest => (StatusCode::BAD_REQUEST, "Invalid request".to_string()),
            Self::Db(err) => (StatusCode::BAD_REQUEST, err.clone()),
        };

        let mut response = json_response(status, &serde_json::json!({ "error": message }));
        if matches!(self, Self::Unauthorized) {
            response.headers_mut().insert(
                WWW_AUTHENTICATE,
                hyper::header::HeaderValue::from_static("Bearer"),
            );
        }

        response
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct DbQuery {
    pub(crate) query: String,
    pub(crate) params: Vec<serde_json::Value>,
}

fn json_response(status: StatusCode, value: &serde_json::Value) -> HttpResponse {
    let body = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());

    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );

    response
}

fn has_json_content_type(req: &Request<Incoming>) -> bool {
    req.headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
        })
        .unwrap_or(false)
}

fn parse_bearer_token(header_value: &str) -> Option<&str> {
    let mut parts = header_value.split_whitespace();
    let scheme = parts.next()?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = parts.next()?;
    if token.is_empty() || parts.next().is_some() {
        return None;
    }
    Some(token)
}

fn bearer_token(req: &Request<Incoming>) -> Option<&str> {
    req.headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_bearer_token)
}

fn is_authorized(tokens: &RwLock<HashSet<String>>, req: &Request<Incoming>) -> bool {
    let Some(token) = bearer_token(req) else {
        return false;
    };
    tokens.read().contains(token)
}

async fn parse_json_query(req: Request<Incoming>) -> Result<DbQuery, ApiError> {
    if !has_json_content_type(&req) {
        return Err(ApiError::InvalidType);
    }

    let body = req
        .into_body()
        .collect()
        .await
        .map_err(|_| ApiError::InvalidRequest)?
        .to_bytes();

    serde_json::from_slice(&body).map_err(|_| ApiError::InvalidRequest)
}

async fn execute_query(
    conn: DbConn,
    query: DbQuery,
) -> Result<serde_json::Value, ApiError> {
    let mut arguments = AnyArguments::default();
    for arg in query.params {
        match match arg {
            serde_json::Value::Bool(b) => arguments.add(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    arguments.add(i)
                } else if let Some(f) = n.as_f64() {
                    arguments.add(f)
                } else {
                    return Err(ApiError::InvalidType);
                }
            },
            serde_json::Value::String(s) => arguments.add(s),
            _ => {
                return Err(ApiError::InvalidType)
            }
        } {
            Ok(_) => {},
            Err(e) => {
                return Err(ApiError::Db(e.to_string()));
            },
        }
    }
    let query = sqlx::query_with(&query.query, arguments);
    let mut conn = conn.lock().await;
    let res = query.fetch_all(&mut *conn).await.map_err(|e| ApiError::Db(e.to_string()))?;

    let mut arr = Vec::with_capacity(res.len());
    for row in res {
        let mut obj = serde_json::Map::with_capacity(row.len());
        for (idx, col) in row.columns().iter().enumerate() {
            let value = if let Ok(v) = row.try_get::<Option<i64>, _>(idx) {
                v.map_or(serde_json::Value::Null, serde_json::Value::from)
            } else if let Ok(v) = row.try_get::<Option<f64>, _>(idx) {
                v.map_or(serde_json::Value::Null, serde_json::Value::from)
            } else if let Ok(v) = row.try_get::<Option<bool>, _>(idx) {
                v.map_or(serde_json::Value::Null, serde_json::Value::Bool)
            } else if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
                v.map_or(serde_json::Value::Null, serde_json::Value::String)
            } else if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(idx) {
                v.map_or(serde_json::Value::Null, |bytes| {
                    serde_json::Value::String(String::from_utf8_lossy(&bytes).into_owned())
                })
            } else {
                serde_json::Value::Null
            };
            obj.insert(col.name().to_owned(), value);
        }
        arr.push(serde_json::Value::Object(obj));
    }
    Ok(serde_json::Value::Array(arr))
}

async fn handle_request(
    conn: DbConn,
    tokens: Arc<RwLock<HashSet<String>>>,
    req: Request<Incoming>,
) -> Result<HttpResponse, Infallible> {
    if req.method() != Method::POST || req.uri().path() != "/api/v1/db/query" {
        return Ok(ApiError::NotFound.to_response());
    }
    if !is_authorized(&tokens, &req) {
        return Ok(ApiError::Unauthorized.to_response());
    }

    let job = match parse_json_query(req).await {
        Ok(job) => job,
        Err(error) => return Ok(error.to_response()),
    };

    match execute_query(conn, job).await {
        Ok(result) => Ok(json_response(StatusCode::OK, &result)),
        Err(error) => Ok(error.to_response()),
    }
}

#[derive(Clone)]
/// HTTP server wrapper around a shared [`AnyConnection`].
///
/// The server exposes a single endpoint:
/// `POST /api/v1/db/query` with a JSON SQL query payload.
pub struct DbServer {
    conn: Arc<Mutex<AnyConnection>>,
    tokens: Arc<RwLock<HashSet<String>>>,
}

impl DbServer {
    /// Creates a new server with an initialized DB connection.
    pub fn new(db_spec: &str) -> std::io::Result<Self> {
        let db_spec = db_spec.to_owned();
        spawn::spawn_blocking(move || {
            let db_spec = db_spec.clone();

            async move {
                let conn = AnyConnection::connect(&db_spec).await.map_err(std::io::Error::other)?;
                let conn = Arc::new(Mutex::new(conn));
                Ok(Self {
                    conn,
                    tokens: Arc::new(RwLock::default()),
                })
            }
        }, true, Some("DbServer::new")).ok_or(std::io::Error::other(""))?
    }

    /// Adds an approved Bearer token to this server.
    ///
    /// Empty or whitespace-only tokens are ignored.
    pub fn add_token(&self, token: String) {
        let token = token.trim();
        if token.is_empty() {
            return;
        }

        self.tokens.write().insert(token.to_string());
    }

    /// Returns a clone of the internal shared pool handle.
    pub(crate) fn conn(&self) -> DbConn {
        self.conn.clone()
    }

    /// Starts the HTTP server on `bind_addr` in a dedicated thread.
    ///
    /// This method is non-blocking from the caller perspective: it spawns the
    /// runtime thread and returns once the listener setup succeeds.
    ///
    /// Returns an I/O error if binding the socket, configuring non-blocking
    /// mode, or spawning the runtime thread fails.
    pub fn run(&self, bind_addr: SocketAddr) -> std::io::Result<()> {
        let std_listener = std::net::TcpListener::bind(bind_addr)?;
        std_listener.set_nonblocking(true)?;

        let server = self.clone();
        spawn::spawn_background(move || {
            let listener = TcpListener::from_std(std_listener).ok();
            async move {
                let listener = if let Some(l) = listener { l } else {
                    return Err(std::io::Error::other("listener error"));
                };
                loop {
                    let (stream, _) = listener.accept().await?;
                    let io = TokioIo::new(stream);
                    let conn = server.conn();
                    let tokens = Arc::clone(&server.tokens);

                    tokio::task::spawn(async move {
                        let service = service_fn(move |req| {
                            handle_request(conn.clone(), Arc::clone(&tokens), req)
                        });
                        if let Err(err) =
                            http1::Builder::new().serve_connection(io, service).await
                        {
                            eprintln!("Error serving connection: {err:?}");
                        }
                    });
                }
                #[allow(unreachable_code)]
                Ok::<_, std::io::Error>(())
            }
        }, false, Some("MechanicsDb"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::parse_bearer_token;

    #[test]
    fn parse_bearer_token_accepts_case_insensitive_scheme() {
        assert_eq!(parse_bearer_token("Bearer abc"), Some("abc"));
        assert_eq!(parse_bearer_token("bearer abc"), Some("abc"));
        assert_eq!(parse_bearer_token("BEARER abc"), Some("abc"));
    }

    #[test]
    fn parse_bearer_token_accepts_flexible_whitespace() {
        assert_eq!(parse_bearer_token("  Bearer   abc  "), Some("abc"));
        assert_eq!(parse_bearer_token("\tBearer\tabc\t"), Some("abc"));
    }

    #[test]
    fn parse_bearer_token_rejects_invalid_values() {
        assert_eq!(parse_bearer_token("Basic abc"), None);
        assert_eq!(parse_bearer_token("Bearer"), None);
        assert_eq!(parse_bearer_token("Bearer "), None);
        assert_eq!(parse_bearer_token("Bearer abc def"), None);
    }
}
