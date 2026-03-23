use std::convert::Infallible;
use std::{net::SocketAddr, str::FromStr};

use mechanics_db::DbServer;

fn main() -> std::io::Result<Infallible> {
    let bind_addr: SocketAddr =
        SocketAddr::from_str(&std::env::var("LISTEN_ADDR").unwrap_or("".to_string()))
            .unwrap_or(SocketAddr::from(([127, 0, 0, 1], 3001)));
    let db_spec = std::env::var("DB_SPEC").expect("DB_SPEC environment variable must be set");
    let server = DbServer::new(&db_spec)?;
    let mut token_count = 0usize;
    if let Ok(tokens) = std::env::var("MECHANICS_ALLOWED_TOKENS") {
        for token in tokens.split(',').map(str::trim).filter(|t| !t.is_empty()) {
            server.add_token(token.to_string());
            token_count += 1;
        }
    }
    server.run(bind_addr)?;
    println!("Running mechanics-db server on {}", bind_addr);
    if token_count == 0 {
        println!(
            "No tokens configured via MECHANICS_ALLOWED_TOKENS. Requests will be denied until tokens are added."
        );
    } else {
        println!("Loaded {} bearer token(s).", token_count);
    }

    loop {
        std::thread::park();
    }
}
