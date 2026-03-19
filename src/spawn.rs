pub fn spawn_blocking<
    'a,
    R: Send + 'static,
    F: Future<Output = R> + Send + 'a,
    C: FnOnce() -> F + Send + 'static,
>(
    callback: C,
    multithreaded: bool,
    thread_name: Option<&str>,
) -> Option<R> {
    let mut builder = std::thread::Builder::new();

    if let Some(name) = thread_name {
        builder = builder.name(name.to_string());
    }
    builder
        .spawn(move || {
            let rt = if multithreaded {
                tokio::runtime::Builder::new_multi_thread()
            } else {
                tokio::runtime::Builder::new_current_thread()
            }
            .enable_all()
            .build()
            .ok()?;

            Some(rt.block_on(callback()))
        })
        .ok()
        .map(|j| j.join().ok())??
}

pub fn spawn_background<
    'a,
    R: Send + 'static,
    F: Future<Output = R> + Send + 'a,
    C: FnOnce() -> F + Send + 'static,
>(
    callback: C,
    multithreaded: bool,
    thread_name: Option<&str>,
) -> std::io::Result<()> {
    let mut builder = std::thread::Builder::new();

    if let Some(name) = thread_name {
        builder = builder.name(name.to_string());
    }
    builder
        .spawn(move || {
            let rt = if multithreaded {
                tokio::runtime::Builder::new_multi_thread()
            } else {
                tokio::runtime::Builder::new_current_thread()
            }
            .enable_all()
            .build()
            .ok();
            
            let rt = if let Some(r) = rt {
                r
            } else {
                return;
            };

            rt.block_on(callback());
        })
        .map(|_| ())
}
