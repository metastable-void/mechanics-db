use std::future::Future;
use std::sync::Arc;

pub struct TokioRt {
    runtime: tokio::runtime::Runtime,
}

impl TokioRt {
    pub fn new(multithreaded: bool, thread_name: Option<&str>) -> std::io::Result<Self> {
        let mut builder = if multithreaded {
            tokio::runtime::Builder::new_multi_thread()
        } else {
            tokio::runtime::Builder::new_current_thread()
        };

        if let Some(name) = thread_name {
            builder.thread_name(name);
        }

        let runtime = builder.enable_all().build()?;
        Ok(Self { runtime })
    }

    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.runtime.block_on(future)
    }

    pub fn spawn_background<R, F, C>(
        self: Arc<Self>,
        callback: C,
        thread_name: Option<&str>,
    ) -> std::io::Result<()>
    where
        R: Send + 'static,
        F: Future<Output = R> + Send + 'static,
        C: FnOnce() -> F + Send + 'static,
    {
        let mut builder = std::thread::Builder::new();
        if let Some(name) = thread_name {
            builder = builder.name(name.to_string());
        }

        builder
            .spawn(move || {
                let _ = self.block_on(callback());
            })
            .map(|_| ())
    }
}
