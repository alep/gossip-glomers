use async_trait::async_trait;
use maelstrom::kv::{lin_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;
use tokio_context::context::Context;

pub(crate) fn main() {
    let _ = Runtime::init(try_main());
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let handler = Arc::new(Handler::new(runtime.clone()));
    runtime.with_handler(handler).run().await
}

#[derive(Clone)]
struct Handler {
    i: Arc<Mutex<Inner>>,
    s: Storage,
}

struct Inner {
    c: usize,
}

impl Handler {
    fn new(runtime: Runtime) -> Self {
        Self {
            s: lin_kv(runtime),
            i: Arc::new(Mutex::new(Inner { c: 0 })),
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Read {}) => {
                let mut value: usize = 0;
                for node in runtime.neighbours() {
                    let (ctx, _handler) = Context::new();
                    match self.s.get::<usize>(ctx, node.to_string()).await {
                        Ok(val) => value = value + val,
                        Err(_) => {}
                    }
                }
                let read_response = Response::ReadOk { value };
                return runtime.reply(req, read_response).await;
            }
            Ok(Request::Add { delta }) => {
                let val = {
                    let i = self.i.lock().unwrap();
                    i.c
                };
                let (ctx, _handler) = Context::new();
                self.s
                    .cas(ctx, runtime.node_id().to_string(), val, val + delta, true)
                    .await
                    .unwrap();
                {
                    let mut i = self.i.lock().unwrap();
                    i.c = val + delta;
                }
                let topo_response = Response::AddOk {};
                return runtime.reply(req, topo_response).await;
            }
            _ => {}
        }
        done(runtime, req)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Init {},
    Read {},
    Add { delta: usize },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    ReadOk { value: usize },
    AddOk {},
}
