use async_trait::async_trait;
use maelstrom::kv::{lin_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
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
    s: Storage,
}

impl Handler {
    fn new(runtime: Runtime) -> Self {
        Self { s: lin_kv(runtime) }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Read {}) => {
                let (ctx, _handler) = Context::new();
                let value = self
                    .s
                    .get(ctx, runtime.node_id().to_string())
                    .await
                    .unwrap();
                let read_response = Response::ReadOk { value };
                return runtime.reply(req, read_response).await;
            }
            Ok(Request::Add { delta }) => {
                let (ctx, _handler) = Context::new();
                self.s
                    .put(ctx, runtime.node_id().to_string(), delta)
                    .await
                    .unwrap();
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
