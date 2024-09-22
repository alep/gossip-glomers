use async_trait::async_trait;
use maelstrom::kv::{seq_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use rand;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::usize;
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
        Self { s: seq_kv(runtime) }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Read {}) => {
                let mut value: usize = 0;
                for n in runtime.nodes() {
                    let (ctx, _handler) = Context::new();
                    let x = rand::random::<usize>();
                    let s = format!("random_{:}_{:}", runtime.node_id(), x);
                    let _ = self.s.put(ctx, s, x).await;
                    let (ctx, _handler) = Context::new();
                    let val = match self.s.get(ctx, n.to_string()).await {
                        Ok(val) => val,
                        Err(_) => 0,
                    };
                    value += val;
                }

                let read_response = Response::ReadOk { value };
                return runtime.reply(req, read_response).await;
            }
            Ok(Request::Add { delta }) => {
                let (ctx, _handler) = Context::new();
                let val = match self.s.get(ctx, runtime.node_id().to_string()).await {
                    Ok(val) => val,
                    Err(_) => 0,
                };
                let (ctx, _handler) = Context::new();
                self.s
                    .cas(ctx, runtime.node_id().to_string(), val, val + delta, true)
                    .await
                    .unwrap();

                let response = Response::AddOk {};
                return runtime.reply(req, response).await;
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

