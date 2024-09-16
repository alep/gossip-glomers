use async_trait::async_trait;
use maelstrom::kv::{seq_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
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
    c: HashMap<String, usize>,
}

impl Handler {
    fn new(runtime: Runtime) -> Self {
        let initial: Vec<(String, usize)> = runtime
            .nodes()
            .iter()
            .map(|x| -> (String, usize) { (x.to_string(), 0) })
            .collect();
        Self {
            s: seq_kv(runtime),
            i: Arc::new(Mutex::new(Inner {
                c: HashMap::from_iter(initial),
            })),
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Init {}) => {
                let (r0, h0) = (runtime.clone(), self.clone());

                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(5000)).await;

                        let counter = {
                            let i = h0.i.lock().unwrap();
                            i.c.clone()
                        };

                        for to in ["n0", "n1", "n2"] {
                            if to != r0.node_id() {
                                let r1 = r0.clone();
                                let counter0 = counter.clone();
                                tokio::spawn(async move {
                                    let (ctx, _handler) =
                                        Context::with_timeout(Duration::from_millis(400));
                                    let msg = Request::Replicate {
                                        values: counter0.clone(),
                                    };

                                    let _message = r1.call(ctx, to, msg).await;
                                });
                            }
                        }
                    }
                });
            }
            Ok(Request::Read {}) => {
                let value = {
                    let i = self.i.lock().unwrap();
                    i.c.values().sum()
                };
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
                {
                    let mut i = self.i.lock().unwrap();
                    i.c.insert(runtime.node_id().to_string(), val + delta);
                }
                let response = Response::AddOk {};
                return runtime.reply(req, response).await;
            }
            Ok(Request::Replicate { values }) => {
                for n in ["n0", "n1", "n2"] {
                    {
                        let mut i = self.i.lock().unwrap();
                        let v0 = i.c.get(n).copied().unwrap_or(0);
                        let v1 = values.get(n).copied().unwrap_or(0);
                        i.c.insert(n.to_string(), max(v0, v1));
                    }
                }
                let response = Response::ReplicateOk {};
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
    Replicate { values: HashMap<String, usize> },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    ReadOk { value: usize },
    AddOk {},
    ReplicateOk {},
}
