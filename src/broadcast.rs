use async_trait::async_trait;
use core::panic;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, RPCResult, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio_context::context::Context;

pub(crate) fn main() {
    let _ = Runtime::init(try_main());
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::new());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Clone, Default)]
struct Handler {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    counter: usize,
    messages: HashSet<usize>,
    topology: HashMap<String, Vec<String>>,
}

impl Inner {
    fn new() -> Self {
        Self {
            counter: 0,
            messages: HashSet::new(),
            topology: HashMap::new(),
        }
    }

    fn neighbors(&self, node_id: &str) -> &Vec<String> {
        self.topology.get(node_id).unwrap()
    }
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            counter: 0,
            messages: HashSet::new(),
            topology: HashMap::new(),
        }
    }
}

impl Handler {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::new())),
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        let src: &String = &req.src;
        match msg {
            Ok(Request::Echo {}) => {
                let echo = req.body.clone().with_type("echo_ok");
                return runtime.reply(req, echo).await;
            }
            Ok(Request::Generate {}) => {
                let msg_id = {
                    let mut inner = self.inner.lock().unwrap();
                    let last_count = inner.counter;
                    inner.counter += 1;
                    last_count
                };
                let id = format!("{}-{}", runtime.node_id(), msg_id);
                let generate = Response::GenerateOk { id };
                return runtime.reply(req, generate).await;
            }
            Ok(Request::Broadcast { message }) => {
                let should_broadcast: Option<Vec<String>> = {
                    let mut inner = self.inner.lock().unwrap();
                    let msg_set = &mut inner.messages;
                    if msg_set.insert(message) {
                        Some(inner.neighbors(runtime.node_id()).clone())
                    } else {
                        None
                    }
                };

                match should_broadcast {
                    Some(neighbors) => {
                        for n in neighbors {
                            if *n != *src {
                                let runtime_clone = runtime.clone();
                                runtime.spawn(async move {
                                    loop {
                                        let (ctx, _handler) =
                                            Context::with_timeout(Duration::from_secs(1));
                                        let res: Result<RPCResult> = runtime_clone
                                            .rpc(n.clone(), Request::Broadcast { message })
                                            .await;
                                        match res {
                                            Ok(mut res) => match res.done_with(ctx).await {
                                                Ok(_) => break,
                                                Err(_) => {} // Just gonna pretend that every error
                                                             // is a time out.
                                            },
                                            // Seriliazing errors!
                                            Err(_) => panic!("What just happened?"),
                                        }
                                    }
                                });
                            }
                        }
                    }
                    None => {}
                }

                let broadcast_response = Response::BroadcastOk {};
                return runtime.reply(req, broadcast_response).await;
            }
            Ok(Request::Read {}) => {
                let values = {
                    let inner = self.inner.lock().unwrap();
                    inner.messages.clone()
                };
                let read_response = Response::ReadOk { messages: values };
                return runtime.reply(req, read_response).await;
            }
            Ok(Request::Topology { topology }) => {
                {
                    let mut inner = self.inner.lock().unwrap();
                    inner.topology = topology;
                };
                let topo_response = Response::TopologyOk {};
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
    Echo {},
    Generate {},
    Broadcast {
        message: usize,
    },
    Read {},
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    EchoOk {},
    GenerateOk { id: String },
    BroadcastOk {},
    ReadOk { messages: HashSet<usize> },
    TopologyOk {},
}
