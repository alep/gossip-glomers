use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, RPCResult, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::usize;
use tokio;
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
    broadcast: HashSet<usize>,
    topology: HashMap<String, Vec<String>>,
}

impl Inner {
    fn new() -> Self {
        Self {
            counter: 0,
            messages: HashSet::new(),
            broadcast: HashSet::new(),
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
            broadcast: HashSet::new(),
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
        match msg {
            Ok(Request::Init {}) => {
                let (r0, h0) = (runtime.clone(), self.clone());
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        let (neighbors, messages, broadcast) = {
                            let state = h0.inner.lock().unwrap();
                            let n = state.neighbors(r0.node_id()).clone();
                            let m = state.messages.clone();
                            let b = state.broadcast.clone();
                            (n, m, b)
                        };
                        let diff = messages
                            .difference(&broadcast)
                            .copied()
                            .collect::<HashSet<usize>>();

                        if diff.is_empty() {
                            continue;
                        }

                        let mut ok = true;
                        for n in neighbors {
                            let msg = Request::BatchBroadcast {
                                message: diff.iter().copied().collect::<Vec<usize>>(),
                            };
                            let (ctx, _handler) = Context::with_timeout(Duration::from_millis(400));
                            let message = r0.call(ctx, n, msg).await;
                            match message {
                                Ok(_) => {}
                                Err(_) => ok = false,
                            }
                        }
                        if ok {
                            let mut state = h0.inner.lock().unwrap();
                            state.broadcast = state.broadcast.union(&diff).copied().collect();
                        }
                    }
                });
            }

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
                {
                    let mut inner = self.inner.lock().unwrap();
                    let msg_set = &mut inner.messages;
                    msg_set.insert(message)
                };

                let broadcast_response = Response::BroadcastOk {};
                return runtime.reply(req, broadcast_response).await;
            }
            Ok(Request::BatchBroadcast { message }) => {
                {
                    let mut inner = self.inner.lock().unwrap();
                    let msg_set = &mut inner.messages;
                    for m in message {
                        msg_set.insert(m);
                    }
                };
                let broadcast_response = Response::BatchBroadcastOk {};
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
    BatchBroadcast {
        message: Vec<usize>,
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
    BatchBroadcastOk {},
    ReadOk { messages: HashSet<usize> },
    TopologyOk {},
}
