use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) fn main() {
    let _ = Runtime::init(try_main());
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::new());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Clone, Default)]
struct Handler {
    pub counter: Arc<AtomicUsize>,
}

impl Handler {
    fn new() -> Self {
        Self {
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Echo {}) => {
                let echo = req.body.clone().with_type("echo_ok");
                return runtime.reply(req, echo).await;
            }
            Ok(Request::Generate {}) => {
                let msg_id = self.counter.clone().fetch_add(1, Ordering::Relaxed);
                let id = format!("{}-{}", runtime.node_id(), msg_id);
                let generate = Response::GenerateOk { id };
                return runtime.reply(req, generate).await;
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
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    EchoOk {},
    GenerateOk { id: String },
}
