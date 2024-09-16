use async_trait::async_trait;

use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};

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
    inner: Arc<Mutex<State>>,
}

impl Handler {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(State::default())),
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Send { key, msg }) => {
                {
                    let mut state = self.inner.lock().unwrap();
                    state.insert(key, msg)
                }
                let resp = Response::SendOk {};
                return runtime.reply(req, resp).await;
            }
            Ok(Request::Poll {}) => {
                let msgs = {
                    let state = self.inner.lock().unwrap();
                    state.poll()
                };
                let resp = Response::PollOk { msgs };
                return runtime.reply(req, resp).await;
            }
            Ok(Request::Commit {}) => todo!(),
            Ok(Request::LimitCommittedOffsets {}) => todo!(),
            _ => {}
        }
        done(runtime, req)
    }
}

#[derive(Default)]
struct State {
    offsets: HashMap<String, usize>,
    committed: HashMap<String, usize>,
    logs: HashMap<String, Vec<Log>>,
}

impl State {
    fn insert(&mut self, key: String, msg: usize) {
        let offset = match self.offsets.get(&key) {
            None => 0,
            Some(offset) => *offset,
        };
        match self.logs.get_mut(&key) {
            None => {
                let key = key.clone();
                let logs = vec![Log(offset, msg)];
                self.logs.insert(key, logs);
            }
            Some(logs) => logs.push(Log(offset, msg)),
        };
        self.offsets.insert(key, offset + 1);
    }

    fn poll(&self) -> HashMap<String, Vec<Log>> {
        self.logs.clone()
    }

    fn commit_offsets(&mut self, key: String, offset: usize) {
        self.committed.insert(key, offset);
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
struct Log(usize, usize);

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Send { key: String, msg: usize },
    Poll {},
    Commit {},
    LimitCommittedOffsets {},
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    SendOk {},
    PollOk { msgs: HashMap<String, Vec<Log>> },
    CommitOk {},
    LimitCommitedOffsetsOk {},
}
