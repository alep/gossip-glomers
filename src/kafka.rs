use async_trait::async_trait;

use maelstrom::kv::{lin_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{done, Error, Node, Result, Runtime};
use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
  inner: Arc<Mutex<State>>,
}

impl Handler {
  fn new(runtime: Runtime) -> Self {
    Self {
      s: lin_kv(runtime),
      inner: Arc::new(Mutex::new(State {
        logs: HashMap::new(),
      })),
    }
  }
}

#[async_trait]
impl Node for Handler {
  async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
    let msg: Result<Request> = req.body.as_obj();
    match msg {
      Ok(Request::Send { key, msg }) => {
        let mut offset: usize;
        loop {
          let offset_key = format!("{:}-offset", key);
          let (ctx, _handler) = Context::new();
          offset = self
            .s
            .get(ctx, offset_key.clone())
            .await
            .unwrap_or_else(|_| 0);

          let (ctx, _handler) = Context::new();
          let result: Result<()> = self
            .s
            .cas(ctx, offset_key.clone(), offset, offset + 1, true)
            .await;

          match result {
            Ok(()) => break,
            Err(e) => assert_eq!(
              *e.downcast_ref::<Error>().unwrap(),
              Error::PreconditionFailed
            ),
          };
        }

        let (ctx, _handler) = Context::new();
        let _ = self.s.put(ctx, format!("{}-{}", key, offset), msg).await;

        {
          let mut inner = self.inner.lock().unwrap();
          inner.insert(key, offset);
        }

        let resp = Response::SendOk { offset };
        return runtime.reply(req, resp).await;
      }
      Ok(Request::Poll { offsets }) => {
        let mut msgs = HashMap::<String, Vec<Log>>::new();

        for (key, offset) in offsets {
          let (ctx, _handler) = Context::new();

          let last_offset = self
            .s
            .get(ctx, format!("{}-offset", key))
            .await
            .unwrap_or_else(|_| 0);

          if last_offset == 0 {
            continue;
          }

          let mut logs: Vec<Log> = vec![];
          for idx in offset..last_offset {
            let (ctx, _handler) = Context::new();
            let v = self.s.get(ctx, format!("{}-{}", key, idx)).await.unwrap();
            logs.push(Log(idx, v));
          }
          msgs.insert(key, logs);
        }

        let resp = Response::PollOk { msgs };
        return runtime.reply(req, resp).await;
      }
      Ok(Request::CommitOffsets { offsets }) => {
        for (key, offset) in offsets {
          loop {
            let commit_key = format!("{}-commit", key);
            let (ctx, _handler) = Context::new();

            let commit = self
              .s
              .get(ctx, commit_key.clone())
              .await
              .unwrap_or_else(|_| 0);

            if commit >= offset {
              break;
            }

            let (ctx, _handler) = Context::new();
            let result: Result<()> = self
              .s
              .cas(ctx, commit_key.clone(), commit, offset, true)
              .await;

            match result {
              Ok(()) => break,
              Err(e) => assert_eq!(
                *e.downcast_ref::<Error>().unwrap(),
                Error::PreconditionFailed
              ),
            };
          }
        }

        let resp = Response::CommitOffsetsOk {};
        return runtime.reply(req, resp).await;
      }
      Ok(Request::ListCommittedOffsets { keys }) => {
        let mut offsets = HashMap::<String, usize>::new();
        for key in keys {
          let commit_key = format!("{:}-commit", key);
          let (ctx, _handler) = Context::new();

          let commit = self
            .s
            .get(ctx, commit_key.clone())
            .await
            .unwrap_or_else(|_| 0);
          if commit != 0 {
            offsets.insert(key, commit);
          }
        }
        let resp = Response::ListCommittedOffsetsOk { offsets };
        return runtime.reply(req, resp).await;
      }
      _ => {}
    }
    done(runtime, req)
  }
}

#[derive(Default)]
struct State {
  logs: HashMap<String, usize>,
}

impl State {
  fn insert(&mut self, key: String, offset: usize) {
    self.logs.insert(key, offset);
  }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
struct Log(usize, usize);

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
  Send { key: String, msg: usize },
  Poll { offsets: HashMap<String, usize> },
  CommitOffsets { offsets: HashMap<String, usize> },
  ListCommittedOffsets { keys: Vec<String> },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
  SendOk { offset: usize },
  PollOk { msgs: HashMap<String, Vec<Log>> },
  CommitOffsetsOk {},
  ListCommittedOffsetsOk { offsets: HashMap<String, usize> },
}
