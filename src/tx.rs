use async_trait::async_trait;
use maelstrom::kv::{lin_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::fmt::{Display, Formatter as FmtFormatter, Result as FmtResult};
use std::result::Result as StdResult;
use std::str::FromStr;
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
        Self { s: lin_kv(runtime) }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Txn { txn }) => {
                let mut result_txn: Vec<Op> = vec![];
                for op in txn {
                    let value = match op {
                        Op(OpType::Read, key, _) => {
                            let (ctx, _handler) = Context::new();
                            self.s.get(ctx, key.to_string()).await.ok()
                        }
                        Op(OpType::Write, key, value) => {
                            let (ctx, _handler) = Context::new();
                            self.s.put(ctx, key.to_string(), value).await.ok();
                            value
                        }
                    };
                    result_txn.push(Op(op.0, op.1, value));
                }
                let response = Response::TxnOk { txn: result_txn };
                return runtime.reply(req, response).await;
            }
            _ => {}
        }
        done(runtime, req)
    }
}

#[derive(Serialize, Deserialize)]
enum OpType {
    Read,
    Write,
}

impl Display for OpType {
    fn fmt(&self, f: &mut FmtFormatter) -> FmtResult {
        match self {
            OpType::Read => write!(f, "r"),
            OpType::Write => write!(f, "w"),
        }
    }
}

struct ParseOpTypeError;

impl Display for ParseOpTypeError {
    fn fmt(&self, f: &mut FmtFormatter) -> FmtResult {
        write!(f, "Operation type error")
    }
}

impl FromStr for OpType {
    type Err = ParseOpTypeError;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        match s {
            "r" => Ok(Self::Read),
            "w" => Ok(Self::Write),
            _ => Err(ParseOpTypeError),
        }
    }
}

#[serde_as]
#[derive(Serialize, Deserialize)]
struct Op(
    #[serde_as(as = "DisplayFromStr")] OpType,
    usize,
    Option<usize>,
);

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Txn { txn: Vec<Op> },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    TxnOk { txn: Vec<Op> },
}
