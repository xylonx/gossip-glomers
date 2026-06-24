use std::sync::atomic::{AtomicU64, Ordering};

use gossip_glomers::maelstrom::{
    Serve,
    error::{Error, Result},
    handler::Handler,
    message::MessageId,
    runtime::Runtime,
};
use serde::{Deserialize, Serialize};
use tokio::io::{BufReader, BufWriter};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

#[derive(Debug, Default)]
struct EchoHandler {
    msg_id: AtomicU64,
}

impl Handler for EchoHandler {
    type T = EchoPayload;

    async fn handle(
        &self,
        runtime: &Runtime<Self::T>,
        msg_id: Option<MessageId>,
        payload: Self::T,
    ) -> Result<Option<(Option<MessageId>, Self::T)>> {
        let msg_id = self.msg_id.fetch_add(1, Ordering::AcqRel);
        match payload {
            EchoPayload::Echo { echo } => Ok(Some((
                Some(MessageId::new(msg_id)),
                EchoPayload::EchoOk { echo },
            ))),
            EchoPayload::EchoOk { echo: _ } => Err(Error::MalformedRequest),
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::Layer::new().with_writer(std::io::stdout).pretty())
        .with(EnvFilter::from_default_env())
        .init();

    let serve = Serve::new(EchoHandler::default());
    let reader = BufReader::new(tokio::io::stdin());
    let writer = BufWriter::new(tokio::io::stdout());
    serve.serve(reader, writer).await
}
