use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicU64, Ordering},
};

use gossip_glomers::maelstrom::{
    NodeId, Serve,
    error::{Error, Result},
    handler::Handler,
    message::MessageId,
    runtime::Runtime,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{BufReader, BufWriter},
    sync::RwLock,
};
use tracing::error;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BroadcastPayload {
    Broadcast {
        // The value is always an integer and it is unique for each message from Maelstrom.
        message: u64,
    },
    BroadcastOk,

    Read,
    ReadOk {
        messages: Vec<u64>,
    },

    /// Maelstrom has multiple topologies available or you can ignore this message
    /// and make your own topology from the list of nodes.
    /// All nodes can communicate with each other regardless of the topology passed in.
    Topology {
        topology: HashMap<NodeId, Vec<NodeId>>,
    },
    TopologyOk,
}

/// The node will need to store the set of integer values that it sees from broadcast
/// messages so that they can be returned later via the read message RPC.
///
///
#[derive(Debug, Default)]
struct BroadcastHandler {
    msg_id: AtomicU64,

    messages: RwLock<HashSet<u64>>,
}

impl Handler for BroadcastHandler {
    type T = BroadcastPayload;

    async fn handle(
        &self,
        _runtime: &Runtime<Self::T>,
        _: Option<MessageId>,
        payload: Self::T,
    ) -> Result<Option<(Option<MessageId>, Self::T)>> {
        let msg_id = MessageId::new(self.msg_id.fetch_add(1, Ordering::AcqRel));
        match &payload {
            BroadcastPayload::Broadcast { message } => {
                let mut guard = self.messages.write().await;
                guard.insert(*message);
                Ok(Some((Some(msg_id), BroadcastPayload::BroadcastOk)))
            }
            BroadcastPayload::Read => {
                let guard = self.messages.read().await;
                let data = guard.iter().map(Clone::clone).collect();
                Ok(Some((
                    Some(msg_id),
                    BroadcastPayload::ReadOk { messages: data },
                )))
            }
            BroadcastPayload::Topology { topology: _ } => {
                Ok(Some((Some(msg_id), BroadcastPayload::TopologyOk)))
            }

            _ => {
                error!(?payload, "Unreachable payload");
                Err(Error::Crash)
            }
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::Layer::new().with_writer(std::io::stdout).pretty())
        .with(EnvFilter::from_default_env())
        .init();

    let serve = Serve::new(BroadcastHandler::default());
    let reader = BufReader::new(tokio::io::stdin());
    let writer = BufWriter::new(tokio::io::stdout());
    serve.serve(reader, writer).await
}
