use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use gossip_glomers::maelstrom::{
    NodeId, Serve,
    error::{Error, Result},
    handler::Handler,
    message::{MessageId, MessageMeta},
    runtime::Runtime,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{BufReader, BufWriter},
    sync::{
        OnceCell, RwLock,
        mpsc::{self, UnboundedReceiver, UnboundedSender},
    },
};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BroadcastPayload {
    Broadcast {
        // The value is always an integer and it is unique for each message from Maelstrom.
        message: i64,
    },
    BroadcastOk,

    Read,
    ReadOk {
        messages: Vec<i64>,
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
    counter: Arc<AtomicU64>,

    messages: RwLock<HashSet<i64>>,

    broadcast: OnceCell<Vec<(NodeId, UnboundedSender<i64>)>>,
}

async fn propagate(
    counter: Arc<AtomicU64>,
    runtime: Runtime<BroadcastPayload>,
    node_id: NodeId,
    mut rx: UnboundedReceiver<i64>,
) {
    while let Some(data) = rx.recv().await {
        let msg_id = MessageId::new(counter.fetch_add(1, Ordering::AcqRel));

        loop {
            let payload = BroadcastPayload::Broadcast { message: data };
            info!(?node_id, %data, "broadcast value");
            match runtime.rpc(msg_id, node_id.clone(), payload).await {
                Ok(_) => {
                    break;
                }
                Err(e) => {
                    info!(?node_id, %data, %e, "Failed to broadcast value");
                    // TODO(xylonx): expotential backoff?
                }
            }
        }
    }
}

impl Handler for BroadcastHandler {
    type T = BroadcastPayload;

    async fn handle(
        &self,
        runtime: &Runtime<Self::T>,
        meta: MessageMeta,
        payload: Self::T,
    ) -> Result<Option<(Option<MessageId>, Self::T)>> {
        let boradcast = self
            .broadcast
            .get_or_init(|| async {
                runtime
                    .node
                    .cluster
                    .iter()
                    .filter(|&n| n != &runtime.node.id)
                    .map(|node_id| {
                        let (tx, rx) = mpsc::unbounded_channel();
                        tokio::spawn({
                            let runtime2 = runtime.clone();
                            let node_id2 = node_id.clone();
                            let counter2 = self.counter.clone();
                            async move { propagate(counter2, runtime2, node_id2, rx).await }
                        });
                        (node_id.clone(), tx)
                    })
                    .collect()
            })
            .await;

        let msg_id = MessageId::new(self.counter.fetch_add(1, Ordering::AcqRel));
        match &payload {
            BroadcastPayload::Broadcast { message } => {
                let mut guard = self.messages.write().await;
                if guard.insert(*message) {
                    // Propagate data to other nodes
                    boradcast
                        .iter()
                        .filter(|(nid, _)| nid != &meta.src)
                        .for_each(|(_, tx)| {
                            tx.send(*message).unwrap();
                        });
                }

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
        .with(fmt::Layer::new().with_writer(std::io::stderr))
        .with(EnvFilter::from_default_env())
        .init();

    let serve = Serve::new(BroadcastHandler::default());
    let reader = BufReader::new(tokio::io::stdin());
    let writer = BufWriter::new(tokio::io::stdout());
    serve.serve(reader, writer).await
}
