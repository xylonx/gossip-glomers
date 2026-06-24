use std::{
    cell::OnceCell,
    sync::atomic::{AtomicU64, Ordering},
};

use gossip_glomers::maelstrom::{
    Serve,
    error::Result,
    handler::Handler,
    message::{MessageId, MessageMeta},
    runtime::Runtime,
};
use serde::{Deserialize, Serialize};
use tokio::io::{BufReader, BufWriter};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum UniqueIdPayload {
    Generate,
    GenerateOk { id: u64 },
}

/// The implementation splits u64 value space into partitions where each node own a partition.
///
/// [ n1 ][ n2 ][ ... ][ n_{n} ]
/// 0                          U64::MAX
///
/// So it allows availability even network partitions as there is no communication among nodes.
/// But it is NOT Fault-Tolerant as the counter will **reset** after restart the node.
#[derive(Debug)]
struct UniqueIdHandler {
    msg_id: AtomicU64,

    counter: OnceCell<AtomicU64>,
}

impl UniqueIdHandler {
    pub fn new() -> Self {
        Self {
            msg_id: AtomicU64::new(0),
            counter: OnceCell::new(),
        }
    }
}

impl Handler for UniqueIdHandler {
    type T = UniqueIdPayload;

    async fn handle(
        &self,
        runtime: &Runtime<Self::T>,
        _: MessageMeta,
        _: Self::T,
    ) -> Result<Option<(Option<MessageId>, Self::T)>> {
        let next_id = self.counter.get_or_init(|| {
            let offset = runtime
                .node
                .cluster
                .iter()
                .position(|r| r == &runtime.node.id)
                .unwrap(); // node must be in cluster. It is SAFE to unwrap here

            let cluster_size = runtime.node.cluster.len();

            let partition_size = u64::MAX / (cluster_size as u64);

            AtomicU64::new(partition_size * (offset as u64))
        });

        let id = next_id.fetch_add(1, Ordering::AcqRel);
        let msg_id = MessageId::new(self.msg_id.fetch_add(1, Ordering::AcqRel));

        Ok(Some((Some(msg_id), UniqueIdPayload::GenerateOk { id })))
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::Layer::new().with_writer(std::io::stdout).pretty())
        .with(EnvFilter::from_default_env())
        .init();

    let serve = Serve::new(UniqueIdHandler::new());
    let reader = BufReader::new(tokio::io::stdin());
    let writer = BufWriter::new(tokio::io::stdout());
    serve.serve(reader, writer).await
}
