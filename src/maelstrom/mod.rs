pub mod error;
pub mod handler;
pub mod message;
pub mod runtime;

use std::cell::OnceCell;

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter},
    sync::mpsc::{self, UnboundedSender},
};
use tracing::{error, info, warn};

use crate::maelstrom::{
    error::{Error, Result},
    handler::Handler,
    message::{Message, MessagePayload},
    runtime::Runtime,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: impl ToString) -> Self {
        Self(id.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub cluster: Vec<NodeId>,
}

pub struct Serve<H: Handler> {
    pub handler: H,

    pub runtime: OnceCell<Runtime<H::T>>,
}

impl<H: Handler> Serve<H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            runtime: OnceCell::new(),
        }
    }

    pub async fn serve<R, W>(&self, reader: BufReader<R>, mut writer: BufWriter<W>)
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let input_handler = async {
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let request = match self.handler.deserialize_request(line.to_string()).await {
                    Ok(msg) => msg,
                    Err(e) => {
                        error!(%line, %e, "Failed to parse message");
                        continue;
                    }
                };

                let request_meta = request.meta();
                let response = match self.handle_request(request, tx.clone()).await {
                    Ok(Some(msg)) => msg,
                    Ok(None) => {
                        continue;
                    }
                    Err(e) => {
                        Message::reply_to(&request_meta, None, MessagePayload::Error(e.into()))
                    }
                };
                if let Err(e) = tx.send(response) {
                    error!(%e, "Failed to write response to writer");
                }
            }
        };

        let output_handler = async {
            while let Some(msg) = rx.recv().await {
                if let Ok(mut response) = self.handler.serialize_response(msg).await {
                    // output by line
                    response.push('\n');
                    writer.write_all(response.as_bytes()).await.unwrap();
                    writer.flush().await.unwrap();
                }
            }
        };

        tokio::select! {
            _ = input_handler => {},
            _ = output_handler => {},
        }
    }

    async fn handle_request(
        &self,
        msg: Message<<H as Handler>::T>,
        tx: UnboundedSender<Message<H::T>>,
    ) -> Result<Option<Message<<H as Handler>::T>>> {
        // First ensure node exists
        let runtime = match self.runtime.get() {
            Some(node) => node,
            None => {
                if let message::MessagePayload::Init(init) = &msg.body.payload {
                    // ensure node_id is inside the cluster
                    if !init.node_ids.contains(&init.node_id) {
                        error!(?init, "node is not inside the nodes for init request");
                        return Err(Error::MalformedRequest);
                    }

                    let node = Node {
                        id: init.node_id.clone(),
                        cluster: init.node_ids.clone(),
                    };

                    match self.runtime.set(Runtime::new(node, tx)) {
                        Ok(_) => {
                            return Ok(Some(Message::reply_to(
                                &msg.meta(),
                                None,
                                MessagePayload::InitOk,
                            )));
                        }
                        Err(n) => {
                            error!(?n.node, "node is already initialized.");
                            return Err(Error::MalformedRequest);
                        }
                    }
                } else {
                    error!("node is uninitialized");
                    return Err(Error::TemporarilyUnavailable);
                }
            }
        };

        info!(?runtime.node, "handle line with node initialized");

        if msg.dest != runtime.node.id {
            warn!(?runtime.node.id, ?msg.dest, "received message to others. Drop it");
            return Err(Error::MalformedRequest);
        }

        if msg.body.in_reply_to.is_some() {
            return runtime.receive(msg).await.and(Ok(None));
        }

        let meta = msg.meta();
        match msg.body.payload {
            message::MessagePayload::Init(_) => {
                error!(?msg, "Unreachable");
                return Err(Error::Crash);
            }
            message::MessagePayload::InitOk => {
                error!(?msg, "Unreachable");
                return Err(Error::Crash);
            }
            message::MessagePayload::Error(_) => {
                warn!(?msg, "Received error message");
                return Err(Error::MalformedRequest);
            }

            message::MessagePayload::Custom(payload) => Ok(self
                .handler
                .handle(runtime, msg.body.msg_id, payload)
                .await?
                .map(|(msg_id, payload)| {
                    Message::reply_to(&meta, msg_id, MessagePayload::Custom(payload))
                })),
        }
    }
}
