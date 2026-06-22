pub mod error;
pub mod handler;
pub mod message;

use std::{cell::OnceCell, collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter},
    sync::{Mutex, oneshot},
};
use tracing::{error, info, warn};

use crate::maelstrom::{
    error::{Error, Result},
    handler::Handler,
    message::{Message, MessageId, MessagePayload},
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

pub struct Runtime<H: Handler> {
    pub node: OnceCell<Node>,
    pub handler: H,

    // rpc oneshot
    pub rpc: Arc<Mutex<HashMap<MessageId, oneshot::Sender<Message<H::T>>>>>,
}

impl<H: Handler> Runtime<H> {
    pub fn new(handler: H) -> Self {
        Self {
            node: OnceCell::new(),
            handler,
            rpc: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn serve<R, W>(&self, reader: BufReader<R>, mut writer: BufWriter<W>)
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let mut lines = reader.lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let msg = match self.handler.deserialize_request(line.to_string()).await {
                Ok(msg) => msg,
                Err(e) => {
                    error!(%line, %e, "Failed to parse message");
                    continue;
                }
            };
            let msg = match self.handle_request(&msg).await {
                Ok(Some(msg)) => msg,
                Ok(None) => {
                    continue;
                }
                Err(e) => Message::reply_to(&msg, None, MessagePayload::Error(e.into())),
            };
            if let Ok(mut response) = self.handler.serialize_response(msg).await {
                // output by line
                response.push('\n');
                if let Err(e) = writer.write_all(response.as_bytes()).await {
                    error!(%e, %response, "Failed to write response to writer");
                }
            }
        }
    }

    async fn handle_request(
        &self,
        msg: &Message<<H as Handler>::T>,
    ) -> Result<Option<Message<<H as Handler>::T>>> {
        // First ensure node exists
        let node = match self.node.get() {
            Some(node) => node,
            None => {
                if let message::MessagePayload::Init(init) = &msg.body.payload {
                    // TODO(xylonx): the clone() here seems unnecessary. Message::reply_to only replies on some metadata.

                    match self.node.set(Node {
                        id: init.node_id.clone(),
                        cluster: init.node_ids.clone(),
                    }) {
                        Ok(_) => {
                            return Ok(Some(Message::reply_to(msg, None, MessagePayload::InitOk)));
                        }
                        Err(n) => {
                            error!(?n, "node is already initialized.");
                            return Err(Error::MalformedRequest);
                        }
                    }
                } else {
                    error!("node is uninitialized");
                    return Err(Error::TemporarilyUnavailable);
                }
            }
        };

        info!(?node, "handle line with node initialized");

        if msg.dest != node.id {
            warn!(?node.id, ?msg.dest, "received message to others. Drop it");
            return Err(Error::MalformedRequest);
        }

        match &msg.body.payload {
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

            message::MessagePayload::Custom(payload) => {
                if let Some(reply_to) = msg.body.in_reply_to {
                    let mut guard = self.rpc.lock().await;
                    match guard.remove(&reply_to) {
                        Some(sender) => {
                            sender
                                .send(msg.clone())
                                .inspect_err(|e| error!(?e, "Failed to send rpc response"))
                                .unwrap();
                        }
                        None => {
                            error!(?reply_to, "reply to a unknown message");
                            return Err(Error::MalformedRequest);
                        }
                    };
                }

                Ok(self
                    .handler
                    .handle(&msg.src, msg.body.msg_id, msg.body.in_reply_to, payload)
                    .await?
                    .map(|(msg_id, payload)| {
                        Message::reply_to(&msg, msg_id, MessagePayload::Custom(payload))
                    }))
            }
        }
    }
}
