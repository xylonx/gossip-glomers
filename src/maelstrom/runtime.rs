use std::{collections::HashMap, fmt::Debug, sync::Arc};

use tokio::sync::{Mutex, mpsc::UnboundedSender, oneshot};
use tracing::error;

use crate::maelstrom::{
    Node, NodeId,
    error::{Error, Result},
    message::{ErrorPayload, Message, MessageBody, MessageId, MessagePayload},
};

#[derive(Debug, Clone)]
pub struct Runtime<T> {
    pub node: Node,

    rpc: Arc<Mutex<HashMap<MessageId, oneshot::Sender<Message<T>>>>>,

    output: UnboundedSender<Message<T>>,
}

impl<T> Runtime<T>
where
    T: Clone + Debug,
{
    pub fn new(node: Node, output: UnboundedSender<Message<T>>) -> Self {
        Self {
            node,
            rpc: Arc::new(Mutex::new(HashMap::new())),
            output,
        }
    }

    pub async fn receive(&self, msg: Message<T>) -> Result<()> {
        let reply = msg.body.in_reply_to.ok_or(Error::MalformedRequest)?;

        let mut guard = self.rpc.lock().await;
        match guard.remove(&reply) {
            Some(sender) => {
                sender
                    .send(msg.clone())
                    .inspect_err(|e| error!(?e, "Failed to send rpc response"))
                    .unwrap();
            }
            None => {
                error!(?reply, "reply to a unknown message");
                return Err(Error::MalformedRequest);
            }
        };

        Ok(())
    }

    /// call only sends message to to dest without waiting for response.
    pub async fn call(&self, dest: NodeId, payload: T) -> Result<()> {
        let msg = self.call_msg(dest, None, payload);
        self.output
            .send(msg)
            .inspect_err(|e| error!(%e, "Failed to send message"))
            .or(Err(Error::Crash))?;
        Ok(())
    }

    /// rpc sends message to dest and wait for response.
    pub async fn rpc(&self, msg_id: MessageId, dest: NodeId, payload: T) -> Result<T> {
        let msg = self.call_msg(dest, Some(msg_id), payload);
        self.output
            .send(msg)
            .inspect_err(|e| error!(%e, "Failed to send message"))
            .or(Err(Error::Crash))?;

        let (tx, rx) = oneshot::channel();
        {
            let mut guard = self.rpc.lock().await;
            guard.insert(msg_id, tx);
        }
        let response = rx
            .await
            .inspect_err(|e| error!(%e, "Failed to receive rpc response from channel"))
            .or(Err(Error::Crash))?;

        match response.body.payload {
            MessagePayload::Error(ErrorPayload { code, text }) => {
                Err(Error::from_code_message(code, text))
            }
            MessagePayload::Custom(payload) => Ok(payload),
            _ => {
                error!(?response, "Unreachable branch");
                unreachable!();
            }
        }
    }

    fn call_msg(&self, dest: NodeId, msg_id: Option<MessageId>, payload: T) -> Message<T> {
        Message {
            src: self.node.id.clone(),
            dest,
            body: MessageBody {
                msg_id,
                in_reply_to: None,
                payload: super::message::MessagePayload::Custom(payload),
            },
        }
    }
}
