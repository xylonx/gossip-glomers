use std::fmt::Debug;

use serde::{Serialize, de::DeserializeOwned};
use tracing::error;

use crate::maelstrom::{
    NodeId,
    error::{Error, Result},
    message::{Message, MessageId},
};

pub trait Handler {
    type T: Debug + Clone + Serialize + DeserializeOwned + Send + 'static;

    fn deserialize_request(&self, msg: String) -> impl Future<Output = Result<Message<Self::T>>> {
        async move {
            tokio::task::spawn_blocking(move || serde_json::from_str(&msg))
                .await
                .inspect_err(|e| error!(%e, "Failed to spawn_blocking json deserialization"))
                .map_err(|_| Error::Crash)?
                .inspect_err(|e| error!(%e, "Failed to deserialize request"))
                .map_err(|_| Error::Crash)
        }
    }

    fn serialize_response(&self, msg: Message<Self::T>) -> impl Future<Output = Result<String>> {
        async move {
            tokio::task::spawn_blocking(move || serde_json::to_string(&msg))
                .await
                .inspect_err(|e| error!(%e, "Failed to spawn_blocking json serialization"))
                .map_err(|_| Error::Crash)?
                .inspect_err(|e| error!(%e, "Failed to serialize response"))
                .map_err(|_| Error::Crash)
        }
    }

    fn handle(
        &self,
        src: &NodeId,
        msg_id: Option<MessageId>,
        in_reply_to: Option<MessageId>,
        payload: &Self::T,
    ) -> impl Future<Output = Result<Option<(Option<MessageId>, Self::T)>>>;
}
