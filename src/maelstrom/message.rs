use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::maelstrom::{NodeId, error::Error};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(u64);

impl MessageId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

impl From<u64> for MessageId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message<T> {
    /// src identifies the node this message came from
    pub src: NodeId,

    // dest identifies the node this message is to
    pub dest: NodeId,

    // body identifies the payload of the message
    pub body: MessageBody<T>,
}

pub struct MessageMeta {
    pub src: NodeId,
    pub dest: NodeId,
    pub msg_id: Option<MessageId>,
    pub in_reply_to: Option<MessageId>,
}

impl<T> Message<T> {
    pub fn meta(&self) -> MessageMeta {
        MessageMeta {
            src: self.src.clone(),
            dest: self.dest.clone(),
            msg_id: self.body.msg_id,
            in_reply_to: self.body.in_reply_to,
        }
    }

    pub fn reply_to(
        source: &MessageMeta,
        msg_id: Option<MessageId>,
        payload: MessagePayload<T>,
    ) -> Self {
        Self {
            src: source.dest.clone(),
            dest: source.src.clone(),
            body: MessageBody {
                msg_id,
                in_reply_to: source.msg_id.clone(),
                payload,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageBody<T> {
    pub msg_id: Option<MessageId>,
    pub in_reply_to: Option<MessageId>,
    #[serde(flatten)]
    pub payload: MessagePayload<T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessagePayload<T> {
    Init(InitPayload),
    InitOk,

    Error(ErrorPayload),

    #[serde(untagged)]
    Custom(T),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitPayload {
    pub node_id: NodeId,
    pub node_ids: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorPayload {
    pub code: i32,
    pub text: String,
}

impl Into<ErrorPayload> for Error {
    fn into(self) -> ErrorPayload {
        ErrorPayload {
            code: self.code(),
            text: self.description().to_owned(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_error() {
        let err = Message {
            src: NodeId::new("n0"),
            dest: NodeId::new("n1"),
            body: MessageBody {
                msg_id: None,
                in_reply_to: None,
                payload: MessagePayload::<()>::Error(Error::Timeout.into()),
            },
        };
        let msg_str = serde_json::to_string(&err).unwrap();
        let msg = serde_json::from_str(&msg_str).unwrap();
        assert_eq!(err, msg);
    }

    #[test]
    fn test_custom_payload() {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(tag = "type", rename_all = "snake_case")]
        pub enum CustomPayload {
            Request(CustomRequest),
            Response,
        }
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        pub struct CustomRequest {
            data: String,
        }

        let data = Message {
            src: NodeId::new("n0"),
            dest: NodeId::new("n1"),
            body: MessageBody {
                msg_id: None,
                in_reply_to: None,
                payload: MessagePayload::Custom(CustomPayload::Request(CustomRequest {
                    data: "data".to_string(),
                })),
            },
        };

        let msg = serde_json::to_string(&data).unwrap();
        let raw = r#"{"src":"n0","dest":"n1","body":{"msg_id":null,"in_reply_to":null,"type":"request","data":"data"}}"#;
        assert_eq!(msg, raw);
        let de_data = serde_json::from_str(&raw).unwrap();
        assert_eq!(data, de_data);
    }
}
