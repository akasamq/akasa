use mqtt_proto::{TopicFilter, TopicName};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct User {
    pub username: String,
    pub superuser: bool,
}

impl User {
    pub fn new(username: &str) -> Self {
        Self {
            username: username.to_string(),
            ..Default::default()
        }
    }
    pub fn super_user(username: &str) -> Self {
        Self {
            username: username.to_string(),
            superuser: true,
        }
    }

    pub fn allow_read(&self, _tf: TopicFilter) -> bool {
        // TODO
        true
    }
    pub fn allow_write(&self, _topic: TopicName) -> bool {
        // TODO
        true
    }
}
