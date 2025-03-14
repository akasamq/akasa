use mqtt_proto::{TopicFilter, TopicName};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct User {
    pub name: String,
    pub superuser: bool,
}

impl User {
    pub fn new(name: String) -> Self {
        Self {
            name,
            ..Default::default()
        }
    }

    pub fn new_superuser(name: String) -> Self {
        Self {
            name,
            superuser: true,
            ..Default::default()
        }
    }

    pub fn allow_subscribe(&self, _tf: &TopicFilter) -> bool {
        // TODO
        true
    }

    pub fn allow_publish(&self, _topic: &TopicName) -> bool {
        // TODO
        true
    }
}
