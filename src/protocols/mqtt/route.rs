use std::ops::Deref;
use std::sync::Arc;

use dashmap::DashMap;
use hashbrown::HashMap;
use mqtt_proto::{QoS, TopicFilter, TopicName, LEVEL_SEP, MATCH_ALL_STR, MATCH_ONE_STR};
use parking_lot::RwLock;

use crate::state::ClientId;

pub struct RouteTable {
    nodes: DashMap<String, RouteNode>,
}

struct RouteNode {
    content: Arc<RwLock<RouteContent>>,
    nodes: Arc<DashMap<String, RouteNode>>,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct RouteContent {
    /// Returned RouteContent always have topic_filter
    pub topic_filter: Option<TopicFilter>,
    pub clients: HashMap<ClientId, QoS>,
    pub groups: HashMap<String, SharedClients>,
}

#[derive(Eq, PartialEq, Debug, Clone, Default)]
pub struct SharedClients {
    items: Vec<(ClientId, QoS)>,
    index: HashMap<ClientId, usize>,
}

impl SharedClients {
    pub fn get_one_by_u64(&self, number: u64) -> (ClientId, QoS) {
        // Empty SharedClients MUST already removed from parent data structure immediately.
        debug_assert!(!self.items.is_empty());
        let idx = number as usize % self.items.len();
        self.items[idx]
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn insert(&mut self, item: (ClientId, QoS)) {
        if let Some(idx) = self.index.get(&item.0) {
            self.items[*idx].1 = item.1;
        } else {
            self.index.insert(item.0, self.items.len());
            self.items.push(item);
        }
    }

    fn remove(&mut self, item_key: &ClientId) {
        if let Some(idx) = self.index.remove(item_key) {
            if self.items.len() == 1 {
                self.items.clear();
            } else {
                let last_client_id = self.items.last().expect("shared items").0;
                self.items.swap_remove(idx);
                self.index.insert(last_client_id, idx);
                if self.items.capacity() >= 16 && self.items.capacity() >= (self.items.len() << 2) {
                    self.items.shrink_to(self.items.len() << 1);
                }
            }
        }
    }
}

impl RouteTable {
    pub fn new() -> RouteTable {
        RouteTable {
            nodes: DashMap::new(),
        }
    }

    pub fn get_matches(&self, topic_name: &TopicName) -> Vec<Arc<RwLock<RouteContent>>> {
        let (topic_item, rest_items) = split_topic(topic_name.deref());
        let mut filters = Vec::new();

        if let Some(pair) = self.nodes.get(topic_item) {
            pair.value()
                .get_matches(topic_item, rest_items, &mut filters);
        }
        // [MQTT-4.7.2-1] The Server MUST NOT match Topic Filters starting with a
        // wildcard character (# or +) with Topic Names beginning with a $ character
        if !topic_name.starts_with('$') {
            for item in [MATCH_ALL_STR, MATCH_ONE_STR] {
                if let Some(pair) = self.nodes.get(item) {
                    pair.value().get_matches(item, rest_items, &mut filters);
                }
            }
        }
        filters
    }

    pub fn subscribe(&self, topic_filter: &TopicFilter, id: ClientId, qos: QoS) {
        if let Some((shared_group_name, shared_filter)) = topic_filter.shared_info() {
            self.subscribe_shared(
                &TopicFilter::try_from(shared_filter.to_owned()).expect("shared filter"),
                id,
                qos,
                Some(shared_group_name.to_owned()),
            );
        } else {
            self.subscribe_shared(topic_filter, id, qos, None);
        }
    }
    fn subscribe_shared(
        &self,
        topic_filter: &TopicFilter,
        id: ClientId,
        qos: QoS,
        group: Option<String>,
    ) {
        let (filter_item, rest_items) = split_topic(topic_filter.deref());
        // Since subscribe is not an frequent action, string clone here is acceptable.
        self.nodes
            .entry(filter_item.to_string())
            .or_insert_with(RouteNode::new)
            .insert(topic_filter, rest_items, id, qos, group);
    }

    pub fn unsubscribe(&self, topic_filter: &TopicFilter, id: ClientId) {
        if let Some((shared_group_name, shared_filter)) = topic_filter.shared_info() {
            self.unsubscribe_shared(
                &TopicFilter::try_from(shared_filter.to_owned()).expect("shared filter"),
                id,
                Some(shared_group_name),
            );
        } else {
            self.unsubscribe_shared(topic_filter, id, None);
        }
    }
    fn unsubscribe_shared(&self, topic_filter: &TopicFilter, id: ClientId, group: Option<&str>) {
        let (filter_item, rest_items) = split_topic(topic_filter.deref());
        // bool variable is for resolve dead lock of access `self.nodes`
        let mut remove_node = false;
        if let Some(mut pair) = self.nodes.get_mut(filter_item) {
            remove_node = pair.value_mut().remove(rest_items, id, group);
        }
        if remove_node {
            self.nodes.remove(filter_item);
        }
    }
}

impl RouteNode {
    fn new() -> RouteNode {
        RouteNode {
            content: Arc::new(RwLock::new(RouteContent {
                topic_filter: None,
                clients: HashMap::new(),
                groups: HashMap::new(),
            })),
            nodes: Arc::new(DashMap::new()),
        }
    }

    fn get_matches(
        &self,
        prev_item: &str,
        topic_items: Option<&str>,
        filters: &mut Vec<Arc<RwLock<RouteContent>>>,
    ) {
        if prev_item == MATCH_ALL_STR {
            if !self.content.read().is_empty() {
                filters.push(Arc::clone(&self.content));
            }
        } else if let Some(topic_items) = topic_items {
            let (topic_item, rest_items) = split_topic(topic_items);
            for item in [topic_item, MATCH_ALL_STR, MATCH_ONE_STR] {
                if let Some(pair) = self.nodes.get(item) {
                    pair.value().get_matches(item, rest_items, filters);
                }
            }
        } else {
            if !self.content.read().is_empty() {
                filters.push(Arc::clone(&self.content));
            }

            // Topic name "abc" will match topic filter "abc/#", since "#" also represent parent level.
            if let Some(pair) = self.nodes.get(MATCH_ALL_STR) {
                if !pair.value().content.read().is_empty() {
                    filters.push(Arc::clone(&pair.value().content));
                }
            }
        }
    }

    fn insert(
        &self,
        topic_filter: &TopicFilter,
        filter_items: Option<&str>,
        id: ClientId,
        qos: QoS,
        group: Option<String>,
    ) {
        if let Some(filter_items) = filter_items {
            let (filter_item, rest_items) = split_topic(filter_items);
            self.nodes
                .entry(filter_item.to_string())
                .or_insert_with(RouteNode::new)
                .insert(topic_filter, rest_items, id, qos, group);
        } else {
            let mut content = self.content.write();
            if content.topic_filter.is_none() {
                content.topic_filter = Some(topic_filter.clone());
            }
            content.clients.insert(id, qos);
            if let Some(name) = group {
                content
                    .groups
                    .entry(name)
                    .or_insert_with(SharedClients::default)
                    .insert((id, qos));
            }
        }
    }

    fn remove(&self, filter_items: Option<&str>, id: ClientId, group: Option<&str>) -> bool {
        if let Some(filter_items) = filter_items {
            let (filter_item, rest_items) = split_topic(filter_items);
            // bool variables are for resolve dead lock of access `self.nodes`
            let mut remove_node = false;
            if let Some(mut pair) = self.nodes.get_mut(filter_item) {
                if pair.value_mut().remove(rest_items, id, group) {
                    remove_node = true;
                }
            }
            let remove_parent = if remove_node {
                self.nodes.remove(filter_item);
                self.content.read().is_empty() && self.nodes.is_empty()
            } else {
                false
            };
            return remove_parent;
        } else {
            let mut content = self.content.write();
            content.clients.remove(&id);
            if let Some(name) = group {
                if let Some(shared_clients) = content.groups.get_mut(name) {
                    shared_clients.remove(&id);
                    if shared_clients.is_empty() {
                        content.groups.remove(name);
                    }
                }
            }
            if content.is_empty() {
                content.topic_filter = None;
                if self.nodes.is_empty() {
                    return true;
                }
            }
        }
        false
    }
}

impl RouteContent {
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty() && self.groups.is_empty()
    }
}

#[inline]
pub(crate) fn split_topic(topic: &str) -> (&str, Option<&str>) {
    if let Some((head, rest)) = topic.split_once(LEVEL_SEP) {
        (head, Some(rest))
    } else {
        (topic, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hashbrown::HashMap;
    use mqtt_proto::TopicName;
    use Action::*;

    impl RouteContent {
        fn to_simple(&self) -> (Option<String>, HashMap<ClientId, QoS>) {
            let filter = self.topic_filter.as_ref().map(|v| v.to_string());
            let clients = self.clients.clone();
            (filter, clients)
        }
    }

    #[derive(Clone)]
    enum Action<'a> {
        // (TopicFilter, ClientId)
        Sub(&'a str, u64),
        // (TopicFilter, ClientId)
        UnSub(&'a str, u64),
        // (TopicName, Vec<(TopicFilter, Vec<ClientId>)>)
        Query(&'a str, Vec<(&'a str, Vec<u64>)>),
    }

    fn run_actions(actions: &[Action]) {
        let table = RouteTable::new();
        for action in actions {
            match action.clone() {
                Sub(filter, id) => {
                    table.subscribe(
                        &TopicFilter::try_from(filter.to_owned()).unwrap(),
                        ClientId::new(id),
                        QoS::Level0,
                    );
                }
                UnSub(filter, id) => {
                    table.unsubscribe(
                        &TopicFilter::try_from(filter.to_owned()).unwrap(),
                        ClientId::new(id),
                    );
                }
                Query(name, expected) => {
                    let mut expected_items: Vec<(String, HashMap<ClientId, QoS>)> = expected
                        .into_iter()
                        .map(|(k, v)| {
                            (
                                k.to_string(),
                                v.into_iter()
                                    .map(|v| (ClientId::new(v), QoS::Level0))
                                    .collect::<HashMap<_, _>>(),
                            )
                        })
                        .collect();
                    let mut items: Vec<(String, HashMap<ClientId, QoS>)> = table
                        .get_matches(&TopicName::try_from(name.to_owned()).unwrap())
                        .into_iter()
                        .map(|content| {
                            let (k, v) = content.read().to_simple();
                            (k.unwrap(), v)
                        })
                        .collect();
                    items.sort_by_key(|(k, _)| k.to_string());
                    expected_items.sort_by_key(|(k, _)| k.to_string());
                    assert_eq!(items, expected_items);
                }
            }
        }
    }

    #[test]
    fn test_one_level() {
        run_actions(&[Sub("abc", 3), Query("abc", vec![("abc", vec![3])])]);
        run_actions(&[Sub("abc", 3), Query("xyz", vec![])]);
        run_actions(&[Sub("abc", 3), UnSub("abc", 3), Query("abc", vec![])]);
        run_actions(&[
            Sub("abc", 3),
            UnSub("abc", 3),
            Sub("abc", 3),
            Query("abc", vec![("abc", vec![3])]),
        ]);

        run_actions(&[Sub("abc/#", 3), UnSub("abc/#", 3)]);
        run_actions(&[Sub("+", 3), Query("abc", vec![("+", vec![3])])]);
        run_actions(&[Sub("#", 3), Query("abc", vec![("#", vec![3])])]);

        run_actions(&[
            Sub("abc", 3),
            Sub("abc", 4),
            Sub("ijk", 5),
            Sub("+", 99),
            Query("abc", vec![("abc", vec![3, 4]), ("+", vec![99])]),
            Query("ijk", vec![("ijk", vec![5]), ("+", vec![99])]),
        ])
    }

    #[test]
    fn test_multi_levels() {
        run_actions(&[
            Sub("abc", 3),
            Sub("abc/ijk", 4),
            Sub("abc/+", 5),
            Sub("abc/#", 6),
            Query("abc", vec![("abc", vec![3]), ("abc/#", vec![6])]),
            Query(
                "abc/ijk",
                vec![("abc/ijk", vec![4]), ("abc/+", vec![5]), ("abc/#", vec![6])],
            ),
            Query("abc/xyz", vec![("abc/+", vec![5]), ("abc/#", vec![6])]),
            Query("xyz/ijk", vec![]),
        ]);

        run_actions(&[
            Sub("abc/+/ijk/+/xyz", 3),
            Sub("abc/1/ijk/+/xyz", 4),
            Query("abc/1/ijk/2/xyz/3", vec![]),
            Query("abc/1/ijk/xyz", vec![]),
            Query("abc/ijk/xyz", vec![]),
            Query(
                "abc/1/ijk/2/xyz",
                vec![("abc/+/ijk/+/xyz", vec![3]), ("abc/1/ijk/+/xyz", vec![4])],
            ),
            Query("abc/8/ijk/9/xyz", vec![("abc/+/ijk/+/xyz", vec![3])]),
        ]);
    }

    #[test]
    fn test_special_dollor_prefix() {
        run_actions(&[
            Sub("$abc/+", 2),
            Sub("+/+", 3),
            Sub("#", 4),
            Sub("$abc/#", 5),
            Query("$abc/dev", vec![("$abc/+", vec![2]), ("$abc/#", vec![5])]),
            Query("$abc/", vec![("$abc/+", vec![2]), ("$abc/#", vec![5])]),
            Query("$abc", vec![("$abc/#", vec![5])]),
        ]);
    }
}
