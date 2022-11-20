use std::ops::Deref;
use std::os::unix::io::RawFd;
use std::sync::Arc;

use dashmap::DashMap;
use hashbrown::HashSet;
use mqtt::{TopicFilter, TopicFilterRef, TopicNameRef};
use parking_lot::RwLock;

const MATCH_ALL: &str = "#";
const MATCH_ONE: &str = "+";
const LEVEL_SEP: char = '/';

pub struct RouteTable {
    nodes: Arc<DashMap<String, RouteNode>>,
}

struct RouteNode {
    content: Arc<RwLock<NodeContent>>,
    nodes: Arc<DashMap<String, RouteNode>>,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct NodeContent {
    /// Returned NodeContent always have topic_filter
    // I wonder if `topic_filter` field is worth saving, since it cost quite some memory.
    pub topic_filter: Option<TopicFilter>,
    pub clients: HashSet<RawFd>,
}

impl RouteTable {
    pub fn new() -> RouteTable {
        RouteTable {
            nodes: Arc::new(DashMap::new()),
        }
    }

    pub fn get_matches(&self, topic_name: &TopicNameRef) -> Vec<Arc<RwLock<NodeContent>>> {
        let (topic_item, rest_items) = split_topic(topic_name.deref());
        let mut filters = Vec::new();
        for item in [topic_item, MATCH_ALL, MATCH_ONE] {
            if let Some(pair) = self.nodes.get(item) {
                pair.value().get_matches(item, rest_items, &mut filters);
            }
        }
        filters
    }

    pub fn subscribe(&self, topic_filter: &TopicFilterRef, fd: RawFd) {
        let (filter_item, rest_items) = split_topic(topic_filter.deref());
        // Since subscribe is not an frequent action, string clone here is acceptable.
        self.nodes
            .entry(filter_item.to_string())
            .or_insert_with(RouteNode::new)
            .insert(topic_filter, rest_items, fd);
    }

    pub fn unsubscribe(&self, topic_filter: &TopicFilterRef, fd: RawFd) {
        let (filter_item, rest_items) = split_topic(topic_filter.deref());
        // bool variable is for resolve dead lock of access `self.nodes`
        let mut remove_node = false;
        if let Some(mut pair) = self.nodes.get_mut(filter_item) {
            remove_node = pair.value_mut().remove(rest_items, fd);
        }
        if remove_node {
            self.nodes.remove(filter_item);
        }
    }
}

impl RouteNode {
    fn new() -> RouteNode {
        RouteNode {
            content: Arc::new(RwLock::new(NodeContent {
                topic_filter: None,
                clients: HashSet::new(),
            })),
            nodes: Arc::new(DashMap::new()),
        }
    }

    fn get_matches(
        &self,
        prev_item: &str,
        topic_items: Option<&str>,
        filters: &mut Vec<Arc<RwLock<NodeContent>>>,
    ) {
        if prev_item == MATCH_ALL {
            if !self.content.read().is_empty() {
                filters.push(Arc::clone(&self.content));
            }
        } else if let Some(topic_items) = topic_items {
            let (topic_item, rest_items) = split_topic(topic_items);
            for item in [topic_item, MATCH_ALL, MATCH_ONE] {
                if let Some(pair) = self.nodes.get(item) {
                    pair.value().get_matches(item, rest_items, filters);
                }
            }
        } else if !self.content.read().is_empty() {
            filters.push(Arc::clone(&self.content));
        }
    }

    fn insert(&self, topic_filter: &TopicFilterRef, filter_items: Option<&str>, fd: RawFd) {
        if let Some(filter_items) = filter_items {
            let (filter_item, rest_items) = split_topic(filter_items);
            self.nodes
                .entry(filter_item.to_string())
                .or_insert_with(RouteNode::new)
                .insert(topic_filter, rest_items, fd);
        } else {
            let mut content = self.content.write();
            if content.topic_filter.is_none() {
                content.topic_filter =
                    Some(unsafe { TopicFilter::new_unchecked(topic_filter.deref()) });
            }
            content.clients.insert(fd);
        }
    }

    fn remove(&self, filter_items: Option<&str>, fd: RawFd) -> bool {
        if let Some(filter_items) = filter_items {
            let (filter_item, rest_items) = split_topic(filter_items);
            // bool variables are for resolve dead lock of access `self.nodes`
            let mut remove_node = false;
            let mut remove_parent = false;
            if let Some(mut pair) = self.nodes.get_mut(filter_item) {
                if pair.value_mut().remove(rest_items, fd) {
                    remove_node = true;
                    remove_parent = self.content.read().is_empty() && self.nodes.is_empty();
                }
            }
            if remove_node {
                self.nodes.remove(filter_item);
            }
            return remove_parent;
        } else {
            let mut content = self.content.write();
            content.clients.remove(&fd);
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

#[inline]
fn split_topic(topic: &str) -> (&str, Option<&str>) {
    if let Some((head, rest)) = topic.split_once(LEVEL_SEP) {
        (head, Some(rest))
    } else {
        (topic, None)
    }
}

impl NodeContent {
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    // TODO: This function is for assert the RouteTable info
    #[cfg(test)]
    fn to_simple(&self) -> (Option<String>, HashSet<RawFd>) {
        let filter = self.topic_filter.as_ref().map(|v| v.to_string());
        let clients = self.clients.clone();
        (filter, clients)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hashbrown::HashMap;
    use mqtt::TopicName;
    use Action::*;

    #[derive(Clone)]
    enum Action<'a> {
        Sub(&'a str, RawFd),
        UnSub(&'a str, RawFd),
        Query(&'a str, Vec<(&'a str, Vec<RawFd>)>),
    }

    fn run_actions(actions: &[Action]) {
        let table = RouteTable::new();
        for action in actions {
            match action.clone() {
                Sub(filter, fd) => {
                    table.subscribe(&TopicFilter::new(filter).unwrap(), fd);
                }
                UnSub(filter, fd) => {
                    table.unsubscribe(&TopicFilter::new(filter).unwrap(), fd);
                }
                Query(name, expected) => {
                    let expected_map: HashMap<String, HashSet<RawFd>> = expected
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v.into_iter().collect::<HashSet<_>>()))
                        .collect();
                    let map: HashMap<String, HashSet<RawFd>> = table
                        .get_matches(&TopicName::new(name).unwrap())
                        .into_iter()
                        .map(|content| {
                            let (k, v) = content.read().to_simple();
                            (k.unwrap(), v)
                        })
                        .collect();
                    assert_eq!(map, expected_map);
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
            Query("abc", vec![("abc", vec![3])]),
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
        ])
    }
}
