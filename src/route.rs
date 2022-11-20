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

pub struct NodeContent {
    /// Returned NodeContent always have topic_filter
    // I wonder if `topic_filter` field is worth saving, since it cost quite some memory.
    pub topic_filter: Option<TopicFilter>,
    pub clients: HashSet<RawFd>,
}

impl RouteTable {
    pub fn get_matches(&self, topic_name: &TopicNameRef) -> Vec<Arc<RwLock<NodeContent>>> {
        let (topic_item, rest_items) = topic_name.split_once(LEVEL_SEP).expect("split once");
        let mut filters = Vec::new();
        for item in [topic_item, MATCH_ALL, MATCH_ONE] {
            if let Some(pair) = self.nodes.get(item) {
                pair.value().get_matches(item, rest_items, &mut filters);
            }
        }
        filters
    }

    pub fn subscribe(&self, topic_filter: &TopicFilterRef, fd: RawFd) {
        let (filter_item, rest_items) = topic_filter.split_once(LEVEL_SEP).expect("split once");
        // Since subscribe is not an frequent action, string clone here is acceptable.
        self.nodes
            .entry(filter_item.to_string())
            .or_insert_with(RouteNode::new)
            .insert(topic_filter, rest_items, fd);
    }

    pub fn unsubscribe(&self, topic_filter: &TopicFilterRef, fd: RawFd) {
        let (filter_item, rest_items) = topic_filter.split_once(LEVEL_SEP).expect("split once");
        if let Some(mut pair) = self.nodes.get_mut(filter_item) {
            if pair.value_mut().remove(rest_items, fd) {
                self.nodes.remove(filter_item);
            }
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
        topic_items: &str,
        filters: &mut Vec<Arc<RwLock<NodeContent>>>,
    ) {
        if prev_item == MATCH_ALL {
            if !self.content.read().is_empty() {
                filters.push(Arc::clone(&self.content));
            }
        } else {
            if let Some((topic_item, rest_items)) = topic_items.split_once(LEVEL_SEP) {
                for item in [topic_item, MATCH_ALL, MATCH_ONE] {
                    if let Some(pair) = self.nodes.get(item) {
                        pair.value().get_matches(item, rest_items, filters);
                    }
                }
            } else if !self.content.read().is_empty() {
                filters.push(Arc::clone(&self.content));
            }
        }
    }

    fn insert(&self, topic_filter: &TopicFilterRef, filter_items: &str, fd: RawFd) {
        if let Some((filter_item, rest_items)) = filter_items.split_once(LEVEL_SEP) {
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

    fn remove(&self, filter_items: &str, fd: RawFd) -> bool {
        if let Some((filter_item, rest_items)) = filter_items.split_once(LEVEL_SEP) {
            if let Some(mut pair) = self.nodes.get_mut(filter_item) {
                if pair.value_mut().remove(rest_items, fd) {
                    self.nodes.remove(filter_item);
                    if self.content.read().is_empty() && self.nodes.is_empty() {
                        return true;
                    }
                }
            }
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

impl NodeContent {
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }
}
