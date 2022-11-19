use std::ops::Deref;
use std::os::unix::io::RawFd;
use std::sync::Arc;

use dashmap::DashMap;
use hashbrown::HashSet;
use mqtt::{TopicFilter, TopicFilterRef, TopicNameRef};
use parking_lot::RwLock;

const MATCH_ALL: &str = "#";
const MATCH_ONE: &str = "+";

pub struct RouteTable {
    nodes: Arc<DashMap<String, RouteNode>>,
}

struct RouteNode {
    content: Arc<RwLock<NodeContent>>,
    nodes: Arc<DashMap<String, RouteNode>>,
}

pub struct NodeContent {
    /// returned NodeContent always have topic_filter
    pub topic_filter: Option<TopicFilter>,
    pub clients: HashSet<RawFd>,
}

impl RouteTable {
    pub fn get_matches(&self, topic_name: &TopicNameRef) -> Vec<Arc<RwLock<NodeContent>>> {
        let mut topic_items = topic_name.split('/');
        let topic_item = topic_items.next().expect("split");
        let mut filters = Vec::new();
        for item in [topic_item, MATCH_ALL, MATCH_ONE] {
            if let Some(pair) = self.nodes.get(item) {
                pair.value()
                    .get_matches(item, topic_items.clone(), &mut filters);
            }
        }
        filters
    }

    pub fn subscribe(&self, topic_filter: &TopicFilterRef, fd: RawFd) {
        let mut filter_items = topic_filter.split('/');
        let filter_item = filter_items.next().expect("split");
        self.nodes
            .entry(filter_item.to_string())
            .or_insert_with(RouteNode::new)
            .insert(topic_filter, filter_items, fd);
    }

    pub fn unsubscribe(&self, topic_filter: &TopicFilterRef, fd: RawFd) {
        let mut filter_items = topic_filter.split('/');
        let filter_item = filter_items.next().expect("split");
        if let Some(mut pair) = self.nodes.get_mut(filter_item) {
            if pair.value_mut().remove(filter_items, fd) {
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
    fn get_matches<'a>(
        &self,
        prev_item: &'a str,
        mut topic_items: impl Iterator<Item = &'a str> + Clone,
        filters: &mut Vec<Arc<RwLock<NodeContent>>>,
    ) {
        if prev_item == MATCH_ALL {
            if !self.content.read().is_empty() {
                filters.push(Arc::clone(&self.content));
            }
        } else if let Some(topic_item) = topic_items.next() {
            for item in [topic_item, MATCH_ALL, MATCH_ONE] {
                if let Some(pair) = self.nodes.get(item) {
                    pair.value().get_matches(item, topic_items.clone(), filters);
                }
            }
        } else if !self.content.read().is_empty() {
            filters.push(Arc::clone(&self.content));
        }
    }
    fn insert<'a>(
        &self,
        topic_filter: &TopicFilterRef,
        mut filter_items: impl Iterator<Item = &'a str>,
        fd: RawFd,
    ) {
        if let Some(filter_item) = filter_items.next() {
            self.nodes
                .entry(filter_item.to_string())
                .or_insert_with(RouteNode::new)
                .insert(topic_filter, filter_items, fd);
        } else {
            let mut content = self.content.write();
            if content.topic_filter.is_none() {
                content.topic_filter =
                    Some(unsafe { TopicFilter::new_unchecked(topic_filter.deref()) });
            }
            content.clients.insert(fd);
        }
    }
    fn remove<'a>(&self, mut filter_items: impl Iterator<Item = &'a str>, fd: RawFd) -> bool {
        if let Some(filter_item) = filter_items.next() {
            if let Some(mut pair) = self.nodes.get_mut(filter_item) {
                if pair.value_mut().remove(filter_items, fd) {
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
