use std::mem;
use std::sync::Arc;

use dashmap::DashMap;
use mqtt::{qos::QualityOfService, TopicName};

use super::route::{split_topic, MATCH_ALL, MATCH_ONE};
use crate::state::ClientId;

#[derive(Debug)]
pub struct RetainTable {
    inner: RetainNode,
}

#[derive(Debug)]
struct RetainNode {
    content: Option<Arc<RetainContent>>,
    nodes: Arc<DashMap<String, RetainNode>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct RetainContent {
    topic_name: TopicName,
    // TODO: may use QualityOfService
    qos: QualityOfService,
    // TODO: maybe should change to bytes::Bytes
    payload: Vec<u8>,
    client_id: ClientId,
}

impl RetainTable {
    pub fn new() -> RetainTable {
        RetainTable {
            inner: RetainNode::new(),
        }
    }

    pub fn get_matches(&self, topic_filter: &str) -> Vec<Arc<RetainContent>> {
        let (filter_item, rest_items) = split_topic(topic_filter);
        let mut retains = Vec::new();
        self.inner
            .get_matches(filter_item, rest_items, &mut retains);
        retains
    }

    pub fn insert(&self, content: Arc<RetainContent>) -> Option<Arc<RetainContent>> {
        let content_clone = Arc::clone(&content);
        let (topic_item, rest_items) = split_topic(&content_clone.topic_name);
        self.inner.insert(topic_item, rest_items, content)
    }

    pub fn remove(&self, topic_name: &str) -> Option<Arc<RetainContent>> {
        let (topic_item, rest_items) = split_topic(topic_name);
        self.inner.remove(topic_item, rest_items)
    }
}

impl RetainNode {
    fn new() -> RetainNode {
        RetainNode {
            content: None,
            nodes: Arc::new(DashMap::new()),
        }
    }

    fn is_empty(&self) -> bool {
        self.content.is_none() && self.nodes.is_empty()
    }

    fn get_matches(
        &self,
        prev_item: &str,
        filter_items: Option<&str>,
        retains: &mut Vec<Arc<RetainContent>>,
    ) {
        match prev_item {
            MATCH_ALL => {
                println!("match all");
                assert!(filter_items.is_none(), "invalid topic filter");
                for pair in self.nodes.iter() {
                    pair.value().get_matches(MATCH_ALL, None, retains);
                }
                // Topic name "abc" will match topic filter "abc/#", since "#" also represent parent level.
                if let Some(content) = self.content.as_ref() {
                    println!("all.push: {:?}", content);
                    retains.push(Arc::clone(content));
                }
            }
            MATCH_ONE => {
                println!("match one, filter_items: {:?}", filter_items);
                if let Some((filter_item, rest_items)) = filter_items.map(split_topic) {
                    for pair in self.nodes.iter() {
                        pair.value().get_matches(filter_item, rest_items, retains);
                    }
                } else {
                    for pair in self.nodes.iter() {
                        if let Some(content) = pair.value().content.as_ref() {
                            println!("one.push: {:?}", content);
                            retains.push(Arc::clone(content));
                        }
                    }
                }
            }
            _ => {
                if let Some(pair) = self.nodes.get(prev_item) {
                    println!(
                        "match item, prev_item: {}, filter_items: {:?}",
                        prev_item, filter_items
                    );
                    if let Some((filter_item, rest_items)) = filter_items.map(split_topic) {
                        pair.value().get_matches(filter_item, rest_items, retains);
                    } else if let Some(content) = pair.value().content.as_ref() {
                        println!("item.push: {:?}", content);
                        retains.push(Arc::clone(content));
                    }
                }
            }
        }
    }

    fn insert(
        &self,
        prev_item: &str,
        topic_items: Option<&str>,
        content: Arc<RetainContent>,
    ) -> Option<Arc<RetainContent>> {
        if let Some(mut pair) = self.nodes.get_mut(prev_item) {
            if let Some((topic_item, rest_items)) = topic_items.map(split_topic) {
                pair.value().insert(topic_item, rest_items, content)
            } else {
                mem::replace(&mut pair.value_mut().content, Some(content))
            }
        } else {
            let mut new_node = RetainNode::new();
            if let Some((topic_item, rest_items)) = topic_items.map(split_topic) {
                new_node.insert(topic_item, rest_items, content);
            } else {
                new_node.content = Some(content);
            }
            self.nodes.insert(prev_item.to_string(), new_node);
            None
        }
    }

    fn remove(&self, prev_item: &str, topic_items: Option<&str>) -> Option<Arc<RetainContent>> {
        let mut old_content = None;
        let mut remove_node = false;
        if let Some(mut pair) = self.nodes.get_mut(prev_item) {
            old_content = if let Some((topic_item, rest_items)) = topic_items.map(split_topic) {
                pair.value().remove(topic_item, rest_items)
            } else {
                pair.value_mut().content.take()
            };
            remove_node = pair.value().is_empty();
        }
        if remove_node {
            self.nodes.remove(prev_item);
        }
        old_content
    }
}

impl RetainContent {
    pub fn new(
        topic_name: TopicName,
        qos: QualityOfService,
        payload: Vec<u8>,
        client_id: ClientId,
    ) -> RetainContent {
        RetainContent {
            topic_name,
            qos,
            payload,
            client_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hashbrown::{HashMap, HashSet};
    use mqtt::TopicFilter;
    use Action::*;
    use QualityOfService::*;

    impl From<(&str, QualityOfService, Vec<u8>, u64)> for RetainContent {
        fn from(
            (topic_name, qos, payload, client_id): (&str, QualityOfService, Vec<u8>, u64),
        ) -> RetainContent {
            let topic_name = unsafe { TopicName::new_unchecked(topic_name.to_string()) };
            let client_id = ClientId(client_id);
            Self::new(topic_name, qos, payload, client_id)
        }
    }

    #[derive(Clone)]
    enum Action<'a> {
        // (NewContent, OldContent)
        Insert(RetainContent, Option<RetainContent>),
        // (TopicName, OldContent)
        Remove(&'a str, Option<RetainContent>),
        // (TopicFilter, Retains)
        Query(&'a str, Vec<RetainContent>),
    }

    fn run_actions(actions: &[Action]) {
        let table = RetainTable::new();
        for action in actions {
            match action.clone() {
                Insert(new_content, old_content) => {
                    let rv = table.insert(Arc::new(new_content));
                    assert_eq!(old_content.map(Arc::new), rv);
                }
                Remove(topic_name, old_content) => {
                    let rv = table.remove(topic_name);
                    assert_eq!(old_content.map(Arc::new), rv);
                }
                Query(topic_filter, retains) => {
                    let mut rv = table.get_matches(topic_filter);
                    rv.sort();
                    let mut retains = retains.into_iter().map(Arc::new).collect::<Vec<_>>();
                    retains.sort();
                    assert_eq!(
                        retains, rv,
                        "\nrv: {:#?}\nexpected: {:#?}\ntable: {:#?}",
                        rv, retains, table
                    );
                }
            }
        }
    }

    #[test]
    fn test_match_all() {
        run_actions(&[
            Insert(("1/2/3/4", Level1, vec![9, 9], 9).into(), None),
            Insert(("abc", Level0, vec![1, 1], 3).into(), None),
            Insert(("abc/ijk", Level1, vec![2, 2], 4).into(), None),
            Insert(("abc/xyz", Level1, vec![3, 3], 5).into(), None),
            Insert(("abc/xyz/xxx", Level1, vec![4, 4], 6).into(), None),
            Query(
                "#",
                vec![
                    ("abc", Level0, vec![1, 1], 3).into(),
                    ("1/2/3/4", Level1, vec![9, 9], 9).into(),
                    ("abc/ijk", Level1, vec![2, 2], 4).into(),
                    ("abc/xyz", Level1, vec![3, 3], 5).into(),
                    ("abc/xyz/xxx", Level1, vec![4, 4], 6).into(),
                ],
            ),
            Query(
                "abc/#",
                vec![
                    ("abc", Level0, vec![1, 1], 3).into(),
                    ("abc/ijk", Level1, vec![2, 2], 4).into(),
                    ("abc/xyz", Level1, vec![3, 3], 5).into(),
                    ("abc/xyz/xxx", Level1, vec![4, 4], 6).into(),
                ],
            ),
            Query(
                "abc/xyz/#",
                vec![
                    ("abc/xyz", Level1, vec![3, 3], 5).into(),
                    ("abc/xyz/xxx", Level1, vec![4, 4], 6).into(),
                ],
            ),
        ]);
    }

    #[test]
    fn test_match_one() {
        run_actions(&[
            Insert(("1/2/3/4", Level1, vec![9, 9], 9).into(), None),
            Insert(("abc", Level0, vec![1, 1], 3).into(), None),
            Insert(("abc/ijk", Level1, vec![2, 2], 4).into(), None),
            Insert(("abc/xyz", Level1, vec![3, 3], 5).into(), None),
            Insert(("abc/xyz/xxx", Level1, vec![4, 4], 6).into(), None),
            Query("+", vec![("abc", Level0, vec![1, 1], 3).into()]),
            Query(
                "abc/+",
                vec![
                    ("abc/ijk", Level1, vec![2, 2], 4).into(),
                    ("abc/xyz", Level1, vec![3, 3], 5).into(),
                ],
            ),
            Query(
                "abc/+/+",
                vec![("abc/xyz/xxx", Level1, vec![4, 4], 6).into()],
            ),
            Query("+/ijk", vec![("abc/ijk", Level1, vec![2, 2], 4).into()]),
            Query("+/6666", vec![]),
            Query("1/+/3/+", vec![("1/2/3/4", Level1, vec![9, 9], 9).into()]),
        ])
    }

    #[test]
    fn test_match_complex() {
        run_actions(&[
            Insert(("1/2/3/4", Level1, vec![9, 9], 9).into(), None),
            Insert(("abc", Level0, vec![1, 1], 3).into(), None),
            Insert(("abc/ijk", Level1, vec![2, 2], 4).into(), None),
            Insert(("abc/xyz", Level1, vec![3, 3], 5).into(), None),
            Insert(("abc/xyz/xxx", Level1, vec![4, 4], 6).into(), None),
            Query(
                "+/#",
                vec![
                    ("1/2/3/4", Level1, vec![9, 9], 9).into(),
                    ("abc", Level0, vec![1, 1], 3).into(),
                    ("abc/ijk", Level1, vec![2, 2], 4).into(),
                    ("abc/xyz", Level1, vec![3, 3], 5).into(),
                    ("abc/xyz/xxx", Level1, vec![4, 4], 6).into(),
                ],
            ),
            Query(
                "abc/+/#",
                vec![
                    ("abc/ijk", Level1, vec![2, 2], 4).into(),
                    ("abc/xyz", Level1, vec![3, 3], 5).into(),
                    ("abc/xyz/xxx", Level1, vec![4, 4], 6).into(),
                ],
            ),
            Query(
                "+/xyz/#",
                vec![
                    ("abc/xyz", Level1, vec![3, 3], 5).into(),
                    ("abc/xyz/xxx", Level1, vec![4, 4], 6).into(),
                ],
            ),
            Query("abc/xyz/3", vec![]),
            Query("1/2/3/4/5", vec![]),
            Query("1/2/3/4/+", vec![]),
            Query("1/2/3/4/#", vec![("1/2/3/4", Level1, vec![9, 9], 9).into()]),
            Query("1/2/#", vec![("1/2/3/4", Level1, vec![9, 9], 9).into()]),
        ]);
    }

    #[test]
    fn test_insert() {
        run_actions(&[
            Insert(("abc", Level0, vec![1, 1], 3).into(), None),
            Insert(("abc/ijk", Level1, vec![2, 2], 4).into(), None),
            Insert(("abc/xyz", Level1, vec![3, 3], 5).into(), None),
            Insert(("abc/xyz/xxx", Level1, vec![4, 4], 6).into(), None),
            Query("abc", vec![("abc", Level0, vec![1, 1], 3).into()]),
            Insert(
                ("abc", Level1, vec![11, 11], 13).into(),
                Some(("abc", Level0, vec![1, 1], 3).into()),
            ),
            Query("abc", vec![("abc", Level1, vec![11, 11], 13).into()]),
            Insert(
                ("abc", Level2, vec![21, 21], 23).into(),
                Some(("abc", Level1, vec![11, 11], 13).into()),
            ),
            Query("abc", vec![("abc", Level2, vec![21, 21], 23).into()]),
            Query(
                "abc/+",
                vec![
                    ("abc/ijk", Level1, vec![2, 2], 4).into(),
                    ("abc/xyz", Level1, vec![3, 3], 5).into(),
                ],
            ),
        ]);
    }

    #[test]
    fn test_remove() {
        run_actions(&[
            Insert(("abc", Level0, vec![1, 1], 3).into(), None),
            Insert(("abc/ijk", Level1, vec![2, 2], 4).into(), None),
            Insert(("abc/xyz", Level1, vec![3, 3], 5).into(), None),
            Insert(("abc/xyz/xxx", Level1, vec![4, 4], 6).into(), None),
            Query("abc", vec![("abc", Level0, vec![1, 1], 3).into()]),
            Remove("abc", Some(("abc", Level0, vec![1, 1], 3).into())),
            Query("abc", vec![]),
            Remove("abc", None),
            Query("abc", vec![]),
            Query(
                "abc/+",
                vec![
                    ("abc/ijk", Level1, vec![2, 2], 4).into(),
                    ("abc/xyz", Level1, vec![3, 3], 5).into(),
                ],
            ),
        ]);
    }
}
