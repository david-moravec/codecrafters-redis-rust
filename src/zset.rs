use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;

#[derive(Debug, PartialEq, PartialOrd)]
struct OrderedFloat(pub f64);

impl Eq for OrderedFloat {}
impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

#[derive(Debug)]
pub struct ZSet {
    hashmap: HashMap<Bytes, f64>,
    btree: BTreeMap<OrderedFloat, Bytes>,
}

impl ZSet {
    pub fn new() -> Self {
        Self {
            hashmap: HashMap::new(),
            btree: BTreeMap::new(),
        }
    }

    fn insert(&mut self, score: f64, member: Bytes) {
        self.hashmap.insert(member.clone(), score);
        self.btree.insert(OrderedFloat(score), member);
    }

    pub fn zadd(&mut self, score: f64, member: Bytes) -> usize {
        if self.hashmap.contains_key(&member) {
            let old_score = self.hashmap.get(&member).unwrap();
            self.btree.remove(&OrderedFloat(*old_score));
            self.insert(score, member);
            0
        } else {
            self.insert(score, member);
            1
        }
    }
}
