use std::cmp::max;
use std::collections::{BTreeSet, HashMap};

use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
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
    btree: BTreeSet<(OrderedFloat, Bytes)>,
}

impl ZSet {
    pub fn new() -> Self {
        Self {
            hashmap: HashMap::new(),
            btree: BTreeSet::new(),
        }
    }

    fn insert(&mut self, score: f64, member: Bytes) {
        self.hashmap.insert(member.clone(), score);
        self.btree.insert((OrderedFloat(score), member));
    }

    pub fn zadd(&mut self, score: f64, member: Bytes) -> usize {
        if self.hashmap.contains_key(&member) {
            let old_score = self.hashmap.get(&member).unwrap();
            self.btree
                .remove(&(OrderedFloat(*old_score), member.clone()));
            self.insert(score, member);
            0
        } else {
            self.insert(score, member);
            1
        }
    }

    pub fn zrank(&self, member: Bytes) -> Option<usize> {
        if !self.hashmap.contains_key(&member) {
            None
        } else {
            let mut count = 0;

            for (_, m) in self.btree.iter() {
                if *m == member {
                    break;
                }
                count += 1;
            }

            Some(count)
        }
    }

    pub fn zrange(&self, start: i64, stop: i64) -> Vec<Bytes> {
        let list_len = self.btree.len();

        let true_stop = {
            if stop < 0 {
                max(list_len as i64 - stop.abs(), 0)
            } else if stop >= list_len as i64 {
                (list_len - 1) as i64
            } else {
                stop
            }
        };

        let true_start = {
            if start < 0 {
                max(list_len as i64 - start.abs(), 0)
            } else if start >= list_len as i64 {
                return vec![];
            } else {
                start
            }
        };

        if true_start > true_stop {
            return vec![];
        }

        let mut result = vec![];

        for (i, (_, member)) in self.btree.iter().enumerate() {
            if i < true_start as usize {
                continue;
            }
            if i > true_stop as usize {
                break;
            }

            result.push(member.clone());
        }

        result
    }

    pub fn zcard(&self) -> usize {
        self.btree.len()
    }

    pub fn zscore(&self, member: &Bytes) -> Option<f64> {
        self.hashmap.get(member).map(|f| *f)
    }

    pub fn zrem(&mut self, member: Bytes) -> usize {
        if !self.hashmap.contains_key(&member) {
            0
        } else {
            let score = self.hashmap.remove(&member).unwrap();
            self.btree.remove(&(OrderedFloat(score), member));
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zrank() {
        let mut set = ZSet::new();
        set.insert(6.328987168741629, Bytes::from("strawberry"));
        set.insert(6.328987168741629, Bytes::from("pineapple"));
        set.insert(9.005105404949294, Bytes::from("orange"));
        set.insert(97.005105404949294, Bytes::from("blueberry"));

        assert!(set.zrank(Bytes::from("pineapple")) == Some(0));
        assert!(set.zrank(Bytes::from("strawberry")) == Some(1));
        assert!(set.zrank(Bytes::from("orange")) == Some(2));
        assert!(set.zrank(Bytes::from("blueberry")) == Some(3));
    }
}
