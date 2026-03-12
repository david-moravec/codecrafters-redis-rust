use crate::frame::Frame;
use crate::parser::Parse;

pub struct LPop {
    key: String,
    start: Option<i64>,
    stop: Option<i64>,
}

impl LPop {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        let start = parse.next_i64().ok();
        let stop = parse.next_i64().ok();

        Ok(LPop { key, start, stop })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let start_stop_is_none = self.start.is_none() && self.stop.is_none();

        let values = db.lpop(&self.key, self.start, self.stop);

        let frame = match values {
            Some(v) => {
                if start_stop_is_none {
                    Frame::BulkString(v.get(0).unwrap().clone())
                } else {
                    Frame::Array(Some(
                        v.into_iter()
                            .map(|bytes| Frame::BulkString(bytes))
                            .collect(),
                    ))
                }
            }

            None => Frame::NullBulkString,
        };

        Ok(frame)
    }
}
