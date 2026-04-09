use crate::frame::Frame;
use crate::parser::Parse;

use bytes::Bytes;

#[derive(Debug)]
pub struct GeoAdd {
    key: String,
    longitude: f64,
    latitude: f64,
    member: Bytes,
}

impl GeoAdd {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        let longitude = parse.next_f64()?;
        let latitude = parse.next_f64()?;
        let member = Bytes::from(parse.next_string()?);

        Ok(GeoAdd {
            key,
            longitude,
            latitude,
            member,
        })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let frame = match db.geoadd(self.key, self.longitude, self.latitude, self.member) {
            Ok(i) => Frame::Integer(i as u64),
            Err(err) => Frame::Error(format!("{:}", err)),
        };
        Ok(frame)
    }
}
