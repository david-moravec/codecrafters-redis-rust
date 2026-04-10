use crate::frame::Frame;
use crate::parser::Parse;

use bytes::Bytes;

#[derive(Debug)]
pub struct GeoPos {
    key: String,
    members: Vec<Bytes>,
}

impl GeoPos {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;

        let mut members = vec![];

        while let Ok(loc) = parse.next_string() {
            members.push(Bytes::from(loc));
        }

        Ok(GeoPos { key, members })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let locations_frames = db
            .geopos(&self.key, self.members)
            .into_iter()
            .map(|opt_lon_lat| match opt_lon_lat {
                Some((lon, lat)) => Frame::bulk_strings_array_from_str(vec![
                    &format!("{:}", lon),
                    &format!("{:}", lat),
                ]),
                None => Frame::Array(None),
            })
            .collect();

        Ok(Frame::Array(Some(locations_frames)))
    }
}
