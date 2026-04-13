use crate::frame::Frame;
use crate::parser::Parse;

#[derive(Debug)]
pub struct GeoSearch {
    key: String,
    lon: f64,
    lat: f64,
    radius: f64,
}

impl GeoSearch {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        parse.next_string()?;
        let lon = parse.next_f64()?;
        let lat = parse.next_f64()?;
        parse.next_string()?;
        let radius = parse.next_f64()?;
        parse.next_string()?;

        Ok(GeoSearch {
            key,
            lon,
            lat,
            radius,
        })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let locations_frames = db
            .geosearch(&self.key, self.lon, self.lat, self.radius)
            .into_iter()
            .map(Frame::BulkString)
            .collect();

        Ok(Frame::Array(Some(locations_frames)))
    }
}
