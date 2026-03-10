use crate::frame::ToFrame;
use crate::parser::{Parse, StreamEntryIDOpt};

pub struct XRead {
    keys: Vec<String>,
    ids: Vec<StreamEntryIDOpt>,
}

impl XRead {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        // parse 'streams' option
        parse.next_string()?;

        let mut bytes_vec = vec![];

        while let Ok(bytes) = parse.next_bytes() {
            bytes_vec.push(bytes);
            bytes_vec.push(parse.next_bytes()?);
        }

        let bytes_vec_len = bytes_vec.len();

        let keys: Vec<String> = bytes_vec[..bytes_vec_len / 2]
            .iter()
            .map(|b| String::from_utf8(b.to_vec()).unwrap())
            .collect();
        let ids: Vec<StreamEntryIDOpt> = bytes_vec[bytes_vec_len / 2..bytes_vec_len]
            .into_iter()
            .map(|b| StreamEntryIDOpt::try_from(b.clone()).unwrap())
            .collect();

        Ok(XRead { keys, ids })
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let xread = db.xread(self.keys, self.ids)?;
        let frame = xread.to_frame();
        dst.write_frame(&frame).await?;
        Ok(())
    }
}
