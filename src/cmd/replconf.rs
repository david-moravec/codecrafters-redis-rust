use crate::frame::Frame;
use crate::parser::Parse;
use bytes::Bytes;

#[derive(Debug)]
pub struct Replconf {
    send_offset: bool,
}

impl Replconf {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let mut send_offset = false;

        for _ in 0..2 {
            if parse.next_string()?.to_lowercase() == "getack" {
                send_offset = true;
            }
        }
        Ok(Replconf { send_offset })
    }
    pub fn apply(self, offset: usize) -> anyhow::Result<Frame> {
        let frame = {
            if self.send_offset {
                let f = Frame::bulk_strings_array(vec![
                    Bytes::from("REPLCONF"),
                    Bytes::from("ACK"),
                    Bytes::from(format!("{:}", offset)),
                ]);
                f
            } else {
                Frame::Simple("OK".to_string())
            }
        };
        Ok(frame)
    }
}
