use crate::frame::Frame;
use crate::parser::Parse;
use crate::server::info::ServerInfo;
use anyhow::anyhow;

#[derive(Debug)]
pub struct Config {
    subcmd: String,
    arg: String,
}

impl Config {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let subcmd = parse.next_string()?;
        let arg = parse.next_string()?;
        Ok(Config { subcmd, arg })
    }
    pub fn apply(self, server_info: ServerInfo) -> anyhow::Result<Frame> {
        let frame = {
            if self.arg == "dir" {
                Frame::bulk_strings_array_from_str(vec![
                    "dir",
                    &format!("{:}", server_info.config_dir().display()),
                ])
            } else if self.arg == "dbfilename" {
                Frame::bulk_strings_array_from_str(vec![
                    "dbfilename",
                    &format!("{:}", server_info.config_db_file_name().display()),
                ])
            } else {
                return Err(anyhow!("unknown CONFIG GET arg '{:}'", self.arg));
            }
        };

        Ok(frame)
    }
}
