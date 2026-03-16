use anyhow::{Result, anyhow};
use bytes::Bytes;
use bytes::{Buf, BytesMut};
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::cmd::Command;
use crate::frame::{Frame, FrameError, ToFrame};
use crate::server::info::ServerInfo;

pub(crate) struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    pub(crate) command_queue: VecDeque<Command>,
    pub(crate) is_queueing_commands: bool,
    pub(crate) server_info: Arc<ServerInfo>,
}

impl Connection {
    pub fn new(stream: TcpStream, info: Arc<ServerInfo>) -> Connection {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4096),
            command_queue: VecDeque::new(),
            is_queueing_commands: false,
            server_info: info,
        }
    }
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(anyhow!("connecition reset by peer"));
                }
            }
        }
    }

    pub async fn write_rdb_file(&mut self, rdb_file: Bytes) -> Result<()> {
        self.stream.write_u8(b'$').await?;
        self.write_u64(rdb_file.len() as u64).await?;
        self.stream.write_all(b"\r\n").await?;
        self.stream
            .write_all(&rdb_file)
            .await
            .map_err(|e| anyhow!("writing rdb file failed; {:}", e))?;

        Ok(self.stream.flush().await?)
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        match frame {
            Frame::Array(array_opt) => match array_opt {
                Some(array) => {
                    self.stream.write_u8(frame.frame_symbol()).await?;
                    Box::pin(self.write_array(array)).await?;
                }
                None => {
                    self.stream.write_u8(frame.frame_symbol()).await?;
                    self.stream.write_all(b"-1\r\n").await?;
                }
            },
            Frame::Map(map) | Frame::Attribute(map) => {
                self.stream.write_u8(frame.frame_symbol()).await?;
                Box::pin(self.write_map(map)).await?;
            }
            Frame::Push(array) | Frame::Set(array) => {
                self.stream.write_u8(frame.frame_symbol()).await?;
                Box::pin(self.write_array(array)).await?;
            }
            _ => self.write_value(frame).await?,
        };

        Ok(self.stream.flush().await?)
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;

                buf.set_position(0);

                let frame = Frame::parse(&mut buf)?;
                self.buffer.advance(len);

                Ok(Some(frame))
            }
            Err(FrameError::Incomplete) => Ok(None),
            Err(FrameError::Other(err)) => Err(err),
        }
    }

    pub fn info_to_frame(&self) -> Frame {
        self.server_info.replication.to_frame()
    }

    pub async fn send_command(&mut self, args: &[&str]) -> Result<()> {
        self.write_frame(&Frame::bulk_strings_array(args)).await
    }

    async fn write_value(&mut self, frame: &Frame) -> Result<()> {
        self.stream.write_u8(frame.frame_symbol()).await?;

        match frame {
            Frame::Simple(val) => {
                self.stream.write_all(val.as_bytes()).await?;
            }
            Frame::Error(val) => {
                self.stream.write_all(val.as_bytes()).await?;
            }
            Frame::Integer(val) => self.write_u64(*val).await?,
            Frame::BulkString(bytes) => {
                self.write_u64(bytes.len() as u64).await?;
                self.stream.write_all(b"\r\n").await?;
                self.stream.write_all(&bytes).await?;
            }
            Frame::NullBulkString => self.stream.write_all(b"-1").await?,
            Frame::Null => self.stream.write_u8(b'_').await?,
            Frame::Bool(b) => {
                if *b {
                    self.stream.write_u8(b't').await?
                } else {
                    self.stream.write_u8(b'f').await?
                }
            }
            Frame::Double(d) => self.write_f64(*d).await?,
            Frame::BigNumber(bytes) => self.stream.write_all(&bytes).await?,
            Frame::BulkError(bytes) => self.stream.write_all(&bytes).await?,
            Frame::VerbatimString(encoding, bytes) => {
                self.stream.write_all(encoding.to_bytes()).await?;
                self.stream.write_u8(b':').await?;
                self.stream.write_all(&bytes).await?;
            }
            _ => todo!(),
        }

        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }

    async fn write_array(&mut self, array: &[Frame]) -> Result<()> {
        let len = array.len();
        self.write_u64(len as u64).await?;
        self.stream.write_all(b"\r\n").await?;

        for i in 0..len {
            self.write_frame(&array[i]).await?;
        }

        Ok(())
    }

    async fn write_map(&mut self, map: &HashMap<String, Frame>) -> Result<()> {
        let len = map.len();
        self.write_u64(len as u64).await?;
        self.stream.write_all(b"\r\n").await?;

        for (key, value) in map.iter() {
            self.stream.write_all(key.as_bytes()).await?;
            self.stream.write_all(b"\r\n").await?;
            self.write_frame(value).await?;
        }

        Ok(())
    }

    async fn write_f64(&mut self, val: f64) -> Result<()> {
        todo!()
    }

    async fn write_u64(&mut self, val: u64) -> Result<()> {
        use std::io::Write;

        let mut buf = [0u8; 12];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        Ok(())
    }

    async fn write_i64(&mut self, val: i64) -> Result<()> {
        use std::io::Write;

        let mut buf = [0u8; 12];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        Ok(())
    }
}
