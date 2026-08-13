use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_JSON_LINE: usize = 64 * 1024;
const MAX_FRAME: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct OutputOffsets {
    pub stdout: u64,
    pub stderr: u64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct AllOffsets {
    pub stdin: u64,
    pub stdout: u64,
    pub stderr: u64,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ClientHello {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offsets: Option<OutputOffsets>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ServerHello {
    pub offsets: AllOffsets,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub nobuffer: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExitResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
}

impl ExitResult {
    #[cfg(unix)]
    pub fn from_status(status: std::process::ExitStatus) -> Self {
        use std::os::unix::process::ExitStatusExt;
        Self {
            code: status.code(),
            signal: status.signal(),
        }
    }

    pub fn process_code(&self) -> i32 {
        self.code.unwrap_or_else(|| 128 + self.signal.unwrap_or(1))
    }
}

#[derive(Debug)]
pub enum Frame {
    StdinData { offset: u64, data: Vec<u8> },
    StdinEof { offset: u64 },
    StdoutData { offset: u64, data: Vec<u8> },
    StderrData { offset: u64, data: Vec<u8> },
    StdinPosition { offset: u64 },
    Exit(ExitResult),
}

pub async fn read_line<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        match reader.read(&mut byte).await? {
            0 if line.is_empty() => return Ok(None),
            0 => bail!("connection ended in the opening JSON message"),
            _ if byte[0] == b'\n' => return Ok(Some(line)),
            _ => {
                line.push(byte[0]);
                if line.len() > MAX_JSON_LINE {
                    bail!("opening JSON message is too large");
                }
            }
        }
    }
}

pub async fn write_json_line<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn write_error_line<W: AsyncWrite + Unpin>(writer: &mut W, error: &str) -> Result<()> {
    write_json_line(writer, &serde_json::json!({ "error": error })).await
}

pub async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, frame: &Frame) -> Result<()> {
    let (kind, payload) = match frame {
        Frame::StdinData { offset, data } => (1, offset_payload(*offset, data)),
        Frame::StdinEof { offset } => (2, offset.to_be_bytes().to_vec()),
        Frame::StdoutData { offset, data } => (3, offset_payload(*offset, data)),
        Frame::StderrData { offset, data } => (4, offset_payload(*offset, data)),
        Frame::StdinPosition { offset } => (5, offset.to_be_bytes().to_vec()),
        Frame::Exit(result) => (6, serde_json::to_vec(result)?),
    };
    if payload.len() > MAX_FRAME {
        bail!("frame is too large");
    }
    let mut header = [0_u8; 5];
    header[0] = kind;
    header[1..].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    writer.write_all(&header).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Frame>> {
    let mut header = [0_u8; 5];
    let first = reader.read(&mut header[..1]).await?;
    if first == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut header[1..]).await?;
    let length = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    if length > MAX_FRAME {
        bail!("received frame exceeds the size limit");
    }
    let mut payload = vec![0_u8; length];
    reader.read_exact(&mut payload).await?;
    let frame = match header[0] {
        1 => {
            let (offset, data) = split_offset(payload)?;
            Frame::StdinData { offset, data }
        }
        2 => Frame::StdinEof {
            offset: only_offset(&payload)?,
        },
        3 => {
            let (offset, data) = split_offset(payload)?;
            Frame::StdoutData { offset, data }
        }
        4 => {
            let (offset, data) = split_offset(payload)?;
            Frame::StderrData { offset, data }
        }
        5 => Frame::StdinPosition {
            offset: only_offset(&payload)?,
        },
        6 => Frame::Exit(serde_json::from_slice(&payload).context("invalid exit frame payload")?),
        other => bail!("unknown frame type {other}"),
    };
    Ok(Some(frame))
}

fn offset_payload(offset: u64, data: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8 + data.len());
    payload.extend_from_slice(&offset.to_be_bytes());
    payload.extend_from_slice(data);
    payload
}

fn only_offset(payload: &[u8]) -> Result<u64> {
    if payload.len() != 8 {
        bail!("offset frame has an invalid length");
    }
    Ok(u64::from_be_bytes(payload.try_into().unwrap()))
}

fn split_offset(payload: Vec<u8>) -> Result<(u64, Vec<u8>)> {
    if payload.len() < 8 {
        bail!("data frame has an invalid length");
    }
    let offset = u64::from_be_bytes(payload[..8].try_into().unwrap());
    Ok((offset, payload[8..].to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frames_round_trip() {
        let (mut left, mut right) = tokio::io::duplex(128);
        let write = tokio::spawn(async move {
            write_frame(
                &mut left,
                &Frame::StdoutData {
                    offset: 42,
                    data: b"hello".to_vec(),
                },
            )
            .await
            .unwrap();
        });
        let frame = read_frame(&mut right).await.unwrap().unwrap();
        write.await.unwrap();
        match frame {
            Frame::StdoutData { offset, data } => {
                assert_eq!(offset, 42);
                assert_eq!(data, b"hello");
            }
            _ => panic!("wrong frame"),
        }
    }
}
