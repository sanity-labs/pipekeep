use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Version of the public attachment handshake and its sticky-state semantics.
/// Compatible additions keep this number; an incompatible change bumps it.
pub const ATTACHMENT_PROTOCOL_VERSION: u32 = 1;
pub const GROUP_PIDFD_CAPABILITY: &str = "group-pidfd-cancel-v1";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SessionFact {
    pub capabilities: Vec<String>,
    pub workload_started: bool,
    pub cancel_state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_error: Option<String>,
}

pub const FRAMED_ATTACHMENT_V1: &str = "framed-v1";

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

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClientAction {
    StdinEof,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ClientHello {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<ClientAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offsets: Option<OutputOffsets>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub stdin_start: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin_eof: Option<u64>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub force: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "lowercase")]
pub enum BrokerRequest {
    Attach {
        #[serde(default)]
        offsets: OutputOffsets,
        #[serde(default)]
        stdin_start: u64,
        #[serde(default)]
        stdin_eof: Option<u64>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        force: bool,
    },
    // A distinct action makes old brokers reject before installing a lease.
    #[serde(rename = "attach-framed-v1")]
    AttachFramedV1 {
        #[serde(default)]
        offsets: OutputOffsets,
        #[serde(default)]
        stdin_start: u64,
        #[serde(default)]
        stdin_eof: Option<u64>,
        #[serde(default)]
        force: bool,
    },
    #[serde(rename = "stdin-eof")]
    StdinEof {
        offset: u64,
    },
    Pid,
    Cancel,
    // Distinct action: old serde brokers reject it before any signal. An
    // optional field on Cancel would be silently ignored by old brokers.
    #[serde(rename = "cancel-group-pidfd")]
    CancelGroupPidfd,
    Session,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ServerHello {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachment: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin_eof_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionFact>,
    pub offsets: AllOffsets,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub nobuffer: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub stdin_eof: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_eof: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_eof: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit: Option<ExitResult>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExitResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
}

/// `CancelWon` means at least one TERM/KILL syscall succeeded. This includes
/// zombie-only groups and proves neither a live recipient nor historical
/// request attribution. A later settled call returns `AlreadyExited`. The
/// actual retained command exit is never relabeled by this verdict.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CancelOutcome {
    AlreadyExited,
    CancelWon,
}

impl ExitResult {
    pub fn validate(&self) -> Result<()> {
        match (self.code, self.signal) {
            (Some(0..=255), None) | (None, Some(1..=127)) => Ok(()),
            _ => bail!("exit frame must contain one actual code or signal"),
        }
    }

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

fn is_zero(value: &u64) -> bool {
    *value == 0
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
        Frame::StdinData { offset, data } => (1, offset_payload(*offset, data)?),
        Frame::StdinEof { offset } => (2, offset.to_be_bytes().to_vec()),
        Frame::StdoutData { offset, data } => (3, offset_payload(*offset, data)?),
        Frame::StderrData { offset, data } => (4, offset_payload(*offset, data)?),
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

#[derive(Clone, Copy)]
pub enum Direction {
    Input,
    Output,
}

pub fn checked_end(offset: u64, length: usize) -> Result<u64> {
    offset
        .checked_add(u64::try_from(length)?)
        .context("frame position overflowed")
}

fn validate_header(kind: u8, length: usize, direction: Direction) -> Result<()> {
    let allowed = match direction {
        Direction::Input => matches!(kind, 1 | 2),
        Direction::Output => matches!(kind, 3..=6),
    };
    if !allowed {
        bail!("frame has invalid direction or type");
    }
    let valid = match kind {
        1 | 3 | 4 => (8..=MAX_FRAME).contains(&length),
        2 | 5 => length == 8,
        6 => (1..=256).contains(&length),
        _ => false,
    };
    if !valid {
        bail!("frame has invalid length or exceeds the size limit");
    }
    Ok(())
}

pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    direction: Direction,
) -> Result<Option<Frame>> {
    let mut header = [0_u8; 5];
    let first = reader.read(&mut header[..1]).await?;
    if first == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut header[1..]).await?;
    let length = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    validate_header(header[0], length, direction)?;
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
        6 => {
            let result: ExitResult =
                serde_json::from_slice(&payload).context("invalid exit frame payload")?;
            result.validate()?;
            Frame::Exit(result)
        }
        other => bail!("unknown frame type {other}"),
    };
    Ok(Some(frame))
}

fn offset_payload(offset: u64, data: &[u8]) -> Result<Vec<u8>> {
    // Check before the payload allocation, including for locally produced frames.
    if data.len() > MAX_FRAME - 8 {
        bail!("frame is too large");
    }
    checked_end(offset, data.len())?;
    let mut payload = Vec::with_capacity(8 + data.len());
    payload.extend_from_slice(&offset.to_be_bytes());
    payload.extend_from_slice(data);
    Ok(payload)
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
    checked_end(offset, payload.len() - 8)?;
    Ok((offset, payload[8..].to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_limits_and_checked_ranges() {
        for kind in [1, 3, 4] {
            let direction = if kind == 1 {
                Direction::Input
            } else {
                Direction::Output
            };
            assert!(validate_header(kind, 8, direction).is_ok());
            assert!(validate_header(kind, MAX_FRAME, direction).is_ok());
            assert!(validate_header(kind, MAX_FRAME + 1, direction).is_err());
            assert!(validate_header(kind, 7, direction).is_err());
        }
        assert!(validate_header(2, 9, Direction::Input).is_err());
        assert!(validate_header(5, 9, Direction::Output).is_err());
        assert!(validate_header(6, 257, Direction::Output).is_err());
        assert!(validate_header(3, MAX_FRAME, Direction::Input).is_err());
        assert_eq!(checked_end(u64::MAX - 4, 4).unwrap(), u64::MAX);
        assert!(checked_end(u64::MAX - 4, 5).is_err());
        assert!(offset_payload(u64::MAX, b"x").is_err());
        for (code, signal) in [
            (None, None),
            (Some(0), Some(15)),
            (Some(-1), None),
            (None, Some(0)),
            (None, Some(i32::MAX)),
        ] {
            assert!(ExitResult { code, signal }.validate().is_err());
        }
    }

    #[tokio::test]
    async fn rejects_bad_headers_without_reading_or_allocating_payload() {
        for (kind, length) in [(1, u32::MAX), (3, 16 * 1024 * 1024), (2, 9), (99, 1)] {
            let mut bytes = vec![kind];
            bytes.extend_from_slice(&length.to_be_bytes());
            // Writer stays open with no payload: timeout would mean the
            // decoder trusted the header enough to allocate/read its body.
            let (mut writer, mut reader) = tokio::io::duplex(5);
            writer.write_all(&bytes).await.unwrap();
            assert!(tokio::time::timeout(
                std::time::Duration::from_secs(1),
                read_frame(&mut reader, Direction::Input)
            )
            .await
            .unwrap()
            .is_err());
        }
        for bytes in [vec![1], vec![1, 0, 0, 0, 9, 0, 0], vec![2, 0, 0, 0, 8, 0]] {
            assert!(read_frame(&mut bytes.as_slice(), Direction::Input)
                .await
                .is_err());
        }
        let mut bytes = vec![1, 0, 0, 0, 9];
        bytes.extend_from_slice(&u64::MAX.to_be_bytes());
        bytes.push(1);
        assert!(read_frame(&mut bytes.as_slice(), Direction::Input)
            .await
            .is_err());
    }

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
        let frame = read_frame(&mut right, Direction::Output)
            .await
            .unwrap()
            .unwrap();
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
