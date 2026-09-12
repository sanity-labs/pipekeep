//! Finite logical retained bytes, in the surviving broker only.
use crate::protocol::ExitResult;
use serde::{Deserialize, Serialize};

pub const CAPABILITY: &str = "finite-output-v1";
pub const ATTACHMENT: &str = "framed-output-v1";
pub const READ_SIZE: usize = 4096;
pub const TAIL_BYTES: u64 = 256 * 1024;
pub const TAIL_MILLIS: u64 = 200;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    OutputLimit,
    StorageFault,
    OutputReadFault,
    PipeSetupFault,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StorageFault {
    Write,
    PartialWrite,
    Flush,
    ReplayOpen,
    ReplayRead,
    WorkerLost,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CollectionCause {
    TailBytes,
    TailTime,
    OperationDeadline,
    GroupUnconfirmed,
    StoragePending,
    OutputRead,
    PipeSetup,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "state", content = "cause", rename_all = "snake_case")]
pub enum Collection {
    Reading,
    Eof,
    Unconfirmed(CollectionCause),
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct StreamFact {
    pub retained: u64,
    pub reserved: u64,
    pub discarded: u64,
    pub storage_fault: Option<StorageFault>,
    pub collection: Collection,
    pub prefix_complete: bool,
}
impl Default for StreamFact {
    fn default() -> Self {
        Self {
            retained: 0,
            reserved: 0,
            discarded: 0,
            storage_fault: None,
            collection: Collection::Reading,
            prefix_complete: true,
        }
    }
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ControlFault {
    Unavailable,
    Wait,
    Signal,
    Deadline,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "state", content = "cause", rename_all = "snake_case")]
pub enum GroupControl {
    Ready,
    Running,
    Settled,
    Unconfirmed(ControlFault),
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OutputFact {
    pub limit: u64,
    pub first_stop: Option<StopReason>,
    pub stdout: StreamFact,
    pub stderr: StreamFact,
    pub io_inflight: u32,
    pub sealed: bool,
    pub tail_read: u64,
    pub leader_exit: Option<ExitResult>,
    pub original_group_absent: bool,
    pub group_control: GroupControl,
}
impl OutputFact {
    pub fn new(limit: u64) -> Self {
        Self {
            limit,
            first_stop: None,
            stdout: StreamFact::default(),
            stderr: StreamFact::default(),
            io_inflight: 0,
            sealed: false,
            tail_read: 0,
            leader_exit: None,
            original_group_absent: false,
            group_control: GroupControl::Ready,
        }
    }
    pub fn available(&self) -> u64 {
        let charged = self
            .stdout
            .retained
            .checked_add(self.stderr.retained)
            .and_then(|v| v.checked_add(self.stdout.reserved))
            .and_then(|v| v.checked_add(self.stderr.reserved))
            .expect("bounded accounting overflow");
        self.limit
            .checked_sub(charged)
            .expect("bounded reservation exceeded")
    }
    pub fn collected(&self) -> bool {
        self.stdout.collection != Collection::Reading
            && self.stderr.collection != Collection::Reading
    }
    pub fn complete(&self) -> bool {
        self.first_stop.is_none()
            && self.io_inflight == 0
            && self.stdout.prefix_complete
            && self.stderr.prefix_complete
            && self.stdout.collection == Collection::Eof
            && self.stderr.collection == Collection::Eof
    }
    pub fn validate(&self, terminal: bool) -> anyhow::Result<()> {
        use anyhow::{bail, Context};
        let charged = self
            .stdout
            .retained
            .checked_add(self.stderr.retained)
            .and_then(|v| v.checked_add(self.stdout.reserved))
            .and_then(|v| v.checked_add(self.stderr.reserved))
            .context("invalid bounded accounting")?;
        if self.limit > i64::MAX as u64
            || charged > self.limit
            || self.tail_read > TAIL_BYTES
            || self.io_inflight > 3
        {
            bail!("invalid finite output fact");
        }
        if let Some(exit) = &self.leader_exit {
            exit.validate()?;
        }
        for s in [&self.stdout, &self.stderr] {
            if s.prefix_complete
                && (s.collection != Collection::Eof
                    || s.reserved != 0
                    || self.io_inflight != 0
                    || s.discarded != 0
                    || s.storage_fault.is_some())
            {
                bail!("invalid output completeness fact");
            }
        }
        if terminal
            && (!self.collected()
                || !self.sealed
                || self.io_inflight != 0
                || self.stdout.reserved != 0
                || self.stderr.reserved != 0)
        {
            bail!("unconfirmed storage or collection cannot be a replay end");
        }
        Ok(())
    }
    pub fn process_code(&self) -> i32 {
        let code = self
            .leader_exit
            .as_ref()
            .map_or(1, ExitResult::process_code);
        if code == 0 && !self.complete() {
            1
        } else {
            code
        }
    }
}
