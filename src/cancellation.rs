//! One broker-owned operation. Requests only join it; they never own signals.
use crate::group_pidfd::GroupSignal;
use crate::output::{ControlFault, GroupControl};
use crate::protocol::{CancelOutcome, ExitResult, SessionFact, GROUP_PIDFD_CAPABILITY};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Ready,
    Term,
    Kill,
    NoSignal,
    Complete,
    Failed,
}

pub type Completion = Result<(CancelOutcome, ExitResult), String>;

pub struct Cancellation {
    handle: Option<Box<dyn GroupSignal>>,
    verified: bool,
    phase: Phase,
    grace_deadline: Option<Instant>,
    finish_deadline: Option<Instant>,
    signaled: bool,
    group_absent: bool,
    fault: Option<ControlFault>,
    pub result: Option<Completion>,
}

impl Cancellation {
    pub fn verify_acquired(handle: Result<Box<dyn GroupSignal>, String>) -> Self {
        let mut state = Self::new(handle);
        if let Some(handle) = &state.handle {
            if let Err(error) = handle.signal(0) {
                state.verified = false;
                state.fault = Some(ControlFault::Unavailable);
                state.fail(format!("workload started; group pidfd acquisition/verification failed: {error}; session retained; cancellation unavailable; do not retry creation"));
            }
        }
        state
    }

    fn new(handle: Result<Box<dyn GroupSignal>, String>) -> Self {
        let (handle, error) = match handle {
            Ok(handle) => (Some(handle), None),
            Err(error) => (None, Some(error)),
        };
        Self {
            verified: handle.is_some(),
            handle,
            phase: if error.is_some() {
                Phase::Failed
            } else {
                Phase::Ready
            },
            grace_deadline: None,
            finish_deadline: None,
            signaled: false,
            group_absent: false,
            fault: error.as_ref().map(|_| ControlFault::Unavailable),
            result: error.map(Err),
        }
    }

    pub fn output_control(&self) -> GroupControl {
        match self.phase {
            Phase::Ready => GroupControl::Ready,
            Phase::Term | Phase::Kill | Phase::NoSignal => GroupControl::Running,
            Phase::Complete => GroupControl::Settled,
            Phase::Failed => {
                GroupControl::Unconfirmed(self.fault.unwrap_or(ControlFault::Unavailable))
            }
        }
    }
    pub fn group_absent(&self) -> bool {
        self.group_absent
    }
    pub fn finish_deadline(&self) -> Option<Instant> {
        self.finish_deadline
    }

    pub fn fact(&self) -> SessionFact {
        SessionFact {
            capabilities: if self.verified {
                vec![GROUP_PIDFD_CAPABILITY.into()]
            } else {
                vec![]
            },
            workload_started: true,
            cancel_state: match self.phase {
                Phase::Ready => "ready",
                Phase::Term | Phase::Kill => "running",
                Phase::NoSignal => "group_settled",
                Phase::Complete => "settled",
                Phase::Failed => "failed",
            }
            .into(),
            cancel_error: self.result.as_ref().and_then(|r| r.as_ref().err()).cloned(),
        }
    }

    /// Called synchronously before the admitting connection can be dropped.
    pub fn start(&mut self, now: Instant, grace: Duration, settlement: Duration) -> bool {
        if self.phase != Phase::Ready {
            return false;
        }
        self.phase = Phase::Term;
        self.grace_deadline = Some(now + grace);
        self.finish_deadline = Some(now + grace + settlement);
        true
    }

    pub fn active(&self) -> bool {
        matches!(self.phase, Phase::Term | Phase::Kill | Phase::NoSignal)
    }

    pub fn finished(&self) -> bool {
        self.result.is_some()
    }

    fn fail(&mut self, error: String) {
        self.phase = Phase::Failed;
        self.result = Some(Err(error));
        // Retain authority until session expiry, but never retry a failed
        // sequence. No error is translated into group absence.
    }

    /// The broker holds its cancellation mutex for this entire step, including
    /// syscalls and handle retirement. Only an actual successful wait supplies
    /// `exit`; EOF, pidfd readiness and failed wait cannot supply it.
    pub fn step(
        &mut self,
        now: Instant,
        exit: Option<ExitResult>,
        terminal: bool,
        failure: Option<String>,
        first: bool,
    ) {
        if !self.active() {
            return;
        }
        if let Some(error) = failure {
            self.fault = Some(ControlFault::Wait);
            self.fail(error);
            return;
        }
        if now >= self.finish_deadline.expect("active operation deadline") {
            self.fault = Some(ControlFault::Deadline);
            self.fail("cancellation unresolved: settlement deadline expired; no further signals will be attempted".into());
            return;
        }
        if self.phase != Phase::NoSignal {
            let result = self.advance_group(now, exit.is_some(), first);
            if let Err(error) = result {
                self.fault = Some(ControlFault::Signal);
                self.fail(format!("cancellation unresolved: group pidfd syscall failed: {error}; no further signals will be attempted"));
                return;
            }
        }
        if self.phase == Phase::NoSignal && terminal {
            self.phase = Phase::Complete;
            self.result = Some(Ok((
                if self.signaled {
                    CancelOutcome::CancelWon
                } else {
                    CancelOutcome::AlreadyExited
                },
                exit.expect("terminal requires actual exit"),
            )));
        }
    }

    fn advance_group(&mut self, now: Instant, reaped: bool, first: bool) -> std::io::Result<()> {
        let handle = self.handle.as_ref().expect("active group authority");
        if !handle.signal(0)? && reaped {
            // A living leader can leave and recreate its group. Only this
            // conjunction makes absence permanent. Publish no-signal BEFORE
            // close, while still holding the same broker mutex.
            self.group_absent = true;
            self.phase = Phase::NoSignal;
            self.handle.take();
            return Ok(());
        }
        if first {
            self.signaled |= handle.signal(libc::SIGTERM)?;
        }
        if self.phase == Phase::Term && now >= self.grace_deadline.expect("grace deadline") {
            self.signaled |= handle.signal(libc::SIGKILL)?;
            self.phase = Phase::Kill;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Kernel {
        calls: Vec<i32>,
        exists: bool,
        error: Option<i32>,
        drops: usize,
        vanish_on_term: bool,
    }
    struct Handle(Arc<Mutex<Kernel>>);
    impl GroupSignal for Handle {
        fn signal(&self, signal: i32) -> std::io::Result<bool> {
            let mut k = self.0.lock().unwrap();
            assert_eq!(k.drops, 0, "signal after handle retirement");
            k.calls.push(signal);
            if signal == libc::SIGTERM && k.vanish_on_term {
                k.exists = false;
            }
            match k.error {
                Some(e) => Err(std::io::Error::from_raw_os_error(e)),
                None => Ok(k.exists),
            }
        }
    }
    impl Drop for Handle {
        fn drop(&mut self) {
            self.0.lock().unwrap().drops += 1;
        }
    }
    fn fixture() -> (Cancellation, Arc<Mutex<Kernel>>, Instant) {
        let kernel = Arc::new(Mutex::new(Kernel {
            exists: true,
            ..Kernel::default()
        }));
        let mut c = Cancellation::new(Ok(Box::new(Handle(kernel.clone()))));
        let now = Instant::now();
        assert!(c.start(now, Duration::from_secs(1), Duration::from_secs(2)));
        (c, kernel, now)
    }
    fn exit23() -> Option<ExitResult> {
        Some(ExitResult {
            code: Some(23),
            signal: None,
        })
    }

    #[test]
    fn one_deadline_zombies_reap_and_permanent_retirement() {
        let (mut c, k, now) = fixture();
        c.step(now, exit23(), true, None, true);
        assert!(!c.start(
            now + Duration::from_millis(900),
            Duration::from_secs(99),
            Duration::from_secs(99)
        ));
        c.step(now + Duration::from_secs(1), exit23(), true, None, false);
        assert!(
            c.result.is_none(),
            "zombie-only signal success is not settlement"
        );
        c.step(
            now + Duration::from_millis(1500),
            exit23(),
            true,
            None,
            false,
        );
        k.lock().unwrap().exists = false; // exact last-member reap
        c.step(now + Duration::from_secs(2), exit23(), true, None, false);
        let (outcome, exit) = c.result.as_ref().unwrap().as_ref().unwrap();
        assert_eq!(*outcome, CancelOutcome::CancelWon);
        assert_eq!(exit.code, Some(23));
        let calls = k.lock().unwrap().calls.clone();
        assert_eq!(calls.iter().filter(|&&s| s == libc::SIGTERM).count(), 1);
        assert_eq!(calls.iter().filter(|&&s| s == libc::SIGKILL).count(), 1);
        assert_eq!(k.lock().unwrap().drops, 1);
        c.step(now + Duration::from_secs(5), exit23(), true, None, true);
        assert!(!c.start(now, Duration::ZERO, Duration::ZERO));
        assert_eq!(calls, k.lock().unwrap().calls);
    }

    #[test]
    fn living_leader_can_recreate_absent_group() {
        let (mut c, k, now) = fixture();
        k.lock().unwrap().exists = false;
        c.step(now, None, false, None, true);
        assert_eq!(k.lock().unwrap().drops, 0);
        assert!(c.result.is_none());
        k.lock().unwrap().exists = true;
        c.step(now + Duration::from_secs(1), None, false, None, false);
        assert!(k.lock().unwrap().calls.contains(&libc::SIGKILL));
        k.lock().unwrap().exists = false;
        c.step(now + Duration::from_secs(2), exit23(), true, None, false);
        assert!(c.result.unwrap().is_ok());
    }

    #[test]
    fn absent_before_signal_preserves_natural_exit_and_waits_for_output() {
        let (mut c, k, now) = fixture();
        k.lock().unwrap().exists = false;
        c.step(now, exit23(), false, None, true);
        assert!(c.result.is_none());
        assert_eq!(c.phase, Phase::NoSignal);
        assert_eq!(k.lock().unwrap().drops, 1);
        c.step(now, exit23(), true, None, false);
        assert_eq!(c.result.unwrap().unwrap().0, CancelOutcome::AlreadyExited);
        assert_eq!(k.lock().unwrap().calls, [0]);
    }

    #[test]
    fn syscall_errors_and_failed_acquisition_never_mean_absence_or_retry() {
        for error in [
            libc::ENOSYS,
            libc::EINVAL,
            libc::EPERM,
            libc::EMFILE,
            libc::EIO,
        ] {
            let (mut c, k, now) = fixture();
            k.lock().unwrap().error = Some(error);
            c.step(now, exit23(), true, None, true);
            assert!(c.result.as_ref().unwrap().is_err());
            assert_eq!(k.lock().unwrap().drops, 0);
            assert!(!c.start(now, Duration::ZERO, Duration::ZERO));
            c.step(now, exit23(), true, None, true);
            assert_eq!(k.lock().unwrap().calls, [0]);
            let mut failed = Cancellation::new(Err(format!("workload started: {error}")));
            assert!(failed.fact().capabilities.is_empty());
            assert!(!failed.start(now, Duration::ZERO, Duration::ZERO));
            assert!(failed.fact().workload_started);
        }
    }

    #[test]
    fn group_disappears_between_zero_probe_and_signal() {
        let (mut c, k, now) = fixture();
        k.lock().unwrap().vanish_on_term = true;
        c.step(now, exit23(), true, None, true);
        assert!(c.result.is_none());
        c.step(now, exit23(), true, None, false);
        assert_eq!(c.result.unwrap().unwrap().0, CancelOutcome::AlreadyExited);
        assert_eq!(k.lock().unwrap().calls, [0, libc::SIGTERM, 0]);
    }

    #[test]
    fn per_child_verification_error_retains_handle_without_advertising_capability() {
        let k = Arc::new(Mutex::new(Kernel {
            error: Some(libc::EPERM),
            ..Kernel::default()
        }));
        let mut c = Cancellation::verify_acquired(Ok(Box::new(Handle(k.clone()))));
        assert!(c.fact().capabilities.is_empty());
        assert!(c.fact().cancel_error.unwrap().contains("workload started"));
        assert_eq!(k.lock().unwrap().drops, 0);
        assert!(!c.start(Instant::now(), Duration::ZERO, Duration::ZERO));
        drop(c);
        assert_eq!(k.lock().unwrap().drops, 1);
    }

    #[test]
    fn wait_failure_and_timeout_are_retained_errors_not_exits() {
        let (mut c, k, now) = fixture();
        c.step(now, None, false, Some("wait failed".into()), true);
        assert!(c.result.unwrap().is_err());
        assert!(k.lock().unwrap().calls.is_empty());
        let (mut c, _, now) = fixture();
        c.step(now, exit23(), true, None, true);
        c.step(now + Duration::from_secs(3), exit23(), true, None, false);
        assert!(c.result.unwrap().unwrap_err().contains("deadline expired"));
    }

    #[test]
    fn retirement_cannot_close_a_handle_during_a_syscall() {
        use std::sync::mpsc;
        struct BlockingHandle {
            entered: mpsc::Sender<()>,
            release: Mutex<mpsc::Receiver<()>>,
            dropped: Arc<std::sync::atomic::AtomicBool>,
        }
        impl GroupSignal for BlockingHandle {
            fn signal(&self, _: i32) -> std::io::Result<bool> {
                self.entered.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
                assert!(!self.dropped.load(std::sync::atomic::Ordering::SeqCst));
                Ok(false)
            }
        }
        impl Drop for BlockingHandle {
            fn drop(&mut self) {
                self.dropped
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let now = Instant::now();
        let mut c = Cancellation::new(Ok(Box::new(BlockingHandle {
            entered: entered_tx,
            release: Mutex::new(release_rx),
            dropped: dropped.clone(),
        })));
        c.start(now, Duration::from_secs(1), Duration::from_secs(2));
        let c = Arc::new(Mutex::new(c));
        let actor = c.clone();
        let worker =
            std::thread::spawn(move || actor.lock().unwrap().step(now, exit23(), true, None, true));
        entered_rx.recv().unwrap();
        assert!(c.try_lock().is_err());
        assert!(!dropped.load(std::sync::atomic::Ordering::SeqCst));
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
        assert!(c.lock().unwrap().finished());
    }
}
