//! Original-leader identity, never a saved numeric process-group lookup.
use std::io;

pub trait GroupSignal: Send + Sync {
    /// false means ESRCH only; success includes zombie-only groups.
    fn signal(&self, signal: i32) -> io::Result<bool>;
}

#[cfg(target_os = "linux")]
pub struct GroupPidfd(std::os::fd::OwnedFd);

#[cfg(not(target_os = "linux"))]
pub struct GroupPidfd;

impl GroupPidfd {
    /// Caller owns an unreaped original leader, with normal SIGCHLD semantics
    /// and no competing reaper. Call before passing Child to ANY wait adapter.
    #[cfg(target_os = "linux")]
    pub fn acquire(pid: u32) -> io::Result<Self> {
        use std::os::fd::FromRawFd;
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0_u32) };
        if fd == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(unsafe {
            std::os::fd::OwnedFd::from_raw_fd(fd as i32)
        }))
    }

    #[cfg(not(target_os = "linux"))]
    pub fn acquire(_pid: u32) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "group pidfds require Linux",
        ))
    }

    /// Read only the child-ownership policy. Also applicable to the creating
    /// proxy, which need not be a process-group leader. Never mutate SIGCHLD.
    pub fn check_sigchld_policy() -> io::Result<()> {
        #[cfg(target_os = "linux")]
        unsafe {
            let mut action: libc::sigaction = std::mem::zeroed();
            if libc::sigaction(libc::SIGCHLD, std::ptr::null(), &mut action) == -1 {
                return Err(io::Error::last_os_error());
            }
            if action.sa_sigaction != libc::SIG_DFL || action.sa_flags & libc::SA_NOCLDWAIT != 0 {
                return Err(io::Error::other(
                    "group pidfd creation requires default SIGCHLD without SA_NOCLDWAIT",
                ));
            }
        }
        Ok(())
    }

    /// The detached broker is itself a session/group leader. Probe its own
    /// group with signal zero, never a workload or a host-wide signal. Verify
    /// SIGCHLD before spawn; this standalone broker installs no other reaper.
    pub fn preflight() -> io::Result<()> {
        Self::check_sigchld_policy()?;
        if !Self::acquire(std::process::id())?.signal(0)? {
            return Err(io::Error::other(
                "broker must be its own process-group leader",
            ));
        }
        Ok(())
    }
}

impl GroupSignal for GroupPidfd {
    fn signal(&self, signal: i32) -> io::Result<bool> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            const PIDFD_SIGNAL_PROCESS_GROUP: u32 = 4;
            // &self holds the OwnedFd across the complete syscall. The only
            // caller serializes retirement and calls under the same mutex.
            let result = unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    self.0.as_raw_fd(),
                    signal,
                    std::ptr::null::<libc::siginfo_t>(),
                    PIDFD_SIGNAL_PROCESS_GROUP,
                )
            };
            if result == 0 {
                return Ok(true);
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                Ok(false)
            } else {
                Err(error)
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = signal;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "group pidfds require Linux",
            ))
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::os::unix::process::CommandExt;

    #[test]
    fn acquire_after_confirmed_instant_exit_before_reap() {
        let mut command = std::process::Command::new("/bin/sh");
        command.args(["-c", "exit 23"]);
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().unwrap();
        // WNOWAIT is only a test barrier. Production continues normal reap
        // after acquisition and never depends on an unreaped zombie anchor.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe {
                libc::waitid(
                    libc::P_PID,
                    child.id(),
                    &mut info,
                    libc::WEXITED | libc::WNOWAIT,
                )
            },
            0
        );
        let handle = GroupPidfd::acquire(child.id());
        let supported = handle.as_ref().is_ok_and(|fd| fd.signal(0).is_ok());
        assert_eq!(child.wait().unwrap().code(), Some(23));
        if !supported {
            assert!(
                std::env::var_os("PIPEKEEP_REQUIRE_GROUP_PIDFD_TESTS").is_none(),
                "required real group pidfd support unavailable"
            );
            eprintln!("UNSUPPORTED instant-exit pidfd test: kernel syscall unavailable");
            return;
        }
        let handle = handle.unwrap();
        assert!(!handle.signal(0).unwrap());
        assert!(!handle.signal(libc::SIGTERM).unwrap());
        assert!(!handle.signal(libc::SIGKILL).unwrap());
    }
}
