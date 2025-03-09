// Copyright © 2024-25 The Johns Hopkins Applied Physics Laboratory LLC.
//
// This program is free software: you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.  If you
// would like to purchase a commercial license for this software, please
// contact APL’s Tech Transfer at 240-592-0817 or
// techtransfer@jhuapl.edu.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public
// License along with this program.  If not, see
// <https://www.gnu.org/licenses/>.

use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::sync::Arc;
use std::thread::JoinHandle;

#[cfg(feature = "standalone")]
use clap::ArgMatches;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_channels::resolve::cache::ThreadedNSNameCaches;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::version::FullVersion;
use constellation_common::version::Version;
use constellation_common::version::VersionSuffix;
#[cfg(feature = "standalone")]
use constellation_standalone::Standalone;
#[cfg(feature = "standalone")]
use constellation_standalone::StandaloneService;
use log::debug;
use log::error;
use log::info;

#[cfg(feature = "standalone")]
use crate::config::StandaloneConfig;

pub struct PeerComponent<Ctx>
where
    Ctx: 'static
        + NSNameCachesCtx
        + Send
        + Sync {
    ctx: Ctx
}

pub struct PeerComponentCleanup;

pub enum PeerComponentRunError {
}

#[cfg(feature = "standalone")]
pub struct StandaloneCtx {
    caches: ThreadedNSNameCaches,
}

#[cfg(feature = "standalone")]
pub struct StandaloneCreateCleanup {
    shutdown: ShutdownFlag,
    caches_join: JoinHandle<()>
}

impl<Ctx> PeerComponent<Ctx>
where
    Ctx: 'static
        + NSNameCachesCtx
        + Send
        + Sync {
    pub fn start(
        self
    ) -> Result<PeerComponentCleanup, PeerComponentRunError> {

        info!(target: "peer-component",
              "starting peer component");

        Ok(PeerComponentCleanup)
    }
}

impl PeerComponentCleanup {
    pub fn cleanup(self) {
    }
}

#[cfg(feature = "standalone")]
impl NSNameCachesCtx for StandaloneCtx {
    /// Exact type of name caches.
    type NameCaches = ThreadedNSNameCaches;

    #[inline]
    fn name_caches(&mut self) -> &mut Self::NameCaches {
        &mut self.caches
    }
}

#[cfg(feature = "standalone")]
impl Standalone for PeerComponent<StandaloneCtx>
{
    type Config = StandaloneConfig;
    type CreateCleanup = StandaloneCreateCleanup;

    const NAME: &str = "peer";
    const CONFIG_FILES: &[&str] = &["peer.conf"];
    const VERSION: FullVersion = FullVersion::new(
        None,
        Version::new(0, 0, 0),
        Some(VersionSuffix::Development)
    );

    fn create(
        _args: ArgMatches,
        config: Self::Config
    ) -> Result<(Self, Self::CreateCleanup), Self::CreateCleanup> {
        let name_caches_config = config.take();
        let shutdown = ShutdownFlag::new();
        let (caches, caches_join) =
            ThreadedNSNameCaches::create(name_caches_config, shutdown.clone());
        let cleanup = StandaloneCreateCleanup {
            shutdown: shutdown.clone(),
            caches_join: caches_join
        };
        let ctx = StandaloneCtx {
            caches: caches
        };
        let peer = PeerComponent {
            ctx: ctx
        };

        Ok((peer, cleanup))
    }
}

impl StandaloneService for PeerComponent<StandaloneCtx>
{
    type RunCleanup = PeerComponentCleanup;
    type RunErrorCleanup = ();

    fn run(self) -> Result<Self::RunCleanup, Self::RunErrorCleanup> {
        match self.start() {
            Ok(out) => Ok(out),
            Err(err) => {
                error!(target: "peer-component",
                       "{}", err);

                Err(())
            }
        }
    }

    fn shutdown(
        mut create_cleanup: Self::CreateCleanup,
        run_cleanup: Option<Self::RunCleanup>
    ) {
        debug!(target: "peer-standalone",
               "cleaning up peer");

        create_cleanup.shutdown.set();

        if let Some(cleanup) = run_cleanup {
            debug!(target: "peer-standalone",
               "cleaning up runtime");

            cleanup.cleanup();
        }

        debug!(target: "peer-standalone",
               "cleaning up caches");

        if create_cleanup.caches_join.join().is_err() {
            error!(target: "standalone-shutdown",
                   "error shutting down name chache threads")
        }
    }

    fn shutdown_err(
        mut create_cleanup: Self::CreateCleanup,
        _run_cleanup: ()
    ) {
        debug!(target: "peer-standalone",
               "cleaning up peer");

        create_cleanup.shutdown.set();

        if create_cleanup.caches_join.join().is_err() {
            error!(target: "standalone-shutdown",
                   "error shutting down name chache threads")
        }
    }
}

impl Display for PeerComponentRunError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "failed to start consensus component")
    }
}
