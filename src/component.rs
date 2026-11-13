// Copyright © 2024-26 The Johns Hopkins Applied Physics Laboratory LLC.
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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
#[cfg(feature = "standalone")]
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;

#[cfg(feature = "standalone")]
use clap::ArgMatches;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_channels::resolve::cache::ThreadedNSNameCaches;
use constellation_common::error::MutexPoison;
use constellation_common::error::WithMutexPoison;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_common::version::FullVersion;
use constellation_common::version::Version;
use constellation_common::version::VersionSuffix;
use constellation_component_common::bus::dispatch::DispatchLargeObjBus;
use constellation_component_common::bus::dispatch::DispatchLargeObjBusStarter;
use constellation_component_common::bus::multicast::MulticastLargeObjBus;
use constellation_component_common::bus::multicast::MulticastLargeObjBusCreateError;
use constellation_component_common::bus::multicast::MulticastLargeObjBusStarter;
#[cfg(feature = "standalone")]
use constellation_standalone::Standalone;
#[cfg(feature = "standalone")]
use constellation_standalone::StandaloneService;
use constellation_streams::large_obj::LargeObjProtoCreateError;
use log::debug;
use log::error;
use log::info;
use uuid::Uuid;

use crate::clients::ClientSessionDispatch;
use crate::config::PeerConfig;
use crate::config::PeerStateConfig;
use crate::config::ProcessorClassesConfig;
#[cfg(feature = "standalone")]
use crate::config::StandaloneConfig;
use crate::consensus::ConsensusMsgs;
use crate::consensus::ConsensusRecv;
use crate::processors::ProcessorSessionDispatch;
use crate::state::InstanceEntry;
use crate::state::PeerState;
use crate::state::ProcessorEntry;
use crate::state::ProcessorIdx;
use crate::types::PeerComponentTypes;

pub struct PeerComponent<Types>
where
    Types: PeerComponentTypes {
    clients_bus: DispatchLargeObjBusStarter<Types::ClientsBusTypes, Types::Ctx>,
    processors_bus:
        DispatchLargeObjBusStarter<Types::ProcessorsBusTypes, Types::Ctx>,
    consensus_bus:
        MulticastLargeObjBusStarter<Types::ConsensusBusTypes, Types::Ctx>,
    peer_state_config: PeerStateConfig,
    processor_ids: HashMap<Types::ProcessorsPrin, ProcessorIdx>,
    processors: HashMap<Uuid, ProcessorEntry>,
    processors_notify: Notify,
    consensus_notify: Notify,
    clients_notify: Notify
}

pub struct PeerComponentCleanup<Types>
where
    Types: PeerComponentTypes {
    consensus_bus_cleanup:
        MulticastLargeObjBus<Types::ConsensusBusTypes, Types::Ctx>,
    processors_bus_cleanup:
        DispatchLargeObjBus<Types::ProcessorsBusTypes, Types::Ctx>,
    clients_bus_cleanup:
        DispatchLargeObjBus<Types::ClientsBusTypes, Types::Ctx>
}

pub struct PeerComponentErrorCleanup<Types>
where
    Types: PeerComponentTypes {
    shutdown: ShutdownFlag,
    consensus_bus_cleanup:
        Option<MulticastLargeObjBus<Types::ConsensusBusTypes, Types::Ctx>>,
    processors_bus_cleanup:
        Option<DispatchLargeObjBus<Types::ProcessorsBusTypes, Types::Ctx>>,
    clients_bus_cleanup:
        Option<DispatchLargeObjBus<Types::ClientsBusTypes, Types::Ctx>>
}

#[derive(Debug)]
pub enum PeerComponentCreateError {
    IO { err: std::io::Error },
    Duplicate { class: Uuid, instance: usize }
}

#[derive(Debug)]
pub enum PeerComponentRunError<Processor, Client, Consensus> {
    ProcessorBus { err: Processor },
    ClientBus { err: Client },
    ConsensusBus { err: Consensus },
    ProcessorStart { err: std::io::Error },
    ClientStart { err: std::io::Error },
    ConsensusStart { err: std::io::Error }
}

impl<Types> PeerComponent<Types>
where
    Types: 'static + PeerComponentTypes
{
    pub fn create(
        config: PeerConfig<
            Types::ClientsHashConfig,
            Types::ClientsSessAuthConfig,
            Types::ClientsMsgAuthConfig,
            Types::ClientsLargeObjMsgAuthConfig,
            Types::ClientsUnixConfig,
            Types::ClientsUDPConfig,
            Types::ClientsIDsConfig,
            Types::ClientsEpochsConfig,
            Types::ProcessorsPrin,
            Types::ProcessorsHashConfig,
            Types::ProcessorsSessAuthConfig,
            Types::ProcessorsMsgAuthConfig,
            Types::ProcessorsLargeObjMsgAuthConfig,
            Types::ProcessorsUnixConfig,
            Types::ProcessorsUDPConfig,
            Types::ProcessorsIDsConfig,
            Types::ProcessorsEpochsConfig,
            Types::ConsensusPrin,
            Types::ConsensusHashConfig,
            Types::ConsensusSessAuthConfig,
            Types::ConsensusLargeObjMsgAuthConfig,
            Types::ConsensusUnixConfig,
            Types::ConsensusUDPConfig,
            Types::ConsensusIDsConfig,
            Types::ConsensusEpochsConfig,
            Types::ConsensusResolverConfig,
            Types::ConsensusMsgAuthConfig
        >,
        ctx: Types::Ctx
    ) -> Result<Self, PeerComponentCreateError> {
        let (
            clients_config,
            processors_config,
            consensus_config,
            peer_state_config
        ) = config.take();
        let (processors_bus_config, classes_config) = processors_config.take();
        let (processors_bus, processors_notify) =
            DispatchLargeObjBusStarter::create(
                processors_bus_config,
                ctx.clone()
            )
            .map_err(|err| PeerComponentCreateError::IO { err: err })?;
        let clients_bus_config = clients_config.take();
        let (clients_bus, clients_notify) =
            DispatchLargeObjBusStarter::create(clients_bus_config, ctx.clone())
                .map_err(|err| PeerComponentCreateError::IO { err: err })?;
        let consensus_bus_config = consensus_config.take();
        let (consensus_bus, consensus_notify) =
            MulticastLargeObjBusStarter::create(
                consensus_bus_config,
                ctx.clone()
            )
            .map_err(|err| PeerComponentCreateError::IO { err: err })?;
        let ProcessorClassesConfig::Static {
            stat: processor_configs
        } = classes_config;
        let mut processor_ids: HashMap<Types::ProcessorsPrin, ProcessorIdx> =
            HashMap::with_capacity(processor_configs.len());
        let mut classes = HashMap::with_capacity(processor_configs.len());
        let mut curr_id = 0;

        // Build the configuration structure for processors.
        for processor_config in processor_configs {
            let (prin, class_configs) = processor_config.take();

            debug!(target: "peer-standalone",
                   "registering transaction classes for processor {}",
                   prin);

            let id = match processor_ids.get(&prin) {
                Some(id) => id.clone(),
                None => {
                    let idx = ProcessorIdx::from(curr_id);

                    curr_id += 1;
                    processor_ids.insert(prin.clone(), idx.clone());

                    idx
                }
            };

            for class_config in class_configs {
                let (classname, instances, versions) = class_config.take();
                let versions = versions.into_iter().map(|config| config.into());
                let class: Uuid = classname.into();
                let instance = InstanceEntry::new(id.clone(), versions);

                debug!(target: "peer-standalone",
                       "registering transaction class {} for processor {}",
                       class, prin);

                match classes.entry(class) {
                    Entry::Vacant(ent) => {
                        let ent = ent.insert(HashMap::new());

                        for i in instances {
                            ent.insert(i as u64, instance.clone());
                        }
                    }
                    Entry::Occupied(mut ent) => {
                        for i in instances {
                            if ent
                                .get_mut()
                                .insert(i as u64, instance.clone())
                                .is_some()
                            {
                                return Err(
                                    PeerComponentCreateError::Duplicate {
                                        instance: i,
                                        class: class
                                    }
                                );
                            }
                        }
                    }
                }
            }
        }

        let processors = classes
            .into_iter()
            .map(|(class, mut instances)| {
                instances.shrink_to_fit();

                (class, ProcessorEntry::new(instances))
            })
            .collect();

        Ok(PeerComponent {
            clients_bus: clients_bus,
            processors_bus: processors_bus,
            consensus_bus: consensus_bus,
            peer_state_config: peer_state_config,
            processor_ids: processor_ids,
            processors: processors,
            processors_notify: processors_notify,
            consensus_notify: consensus_notify,
            clients_notify: clients_notify
        })
    }

    pub fn start(
        self,
        shutdown: ShutdownFlag
    ) -> Result<
        PeerComponentCleanup<Types>,
        (
            PeerComponentErrorCleanup<Types>,
            PeerComponentRunError<
                WithMutexPoison<std::io::Error>,
                WithMutexPoison<std::io::Error>,
                MulticastLargeObjBusCreateError<
                    Types::ConsensusHashCreateError,
                    Types::ConsensusMsgAuthCreateError,
                    LargeObjProtoCreateError<
                        Infallible,
                        Infallible,
                        Types::ConsensusIDsCreateError
                    >,
                    MutexPoison
                >
            >
        )
    > {
        let PeerComponent {
            clients_bus,
            processors_bus,
            consensus_bus,
            peer_state_config,
            processor_ids,
            processors,
            processors_notify,
            consensus_notify,
            clients_notify
        } = self;

        info!(target: "peer-component",
              "starting peer component");

        let state = PeerState::new(
            peer_state_config,
            processors,
            consensus_notify,
            processors_notify,
            clients_notify
        );
        let state = Arc::new(state);
        let processor_dispatch =
            ProcessorSessionDispatch::new(processor_ids, state.clone());
        let client_dispatch = ClientSessionDispatch::new(state.clone());
        let consensus_recv = ConsensusRecv::new(state.clone());
        let consensus_msgs = ConsensusMsgs::new(state.clone());

        match processors_bus.start(processor_dispatch, shutdown.clone()) {
            Ok(processors_bus) => match clients_bus
                .start(client_dispatch, shutdown.clone())
            {
                Ok(clients_bus) => match consensus_bus.start(
                    consensus_recv,
                    consensus_msgs,
                    shutdown.clone()
                ) {
                    Ok((consensus_bus, _)) => Ok(PeerComponentCleanup {
                        consensus_bus_cleanup: consensus_bus,
                        processors_bus_cleanup: processors_bus,
                        clients_bus_cleanup: clients_bus
                    }),
                    Err(err) => {
                        let cleanup = PeerComponentErrorCleanup {
                            consensus_bus_cleanup: None,
                            processors_bus_cleanup: Some(processors_bus),
                            clients_bus_cleanup: Some(clients_bus),
                            shutdown: shutdown
                        };
                        let err =
                            PeerComponentRunError::ConsensusBus { err: err };

                        Err((cleanup, err))
                    }
                },
                Err(err) => {
                    let cleanup = PeerComponentErrorCleanup {
                        consensus_bus_cleanup: None,
                        processors_bus_cleanup: Some(processors_bus),
                        clients_bus_cleanup: None,
                        shutdown: shutdown
                    };
                    let err = PeerComponentRunError::ClientBus { err: err };

                    Err((cleanup, err))
                }
            },
            Err(err) => {
                let cleanup = PeerComponentErrorCleanup {
                    consensus_bus_cleanup: None,
                    processors_bus_cleanup: None,
                    clients_bus_cleanup: None,
                    shutdown: shutdown
                };
                let err = PeerComponentRunError::ProcessorBus { err: err };

                Err((cleanup, err))
            }
        }
    }
}

impl<Types> PeerComponentErrorCleanup<Types>
where
    Types: PeerComponentTypes
{
    pub fn cleanup(self) {
        let PeerComponentErrorCleanup {
            clients_bus_cleanup,
            processors_bus_cleanup,
            consensus_bus_cleanup,
            mut shutdown
        } = self;

        if let Err(err) = shutdown.set() {
            error!(target: "peer-standalone-shutdown",
                   "error setting shutdown flag: {}",
                   err)
        } else {
            if let Some(clients_bus_cleanup) = clients_bus_cleanup {
                clients_bus_cleanup.cleanup();
            }

            if let Some(processors_bus_cleanup) = processors_bus_cleanup {
                processors_bus_cleanup.cleanup();
            }

            if let Some(consensus_bus_cleanup) = consensus_bus_cleanup {
                consensus_bus_cleanup.cleanup();
            }
        }
    }
}

impl<Types> PeerComponentCleanup<Types>
where
    Types: PeerComponentTypes
{
    pub fn cleanup(self) {
        let PeerComponentCleanup {
            clients_bus_cleanup,
            processors_bus_cleanup,
            consensus_bus_cleanup
        } = self;

        clients_bus_cleanup.cleanup();
        processors_bus_cleanup.cleanup();
        consensus_bus_cleanup.cleanup();
    }
}

impl Display for PeerComponentCreateError {
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            PeerComponentCreateError::IO { err } => write!(f, "{}", err),
            PeerComponentCreateError::Duplicate { class, instance } => write!(
                f,
                "duplicate processor for class {}, instance {}",
                class, instance
            )
        }
    }
}

impl<Processor, Client, Consensus> Display
    for PeerComponentRunError<Processor, Client, Consensus>
where
    Processor: Display,
    Client: Display,
    Consensus: Display
{
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            PeerComponentRunError::ProcessorBus { err } => err.fmt(f),
            PeerComponentRunError::ClientBus { err } => err.fmt(f),
            PeerComponentRunError::ConsensusBus { err } => err.fmt(f),
            PeerComponentRunError::ProcessorStart { err } => {
                write!(f, "{}", err)
            }
            PeerComponentRunError::ClientStart { err } => write!(f, "{}", err),
            PeerComponentRunError::ConsensusStart { err } => {
                write!(f, "{}", err)
            }
        }
    }
}

#[cfg(feature = "standalone")]
#[derive(Clone)]
pub struct StandaloneCtx {
    caches: ThreadedNSNameCaches
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
pub struct StandaloneCreateCleanup {
    shutdown: ShutdownFlag,
    caches_join: JoinHandle<()>
}

#[cfg(feature = "standalone")]
pub struct StandaloneCreateErrorCleanup {
    caches: Option<StandaloneCreateCleanup>
}

#[cfg(feature = "standalone")]
impl<Types> Standalone for PeerComponent<Types>
where
    Types: 'static + PeerComponentTypes<Ctx = StandaloneCtx>
{
    type Config = StandaloneConfig<
        Types::ClientsHashConfig,
        Types::ClientsSessAuthConfig,
        Types::ClientsMsgAuthConfig,
        Types::ClientsLargeObjMsgAuthConfig,
        Types::ClientsUnixConfig,
        Types::ClientsUDPConfig,
        Types::ClientsIDsConfig,
        Types::ClientsEpochsConfig,
        Types::ProcessorsPrin,
        Types::ProcessorsHashConfig,
        Types::ProcessorsSessAuthConfig,
        Types::ProcessorsMsgAuthConfig,
        Types::ProcessorsLargeObjMsgAuthConfig,
        Types::ProcessorsUnixConfig,
        Types::ProcessorsUDPConfig,
        Types::ProcessorsIDsConfig,
        Types::ProcessorsEpochsConfig,
        Types::ConsensusPrin,
        Types::ConsensusHashConfig,
        Types::ConsensusSessAuthConfig,
        Types::ConsensusLargeObjMsgAuthConfig,
        Types::ConsensusUnixConfig,
        Types::ConsensusUDPConfig,
        Types::ConsensusIDsConfig,
        Types::ConsensusEpochsConfig,
        Types::ConsensusResolverConfig,
        Types::ConsensusMsgAuthConfig
    >;
    type CreateCleanup = StandaloneCreateCleanup;
    type CreateErrorCleanup = StandaloneCreateErrorCleanup;

    const CONFIG_FILES: &[&str] = &["peer.conf"];
    const NAME: &str = "peer";
    const VERSION: FullVersion = FullVersion::new(
        None,
        Version::new(0, 0, 0),
        Some(VersionSuffix::Development)
    );

    fn cleanup_create_err(create: Self::CreateErrorCleanup) {
        if let Some(cleanup) = create.caches {
            let StandaloneCreateCleanup {
                caches_join,
                mut shutdown
            } = cleanup;

            debug!(target: "peer-standalone-shutdown",
                   "cleaning up caches");

            if let Err(err) = shutdown.set() {
                error!(target: "peer-standalone-shutdown",
                       "error setting shutdown flag: {}",
                       err)
            } else if caches_join.join().is_err() {
                error!(target: "peer-standalone-shutdown",
                       "error shutting down name chache threads")
            }
        }
    }

    fn create(
        _args: ArgMatches,
        config: Self::Config,
        shutdown: &ShutdownFlag
    ) -> Result<(Self, Self::CreateCleanup), Self::CreateErrorCleanup> {
        let (name_caches_config, component_config) = config.take();
        let (caches, caches_join) =
            ThreadedNSNameCaches::create(name_caches_config, shutdown.clone())
                .map_err(|err| {
                    error!(target: "peer-standalone",
                       "error creating name caches: {}",
                       err);

                    StandaloneCreateErrorCleanup { caches: None }
                })?;
        let ctx = StandaloneCtx { caches: caches };
        let cleanup = StandaloneCreateCleanup {
            shutdown: shutdown.clone(),
            caches_join: caches_join
        };

        match PeerComponent::create(component_config, ctx) {
            Ok(out) => Ok((out, cleanup)),
            Err(err) => {
                error!(target: "peer-standalone",
                       "error creating component: {}",
                       err);

                Err(StandaloneCreateErrorCleanup {
                    caches: Some(cleanup)
                })
            }
        }
    }
}

#[cfg(feature = "standalone")]
impl<Types> StandaloneService for PeerComponent<Types>
where
    Types: 'static + PeerComponentTypes<Ctx = StandaloneCtx>
{
    type RunCleanup = PeerComponentCleanup<Types>;
    type RunErrorCleanup = PeerComponentErrorCleanup<Types>;

    const DEFAULT_PIDFILE: &str = "/tmp/constellation-peer.pid";

    fn run(
        self,
        _rundir: PathBuf,
        shutdown: ShutdownFlag
    ) -> Result<Self::RunCleanup, Self::RunErrorCleanup> {
        self.start(shutdown).map_err(|(cleanup, err)| {
            error!(target: "peer-standalone",
                       "error running component: {}",
                       err);

            cleanup
        })
    }

    fn join(
        create_cleanup: Self::CreateCleanup,
        run_cleanup: Self::RunCleanup
    ) {
        run_cleanup.cleanup();

        if create_cleanup.caches_join.join().is_err() {
            error!(target: "peer-standalone-shutdown",
                   "error shutting down name chache threads")
        }
    }

    fn shutdown_err(
        mut create_cleanup: Self::CreateCleanup,
        run_cleanup: Self::RunErrorCleanup
    ) {
        run_cleanup.cleanup();

        if let Err(err) = create_cleanup.shutdown.set() {
            error!(target: "peer-standalone-shutdown",
                   "error setting shutdown flag: {}",
                   err)
        } else if create_cleanup.caches_join.join().is_err() {
            error!(target: "peer-standalone-shutdown",
                   "error shutting down name chache threads")
        }
    }
}

#[cfg(feature = "standalone")]
use crate::types::StandalonePeerTypes;

#[cfg(feature = "standalone")]
pub type StandalonePeerComponent = PeerComponent<StandalonePeerTypes>;
