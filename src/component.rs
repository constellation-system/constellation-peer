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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::thread::JoinHandle;

#[cfg(feature = "standalone")]
use clap::ArgMatches;
use constellation_auth::authn::MsgAuthN;
use constellation_auth::authn::PassthruMsgAuthN;
use constellation_auth::authn::SessionAuthN;
use constellation_auth::config::TestCredConfig;
use constellation_channels::config::CompoundFarEndpoint;
use constellation_channels::config::ResolverConfig;
use constellation_channels::far::compound::CompoundFarChannel;
use constellation_channels::far::compound::CompoundFarChannelXfrm;
use constellation_channels::far::udp::UDPDatagramXfrm;
use constellation_channels::far::unix::UnixDatagramXfrm;
use constellation_channels::far::FarChannelAcquired;
use constellation_channels::far::FarChannelAcquiredResolve;
use constellation_channels::far::FarChannelCreate;
use constellation_channels::far::FarChannelFlowsError;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_channels::resolve::cache::ThreadedNSNameCaches;
use constellation_channels::resolve::MixedResolver;
use constellation_common::codec::Decoder;
use constellation_common::codec::Encoder;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_common::hashid::SHA3Algo;
use constellation_common::hashid::SHA3ID;
use constellation_common::ids::AscendingCount;
use constellation_common::net::DatagramXfrm;
use constellation_common::net::DatagramXfrmCreate;
use constellation_common::net::IPEndpointAddr;
use constellation_common::net::Socket;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::version::FullVersion;
use constellation_common::version::Version;
use constellation_common::version::VersionSuffix;
use constellation_component_common::config::MulticastLargeObjBusConfig;
use constellation_component_common::config::PartiesConfig;
use constellation_component_common::consensus_ctl::ConsensusCtl;
use constellation_component_common::consensus_ctl::ConsensusCtlCodec;
use constellation_component_common::xact::XactBatchBlobCodec;
use constellation_component_common::xact::XactBlobBatch;
#[cfg(feature = "standalone")]
use constellation_standalone::Standalone;
#[cfg(feature = "standalone")]
use constellation_standalone::StandaloneService;
use constellation_streams::addrs::Addrs;
use constellation_streams::addrs::AddrsCreate;
use constellation_streams::channels::ChannelParam;
use constellation_streams::config::LargeObjProtoConfig;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::large_obj::LargeObjMsg;
use constellation_streams::large_obj::LargeObjMsgCodec;
use constellation_streams::large_obj::LargeObjProto;
use constellation_streams::large_obj::LargeObjProtoCreateError;
use constellation_streams::select::StreamSelectorCreateError;
use constellation_streams::select::ThreadedStreamSelectorError;
use constellation_streams::stream::StreamID;
use log::debug;
use log::error;
use log::info;
use log::warn;
use uuid::Uuid;

use crate::clients::ClientSessionDispatch;
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

pub trait PeerComponentTypes {
}

// XXX this is going to need separate session authenticators, and
// ultimately a whole separate instantiation of the flows types.  This
// is best done with type traits, when we get to that.
pub struct PeerComponent<Types>
where Types: PeerComponentTypes {
    client_comm_config: DispatchLargeObjBusConfig<Epochs::Config>,
    client_large_obj_config: LargeObjProtoConfig<
        <XactBatchBlobCodec<u128, H, TestSeal, TestSealCodec> as Codec<
            XactBlobBatch<u128, H::HashID, TestSeal>
        >>::Param,
        IDs::Config
    >,
    processor_comm_config: DispatchLargeObjBusConfig<Epochs::Config>,
    processor_large_obj_config: LargeObjProtoConfig<
        <XactBatchBlobCodec<u128, H, TestSeal, TestSealCodec> as Codec<
            XactBlobBatch<u128, H::HashID, TestSeal>
        >>::Param,
        IDs::Config
    >,
    consensus_comm_config: MulticastLargeObjBusConfig<
        SessionAuth::Prin,
        ChannelRegistryChannelsConfig<()>,
        Epochs::Config,
        Endpoint
    >,
    consensus_large_obj_config: LargeObjProtoConfig<
        <ConsensusCtlCodec<u128, H, TestSeal, TestSealCodec> as Codec<
            ConsensusCtl<u128, H::HashID, TestSeal>
        >>::Param,
        IDs::Config
    >,
    peer_state_config: PeerStateConfig,
    client_listener: ThreadedFlowsListener<
        <Channel::Nego as OwnedFlowNegotiator<F::Flow>>::Flow,
        StreamID<
            <Channel::Xfrm as DatagramXfrm>::PeerAddr,
            F::ChannelID,
            Channel::Param
        >,
        SessionAuth::Prin
    >,
    processor_listener: ThreadedFlowsListener<
        <Channel::Nego as OwnedFlowNegotiator<F::Flow>>::Flow,
        StreamID<
            <Channel::Xfrm as DatagramXfrm>::PeerAddr,
            F::ChannelID,
            Channel::Param
        >,
        SessionAuth::Prin
    >,
    consensus_listener: ThreadedFlowsListener<
        <Channel::Nego as OwnedFlowNegotiator<F::Flow>>::Flow,
        StreamID<
            <Channel::Xfrm as DatagramXfrm>::PeerAddr,
            F::ChannelID,
            Channel::Param
        >,
        SessionAuth::Prin
    >,
    // XXX this should be a session principal for processors.
    processor_ids: HashMap<XactMsgAuth::Prin, ProcessorIdx>,
    processors: HashMap<Uuid, ProcessorEntry>,
    consensus_authn: CtlMsgAuth,
    shutdown: ShutdownFlag,
    consensus_ctx: Ctx,
    processors_ctx: Ctx,
    client_ctx: Ctx
}

pub struct PeerComponentCleanup {
    shutdown: ShutdownFlag,
    consensus_comm_cleanup: MulticastLargeObjBusCleanup,
    processor_comm_cleanup: DispatchLargeObjBusCleanup,
    client_comm_cleanup: DispatchLargeObjBusCleanup
}

pub enum PeerComponentRunError<Acquire, Consensus, C> {
    ClientComm {
        err: DispatchLargeObjBusCreateError<
            <LargeObjMsgCodec<SHA3Algo> as
             Codec<LargeObjMsg<SHA3ID>>>::CreateError,
            Acquire
        >
    },
    ProcessorComm {
        err: DispatchLargeObjBusCreateError<
            <LargeObjMsgCodec<SHA3Algo> as
             Codec<LargeObjMsg<SHA3ID>>>::CreateError,
            Acquire
        >
    },
    ConsensusComm {
        err: Consensus
    },
    LargeObj {
        err: LargeObjProtoCreateError<C>
    },
    ClientStart {
        err: std::io::Error
    },
    ProcessorStart {
        err: std::io::Error
    },
    ConsensusStart {
        err: std::io::Error
    }
}

#[cfg(feature = "standalone")]
pub type StandaloneRegistry = CompoundFarChannelRegistry<
    Arc<TestAuthN<String, TestCred>>,
    UnixDatagramXfrm,
    UDPDatagramXfrm,
    FarChannelRegistryID
>;

#[cfg(feature = "standalone")]
#[derive(Clone)]
pub struct StandaloneCtx {
    caches: ThreadedNSNameCaches,
    registry: Arc<StandaloneRegistry>
}

#[cfg(feature = "standalone")]
pub struct StandaloneCreateCleanup {
    shutdown: ShutdownFlag,
    caches_join: JoinHandle<()>
}

impl<
        XactWrapper,
        XactWrapperCodec,
        H,
        IDs,
        XactMsgAuth,
        CtlMsgAuth,
        Epochs,
        Channel,
        F,
        SessionAuth,
        Xfrm,
        Resolver,
        Endpoint,
        Ctx
    >
    PeerComponent<
        XactWrapper,
        XactWrapperCodec,
        H,
        IDs,
        XactMsgAuth,
        CtlMsgAuth,
        Epochs,
        Channel,
        F,
        SessionAuth,
        Xfrm,
        Resolver,
        Endpoint,
        Ctx
    >
where
    XactWrapper: 'static + Clone + Send,
    XactMsgAuth: 'static
        + Clone
        + MsgAuthN<
            XactBlobBatch<u128, H::HashID, TestSeal>,
            XactWrapper,
            SessionPrin = SessionAuth::Prin,
            Prin = SessionAuth::Prin
        >
        + Send,
    XactMsgAuth::SessionPrin: Send + Sync,
    CtlMsgAuth: 'static
        + Clone
        + MsgAuthN<
            ConsensusCtl<u128, H::HashID, TestSeal>,
            ConsensusCtl<u128, H::HashID, TestSeal>,
            SessionPrin = SessionAuth::Prin,
            Prin = SessionAuth::Prin
        >
        + Send,
    CtlMsgAuth::SessionPrin: Send + Sync,
    IDs: 'static + Clone + IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    H: 'static + Clone + Default + HashAlgo + Send,
    H::HashID: 'static + Clone + Display + Hash + HashID + Eq + Send,
    SessionAuth: 'static
        + Clone
        + SessionAuthN<<Channel::Nego as OwnedFlowNegotiator<F::Flow>>::Flow>
        + Send
        + Sync,
    SessionAuth::Prin: 'static + Clone + Display + Eq + Hash + Send + Sync,
    XactWrapperCodec: 'static + Clone + Codec<XactWrapper> + Send,
    <XactWrapperCodec as Codec<XactWrapper>>::Param: Default,
    Epochs: 'static + Default + IDGen + Iterator<Item = u128> + Send + Sync,
    Epochs::Config: Clone + Send,
    Channel: 'static
        + FarChannelOwnedFlows<F, SessionAuth, Xfrm>
        + FarChannelCreate
        + Send
        + Sync,
    Channel::Acquired: FarChannelAcquiredResolve<Resolved = Channel::Param>,
    Channel::Param: Clone
        + Display
        + Eq
        + Hash
        + PartialEq
        + ChannelParam<<Channel::Xfrm as DatagramXfrm>::PeerAddr>
        + Send
        + Sync,
    Channel::Acquired:
        FarChannelAcquiredResolve<Resolved = Channel::Param> + Send + Sync,
    <Channel::Nego as OwnedFlowNegotiator<F::Flow>>::Flow:
        'static + ConcurrentStream + Send,
    <Channel::Xfrm as DatagramXfrm>::PeerAddr:
        'static + Eq + Hash + Send + Sync,
    F: 'static
        + OwnedFlowsCreate<
            Channel::Socket,
            Channel::Nego,
            SessionAuth,
            Channel::Xfrm
        >
        + Send,
    F::Flow: 'static + ConcurrentStream + Send,
    F::CreateParam: Clone + Default + Send + Sync,
    F::Reporter: Clone + Send + Sync,
    F::ChannelID: 'static + From<usize> + Into<usize> + Send + Sync,
    Xfrm: 'static
        + DatagramXfrm
        + DatagramXfrmCreate<Addr = Channel::Param>
        + Send
        + Sync,
    Xfrm::CreateParam: Clone + Default + Send + Sync,
    Xfrm::LocalAddr: From<<Channel::Socket as Socket>::Addr>,
    Resolver: 'static
        + Addrs<Addr = <Channel::Xfrm as DatagramXfrm>::PeerAddr>
        + AddrsCreate<Ctx, Vec<Endpoint>, Config = ResolverConfig>
        + Send
        + Sync,
    Resolver::Origin:
        Clone + Eq + Hash + Into<Option<IPEndpointAddr>> + Send + Sync,
    Endpoint: 'static + Clone + Send + Sync,
    Ctx: 'static
        + Clone
        + FarChannelRegistryCtx<Channel, F, SessionAuth, Xfrm>
        + NSNameCachesCtx
        + Send
        + Sync
{
    pub fn start(
        self
    ) -> Result<
        PeerComponentCleanup,
        PeerComponentRunError<
           FarChannelRegistryAcquireError<
                RegistryAcquireError<
                    Channel::AcquireError,
                    <Channel::Acquired as FarChannelAcquiredResolve>::ResolverError,
                    FarChannelFlowsError<
                        Channel::SocketError,
                        F::CreateError,
                        Channel::XfrmError
                    >,
                    <Channel::Acquired as FarChannelAcquired>::WrapError
                >
            >,
            MulticastLargeObjBusRunError<
                FarChannelRegistryAcquireError<
                    RegistryAcquireError<
                        Channel::AcquireError,
                        <Channel::Acquired as FarChannelAcquiredResolve>::ResolverError,
                        FarChannelFlowsError<
                            Channel::SocketError,
                            F::CreateError,
                            Channel::XfrmError
                        >,
                        <Channel::Acquired as FarChannelAcquired>::WrapError
                    >
                >,
                <LargeObjMsgCodec<H> as Codec<LargeObjMsg<H::HashID>>>::CreateError,
                StreamSelectorCreateError<
                    FarChannelRegistryChannelsCreateError<
                        <LargeObjMsgCodec<H> as Codec<LargeObjMsg<H::HashID>>>::CreateError
                    >,
                    Resolver::CreateError
                >,
                ThreadedStreamSelectorError<
                    Resolver::AddrsError,
                    FarChannelRegistryAcquireError<
                        RegistryAcquireError<
                            Channel::AcquireError,
                            <Channel::Acquired as FarChannelAcquiredResolve>::ResolverError,
                            FarChannelFlowsError<
                                Channel::SocketError,
                                F::CreateError,
                                Channel::XfrmError
                            >,
                            <Channel::Acquired as FarChannelAcquired>::WrapError
                        >
                    >
                >
            >,
            <ConsensusCtlCodec<u128, H, TestSeal, TestSealCodec> as Codec<
                ConsensusCtl<u128, H::HashID, TestSeal>
            >>::CreateError
        >
    >{
        let PeerComponent {
            peer_state_config,
            consensus_large_obj_config,
            consensus_comm_config,
            consensus_listener,
            client_comm_config,
            client_large_obj_config,
            client_listener,
            processor_comm_config,
            processor_large_obj_config,
            processor_listener,
            processor_ids,
            processors_ctx,
            consensus_ctx,
            consensus_authn,
            client_ctx,
            processors,
            shutdown,
            ..
        } = self;

        info!(target: "peer-component",
              "starting peer component");

        let state = Arc::new(PeerState::new(peer_state_config, processors));
        let processor_dispatch = ProcessorSessionDispatch::new(
            processor_large_obj_config,
            processor_ids,
            state.clone()
        );
        let processor_comm: DispatchLargeObjBus<
            XactBlobBatch<u128, H::HashID, TestSeal>,
            XactBlobBatch<u128, H::HashID, TestSeal>,
            XactBatchBlobCodec<u128, H, TestSeal, TestSealCodec>,
            H,
            IDs,
            _,
            _,
            _,
            Epochs,
            _,
            _,
            _,
            _,
            Resolver,
            Endpoint,
            _,
            _
        > = DispatchLargeObjBus::create(
            processor_comm_config,
            processor_dispatch,
            processor_listener,
            shutdown.clone(),
            processors_ctx
        )
        .map_err(|err| PeerComponentRunError::ProcessorComm { err: err })?;
        let client_dispatch =
            ClientSessionDispatch::new(client_large_obj_config, state.clone());
        let client_comm: DispatchLargeObjBus<
            XactBlobBatch<u128, H::HashID, TestSeal>,
            XactBlobBatch<u128, H::HashID, TestSeal>,
            XactBatchBlobCodec<u128, H, TestSeal, TestSealCodec>,
            H,
            IDs,
            _,
            _,
            _,
            Epochs,
            _,
            _,
            _,
            _,
            Resolver,
            Endpoint,
            _,
            _
        > = DispatchLargeObjBus::create(
            client_comm_config,
            client_dispatch,
            client_listener,
            shutdown.clone(),
            client_ctx
        )
        .map_err(|err| PeerComponentRunError::ClientComm { err: err })?;
        let consensus_recv = ConsensusRecv::new(state.clone());
        let consensus_msgs = ConsensusMsgs::new(state.clone());
        let consensus_hash = H::default();
        let consensus_notify = state.notify();
        let consensus_proto = LargeObjProto::create(
            consensus_large_obj_config,
            consensus_notify.clone(),
            consensus_recv,
            consensus_msgs,
            consensus_authn,
            consensus_hash
        )
        .map_err(|err| PeerComponentRunError::LargeObj { err: err })?;
        let consensus_comm: MulticastLargeObjBus<
            _,
            _,
            ConsensusCtlCodec<u128, H, TestSeal, TestSealCodec>,
            H,
            IDs,
            _,
            _,
            _,
            Epochs,
            _,
            _,
            _,
            _,
            Resolver,
            _,
            _
        > = MulticastLargeObjBus::create(
            None,
            consensus_comm_config,
            consensus_listener,
            consensus_ctx,
            shutdown.clone(),
            consensus_notify,
            consensus_proto
        )
        .map_err(|err| PeerComponentRunError::ConsensusComm { err: err })?;

        let consensus_comm_cleanup = consensus_comm.start().map_err(|err| {
            PeerComponentRunError::ConsensusStart { err: err }
        })?;

        // XXX this doesn't clean up properly if consensus uplink
        // fails to start.
        let processor_comm_cleanup = processor_comm.start().map_err(|err| {
            PeerComponentRunError::ProcessorStart { err: err }
        })?;

        let client_comm_cleanup = client_comm
            .start()
            .map_err(|err| PeerComponentRunError::ClientStart { err: err })?;

        Ok(PeerComponentCleanup {
            consensus_comm_cleanup: consensus_comm_cleanup,
            processor_comm_cleanup: processor_comm_cleanup,
            client_comm_cleanup: client_comm_cleanup,
            shutdown: shutdown
        })
    }
}

impl PeerComponentCleanup {
    pub fn cleanup(self) {
        let PeerComponentCleanup {
            client_comm_cleanup,
            processor_comm_cleanup,
            consensus_comm_cleanup,
            mut shutdown
        } = self;

        shutdown.set();
        client_comm_cleanup.cleanup();
        processor_comm_cleanup.cleanup();
        consensus_comm_cleanup.cleanup();
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
impl
    FarChannelRegistryCtx<
        CompoundFarChannel,
        CompoundFarChannelThreadedFlows<
            Arc<TestAuthN<String, TestCred>>,
            UnixDatagramXfrm,
            UDPDatagramXfrm,
            FarChannelRegistryID
        >,
        Arc<TestAuthN<String, TestCred>>,
        CompoundFarChannelXfrm<UnixDatagramXfrm, UDPDatagramXfrm>
    > for StandaloneCtx
{
    #[inline]
    fn far_channel_registry(&mut self) -> Arc<StandaloneRegistry> {
        self.registry.clone()
    }
}

#[cfg(feature = "standalone")]
impl Standalone
    for CompoundPeerComponent<
        XactBlobBatch<u128, SHA3ID, TestSeal>,
        XactBatchBlobCodec<u128, SHA3Algo, TestSeal, TestSealCodec>,
        SHA3Algo,
        AscendingCount<LargeObjID>,
        PassthruMsgAuthN<XactBlobBatch<u128, SHA3ID, TestSeal>, String>,
        PassthruMsgAuthN<ConsensusCtl<u128, SHA3ID, TestSeal>, String>,
        AscendingCount<u128>,
        StandaloneCtx
    >
{
    type Config = StandaloneConfig;
    type CreateCleanup = StandaloneCreateCleanup;

    const CONFIG_FILES: &[&str] = &["peer.conf"];
    const NAME: &str = "peer";
    const VERSION: FullVersion = FullVersion::new(
        None,
        Version::new(0, 0, 0),
        Some(VersionSuffix::Development)
    );

    fn create(
        _args: ArgMatches,
        config: Self::Config
    ) -> Result<(Self, Self::CreateCleanup), Self::CreateCleanup> {
        let (name_caches_config, peer_config) = config.take();
        let (
            clients_config,
            processors_config,
            consensus_config,
            peer_state_config
        ) = peer_config.take();
        let (
            client_registry_config,
            client_comm_config,
            client_large_obj_config,
            client_auth_config
        ) = clients_config.take();
        let (
            processor_registry_config,
            processor_comm_config,
            processor_large_obj_config,
            classes_config,
            processor_auth_config
        ) = processors_config.take();
        let (
            consensus_registry_config,
            consensus_comm_config,
            consensus_large_obj_config
        ) = consensus_config.take();
        let shutdown = ShutdownFlag::new();
        let (mut caches, caches_join) =
            ThreadedNSNameCaches::create(name_caches_config, shutdown.clone());
        let cleanup = StandaloneCreateCleanup {
            shutdown: shutdown.clone(),
            caches_join: caches_join
        };
        let (client_listener, client_reporter) = ThreadedFlowsListener::new();
        let (processor_listener, processor_reporter) =
            ThreadedFlowsListener::new();
        let (consensus_listener, consensus_reporter) =
            ThreadedFlowsListener::new();

        // XXX maybe move this part into state?
        let ProcessorClassesConfig::Static {
            stat: processor_configs
        } = classes_config;
        let mut processor_ids: HashMap<String, ProcessorIdx> =
            HashMap::with_capacity(processor_configs.len());
        let mut classes = HashMap::with_capacity(processor_configs.len());
        let mut curr_id = 0;

        // Build the configuration structure for processors.
        for processor_config in processor_configs {
            let (prin, class_configs) = processor_config.take();
            let id = match processor_ids.get(&prin) {
                Some(id) => id.clone(),
                None => {
                    let idx = ProcessorIdx::from(curr_id);

                    curr_id += 1;
                    processor_ids.insert(prin, idx.clone());

                    idx
                }
            };

            for class_config in class_configs {
                let (class, instances, versions) = class_config.take();
                let versions = versions.into_iter().map(|config| config.into());
                let class: Uuid = class.into();
                let instance = InstanceEntry::new(id.clone(), versions);

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
                                error!(target: "start",
                                       concat!("duplicate processor: ",
                                               "class {}, instance {}"),
                                       class, i);

                                return Err(cleanup);
                            }
                        }
                    }
                }
            }
        }

        // XXX this is brought over from consensus, and should be
        // turned into a general function.
        let parties_config = consensus_comm_config.parties();
        let authn_parties = match parties_config {
            PartiesConfig::Static { stat } => {
                let mut authn_parties = Vec::with_capacity(stat.len());

                for party in stat {
                    let id = party.party();

                    for conn in party.party_config().connections() {
                        for endpoint in conn.endpoints() {
                            match endpoint {
                                CompoundFarEndpoint::Unix { unix_datagram } => {
                                    match UnixSocketAddr::try_from(
                                        unix_datagram
                                    ) {
                                        Ok(addr) => {
                                            let cred =
                                                TestCred::Unix { addr: addr };

                                            authn_parties
                                                .push((cred, id.clone()));
                                        }
                                        Err(err) => {
                                            warn!(target: "start",
                                              "error converting path: {}",
                                              err);
                                        }
                                    }
                                }
                                CompoundFarEndpoint::UDP { udp } => match udp
                                    .ip_endpoint()
                                {
                                    IPEndpointAddr::Addr(addr) => {
                                        let addr =
                                            SocketAddr::new(*addr, udp.port());
                                        let cred = TestCred::IP { addr: addr };

                                        authn_parties.push((cred, id.clone()));
                                    }
                                    IPEndpointAddr::Name(name) => {
                                        warn!(target: "start",
                                               "discarding endpoint {}",
                                               name);
                                    }
                                }
                            }
                        }
                    }
                }

                authn_parties
            }
        };
        let consensus_authn =
            Arc::new(TestAuthN::from_parties(authn_parties.into_iter()));
        let processors = classes
            .into_iter()
            .map(|(class, mut instances)| {
                instances.shrink_to_fit();

                (class, ProcessorEntry::new(instances))
            })
            .collect();
        let processors_authn = match TestAuthN::create(processor_auth_config) {
            Ok(processor_authn) => processor_authn,
            Err(err) => {
                error!(target: "start",
                       "error creating processor authn: {}",
                       err);

                return Err(cleanup);
            }
        };
        let processors_authn = Arc::new(processors_authn);
        let processors_ctx = match StandaloneRegistry::create(
            &mut caches,
            processors_authn,
            processor_reporter,
            processor_registry_config
        ) {
            Ok(processor_registry) => StandaloneCtx {
                registry: Arc::new(processor_registry),
                caches: caches.clone()
            },
            Err(err) => {
                error!(target: "start",
                       "error creating processor channel registry: {}",
                       err);

                return Err(cleanup);
            }
        };
        let client_authn = match TestAuthN::create(client_auth_config) {
            Ok(processor_authn) => processor_authn,
            Err(err) => {
                error!(target: "start",
                       "error creating processor authn: {}",
                       err);

                return Err(cleanup);
            }
        };
        let client_authn = Arc::new(client_authn);
        let client_ctx = match StandaloneRegistry::create(
            &mut caches,
            client_authn,
            client_reporter,
            client_registry_config
        ) {
            Ok(client_registry) => StandaloneCtx {
                registry: Arc::new(client_registry),
                caches: caches.clone()
            },
            Err(err) => {
                error!(target: "start",
                       "error creating client channel registry: {}",
                       err);

                return Err(cleanup);
            }
        };
        let consensus_ctx = match StandaloneRegistry::create(
            &mut caches,
            consensus_authn,
            consensus_reporter,
            consensus_registry_config
        ) {
            Ok(consensus_registry) => StandaloneCtx {
                registry: Arc::new(consensus_registry),
                caches: caches
            },
            Err(err) => {
                error!(target: "start",
                       "error creating consensus channel registry: {}",
                       err);

                return Err(cleanup);
            }
        };
        let consensus_authn = PassthruMsgAuthN::default();
        let peer = PeerComponent {
            xact_wrapper: PhantomData,
            xact_codec: PhantomData,
            hash: PhantomData,
            xact_auth: PhantomData,
            ctl_auth: PhantomData,
            ids: PhantomData,
            resolver: PhantomData,
            endpoint: PhantomData,
            processor_comm_config: processor_comm_config,
            processor_large_obj_config: processor_large_obj_config,
            processor_listener: processor_listener,
            peer_state_config: peer_state_config,
            client_comm_config: client_comm_config,
            client_large_obj_config: client_large_obj_config,
            client_listener: client_listener,
            consensus_listener: consensus_listener,
            consensus_large_obj_config: consensus_large_obj_config,
            consensus_comm_config: consensus_comm_config,
            processors: processors,
            processor_ids: processor_ids,
            processors_ctx: processors_ctx,
            consensus_ctx: consensus_ctx,
            client_ctx: client_ctx,
            shutdown: shutdown,
            consensus_authn: consensus_authn
        };

        Ok((peer, cleanup))
    }
}

impl StandaloneService
    for CompoundPeerComponent<
        XactBlobBatch<u128, SHA3ID, TestSeal>,
        XactBatchBlobCodec<u128, SHA3Algo, TestSeal, TestSealCodec>,
        SHA3Algo,
        AscendingCount<LargeObjID>,
        PassthruMsgAuthN<XactBlobBatch<u128, SHA3ID, TestSeal>, String>,
        PassthruMsgAuthN<ConsensusCtl<u128, SHA3ID, TestSeal>, String>,
        AscendingCount<u128>,
        StandaloneCtx
    >
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

impl<Acquire, Consensus, C> Display
    for PeerComponentRunError<Acquire, Consensus, C>
where
    Acquire: Display,
    Consensus: Display,
    C: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "failed to start consensus component")
    }
}

// ISSUE #2: Delete from here

use std::convert::Infallible;
use std::net::SocketAddr;

use constellation_auth::cred::SSLCred;
use constellation_channels::far::compound::CompoundFarChannelSessionCred;
use constellation_channels::far::compound::CompoundFarChannelXfrmPeerAddr;
use constellation_channels::far::compound::CompoundFarIPChannelXfrmPeerAddr;
use constellation_channels::unix::UnixSocketAddr;

#[derive(Clone, Debug)]
pub struct TestSeal;

#[derive(Clone)]
pub struct TestSealCodec;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum TestCred {
    IP { addr: SocketAddr },
    Unix { addr: UnixSocketAddr }
}

impl TryFrom<TestCredConfig> for TestCred {
    type Error = std::io::Error;

    #[inline]
    fn try_from(val: TestCredConfig) -> Result<TestCred, Self::Error> {
        match val {
            TestCredConfig::Unix { unix } => {
                let addr = UnixSocketAddr::try_from(unix)?;

                Ok(TestCred::Unix { addr: addr })
            }
            TestCredConfig::IP { ip } => Ok(TestCred::IP { addr: ip })
        }
    }
}

impl Codec<TestSeal> for TestSealCodec {
    type CreateError = Infallible;
    type DecodeError = Infallible;
    type EncodeError = Infallible;
    type Param = ();

    #[inline]
    fn create(_param: ()) -> Result<Self, Infallible> {
        Ok(TestSealCodec)
    }

    #[inline]
    fn buf_size(
        &self,
        _val: &TestSeal
    ) -> usize {
        0
    }

    #[inline]
    fn encode(
        &mut self,
        _val: &TestSeal,
        _buf: &mut [u8]
    ) -> Result<usize, Self::EncodeError> {
        Ok(0)
    }

    #[inline]
    fn decode(
        &mut self,
        _buf: &[u8]
    ) -> Result<(TestSeal, usize), Self::DecodeError> {
        Ok((TestSeal, 0))
    }
}

impl<Basic> From<SSLCred<CompoundFarChannelSessionCred<Basic>>> for TestCred
where
    TestCred: From<Basic>
{
    fn from(_val: SSLCred<CompoundFarChannelSessionCred<Basic>>) -> TestCred {
        panic!("Not supported!")
    }
}

impl From<CompoundFarIPChannelXfrmPeerAddr> for TestCred {
    fn from(val: CompoundFarIPChannelXfrmPeerAddr) -> TestCred {
        match val {
            CompoundFarIPChannelXfrmPeerAddr::UDP { udp } => {
                TestCred::IP { addr: udp }
            }
            _ => panic!("Not supported!")
        }
    }
}

impl From<CompoundFarChannelXfrmPeerAddr> for TestCred {
    fn from(val: CompoundFarChannelXfrmPeerAddr) -> TestCred {
        match val {
            CompoundFarChannelXfrmPeerAddr::Unix { unix } => {
                TestCred::Unix { addr: unix }
            }
            CompoundFarChannelXfrmPeerAddr::IP { ip } => TestCred::from(ip)
        }
    }
}

impl<Basic> From<CompoundFarChannelSessionCred<Basic>> for TestCred
where
    TestCred: From<Basic>
{
    fn from(val: CompoundFarChannelSessionCred<Basic>) -> TestCred {
        match val {
            CompoundFarChannelSessionCred::Basic { basic } => {
                TestCred::from(basic)
            }
            _ => panic!("Not supported!")
        }
    }
}

impl Display for TestCred {
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            TestCred::IP { addr } => write!(f, "ip://{}", addr),
            TestCred::Unix { addr } => write!(f, "unix://{}", addr)
        }
    }
}

// ISSUE #2: to here
