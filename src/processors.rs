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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::PassthruMsgAuthN;
use constellation_common::codec::Codec;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_common::ids::IDGen;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_component_common::bus::large_obj::dispatch::SessionDispatch;
use constellation_component_common::xact::XactBatchBlobCodec;
use constellation_component_common::xact::XactBlobBatch;
use constellation_component_common::xact::XactNotifyState;
use constellation_streams::config::LargeObjProtoConfig;
use constellation_streams::frags::Frags;
use constellation_streams::frags::OutboundFrags;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::large_obj::LargeObjProto;
use constellation_streams::large_obj::LargeObjProtoAddOutboundError;
use constellation_streams::large_obj::LargeObjProtoCreateError;
use constellation_streams::large_obj::LargeObjSender;
use log::debug;
use log::trace;
use log::warn;

use crate::state::PeerState;
use crate::state::ProcessorIdx;

pub(crate) struct ProcessorSessionDispatch<H, IDs, Prin, Seal, SealCodec>
where
    SealCodec: Clone + Codec<Seal>,
    SealCodec::Param: Clone + Default,
    Seal: Clone,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync {
    hash: PhantomData<H>,
    ids: PhantomData<IDs>,
    sessions: Arc<Mutex<HashMap<Prin, ProcessorSession>>>,
    state: Arc<PeerState<H::HashID, Prin, Seal>>,
    config: LargeObjProtoConfig<
        <XactBatchBlobCodec<u128, H, Seal, SealCodec> as Codec<
            XactBlobBatch<u128, H::HashID, Seal>
        >>::Param,
        IDs::Config
    >,
    processors: HashMap<Prin, ProcessorIdx>
}

#[derive(Clone)]
pub(crate) struct ProcessorSessionRecv<H, Prin, Seal>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Seal: Clone {
    hash: PhantomData<H>,
    state: Arc<PeerState<H, Prin, Seal>>,
    sessions: Arc<Mutex<HashMap<Prin, ProcessorSession>>>
}

#[derive(Clone)]
pub(crate) struct ProcessorSessionMsgs<H, Prin, Seal>
where
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    H: HashAlgo,
    H::HashID: Clone + Display + Eq + Hash + HashID,
    Seal: Clone {
    state: Arc<PeerState<H::HashID, Prin, Seal>>,
    idx: ProcessorIdx
}

struct ProcessorSession {
    local_shutdown: ShutdownFlag
}

#[derive(Debug)]
pub(crate) enum ProcessorSessionDispatchError<Prin, Codec> {
    Proto {
        err: LargeObjProtoCreateError<Codec>
    },
    Exists {
        prin: Prin
    },
    Unknown {
        prin: Prin
    },
    MutexPoison
}

#[derive(Debug)]
pub(crate) enum ProcessorSessionRecvError<Prin> {
    NotFound { prin: Prin },
    MutexPoison
}

unsafe impl<H, IDs, Prin, Seal, SealCodec> Send
    for ProcessorSessionDispatch<H, IDs, Prin, Seal, SealCodec>
where
    SealCodec: Clone + Codec<Seal>,
    SealCodec::Param: Clone + Default,
    Seal: Clone,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
}

unsafe impl<H, IDs, Prin, Seal, SealCodec> Sync
    for ProcessorSessionDispatch<H, IDs, Prin, Seal, SealCodec>
where
    SealCodec: Clone + Codec<Seal>,
    SealCodec::Param: Clone + Default,
    Seal: Clone,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
}

unsafe impl<H, Prin, Seal> Send for ProcessorSessionRecv<H, Prin, Seal>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Seal: Clone
{
}

unsafe impl<H, Prin, Seal> Sync for ProcessorSessionRecv<H, Prin, Seal>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Seal: Clone
{
}

impl<H, Prin, Seal> LargeObjMsgs<H, XactBlobBatch<u128, H::HashID, Seal>>
    for ProcessorSessionMsgs<H, Prin, Seal>
where
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    H: Clone + HashAlgo,
    H::HashID: Clone + Display + Eq + Hash + HashID,
    Seal: Clone
{
    type AddMsgsError<Encode>
        = WithMutexPoison<LargeObjProtoAddOutboundError<Encode>>
    where
        Encode: Display + ScopedError;

    fn add_msgs<WrapperCodec, F>(
        &mut self,
        sender: &mut LargeObjSender<
            H,
            XactBlobBatch<u128, H::HashID, Seal>,
            WrapperCodec,
            F
        >
    ) -> Result<Option<Instant>, Self::AddMsgsError<WrapperCodec::EncodeError>>
    where
        WrapperCodec: Clone + Codec<XactBlobBatch<u128, H::HashID, Seal>>,
        WrapperCodec::Param: Default,
        F: Frags {
        let (committed, reqs, next) =
            self.state.get_processor_msgs(self.idx.clone())?;

        if !committed.is_empty() || !reqs.is_empty() {
            let batch = XactBlobBatch::new(committed, reqs, vec![]);

            sender
                .add_outbound(&batch)
                .map_err(|err| WithMutexPoison::Inner { error: err })?;

            Ok(next)
        } else {
            Ok(None)
        }
    }
}

impl<Prin> ScopedError for ProcessorSessionRecvError<Prin> {
    fn scope(&self) -> ErrorScope {
        match self {
            ProcessorSessionRecvError::NotFound { .. } => ErrorScope::Session,
            ProcessorSessionRecvError::MutexPoison => ErrorScope::Unrecoverable
        }
    }
}

impl Drop for ProcessorSession {
    fn drop(&mut self) {
        trace!(target: "processor-session",
               "signaling local shutdown");

        self.local_shutdown.set()
    }
}

impl<Prin, H, Seal> AuthNMsgRecv<Prin, XactBlobBatch<u128, H, Seal>>
    for ProcessorSessionRecv<H, Prin, Seal>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Seal: Clone
{
    /// Errors that can occur reporting messages.
    type RecvError = ProcessorSessionRecvError<Prin>;

    /// Receive an authenticated message.
    fn recv_auth_msg(
        &mut self,
        prin: &Prin,
        msg: XactBlobBatch<u128, H, Seal>
    ) -> Result<(), Self::RecvError> {
        debug!(target: "processor-session-recv",
               "received batch from {}",
               prin);

        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| ProcessorSessionRecvError::MutexPoison)?;
        // This is a placeholder for eventual authorization.
        let _ = sessions.get_mut(prin).ok_or(
            ProcessorSessionRecvError::NotFound { prin: prin.clone() }
        )?;
        let (committed, reqs, notifies) = msg.take();

        // We shouldn't be getting these at all at this point.  We
        // will eventually get them in the form of derived
        // transactions, though.
        for _ in reqs.into_iter() {
            warn!(target: "processor-session-recv",
                  "discarding unauthorized uncommitted request message");
        }

        // We shouldn't be getting these at all.
        for _ in committed.into_iter() {
            warn!(target: "processor-session-recv",
                  "discarding unauthorized committed round message");
        }

        // These might make sense as a forwarding mechanism, but
        // unlikely.
        for notify in notifies.into_iter() {
            trace!(target: "processor-session-recv",
                   "processing notify from {}",
                   prin);

            // XXX will need to authorize the seal here.
            let (hash, notify) = notify.take();

            match notify {
                // This is fine, and will probably be meaningful at
                // some point.
                XactNotifyState::Accept => {
                    trace!(target: "processor-session-recv",
                           "received accept from processor");
                }
                XactNotifyState::PrecommitDispatch { .. } => {
                    warn!(target: "processor-session-recv",
                          "unexpected precommit dispatch notification");
                }
                XactNotifyState::Consensus => {
                    warn!(target: "processor-session-recv",
                          "unexpected consensus notification");
                }
                XactNotifyState::Commit { .. } => {
                    warn!(target: "processor-session-recv",
                          "unexpected commit notification");
                }
                XactNotifyState::Dispatch { .. } => {
                    warn!(target: "processor-session-recv",
                          "unexpected dispatch notification");
                }
                XactNotifyState::Success { result: None, .. } => {
                    warn!(target: "processor-session-recv",
                          "unexpected empty success notification");
                }
                XactNotifyState::Error { error: None } => {
                    warn!(target: "processor-session-recv",
                          "unexpected empty error notification");
                }
                XactNotifyState::Success {
                    result: Some(result),
                    when
                } => {
                    self.state
                        .add_result(hash, when, result)
                        .map_err(|_| ProcessorSessionRecvError::MutexPoison)?;
                }
                XactNotifyState::Error { error: Some(error) } => {
                    self.state
                        .add_error(hash, error)
                        .map_err(|_| ProcessorSessionRecvError::MutexPoison)?;
                }
            }
        }

        Ok(())
    }
}

impl<H, IDs, Prin, Seal, SealCodec>
    ProcessorSessionDispatch<H, IDs, Prin, Seal, SealCodec>
where
    SealCodec: Clone + Codec<Seal>,
    SealCodec::Param: Clone + Default,
    Seal: Clone,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
    pub(crate) fn new(
        config: LargeObjProtoConfig<
            <XactBatchBlobCodec<u128, H, Seal, SealCodec> as Codec<
                XactBlobBatch<u128, H::HashID, Seal>
            >>::Param,
            IDs::Config
        >,
        processors: HashMap<Prin, ProcessorIdx>,
        state: Arc<PeerState<H::HashID, Prin, Seal>>
    ) -> Self {
        let sessions = Arc::new(Mutex::new(HashMap::new()));

        ProcessorSessionDispatch {
            hash: PhantomData,
            ids: PhantomData,
            processors: processors,
            sessions: sessions,
            config: config,
            state: state
        }
    }
}

impl<H, IDs, Prin, Seal, SealCodec>
    SessionDispatch<
        H,
        XactBlobBatch<u128, H::HashID, Seal>,
        XactBlobBatch<u128, H::HashID, Seal>,
        PassthruMsgAuthN<XactBlobBatch<u128, H::HashID, Seal>, Prin>,
        XactBatchBlobCodec<u128, H, Seal, SealCodec>,
        IDs,
        ProcessorSessionMsgs<H, Prin, Seal>,
        ProcessorSessionRecv<H::HashID, Prin, Seal>,
        Prin
    > for ProcessorSessionDispatch<H, IDs, Prin, Seal, SealCodec>
where
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Seal: Clone,
    SealCodec: Clone + Codec<Seal>,
    SealCodec::Param: Clone + Default
{
    type SessionError = ProcessorSessionDispatchError<
        Prin,
        <XactBatchBlobCodec<u128, H, Seal, SealCodec> as Codec<
            XactBlobBatch<u128, H::HashID, Seal>
        >>::CreateError
    >;

    fn session(
        &self,
        prin: Prin
    ) -> Result<
        (
            ShutdownFlag,
            Notify,
            LargeObjProto<
                H,
                XactBlobBatch<u128, H::HashID, Seal>,
                XactBlobBatch<u128, H::HashID, Seal>,
                PassthruMsgAuthN<XactBlobBatch<u128, H::HashID, Seal>, Prin>,
                (),
                XactBatchBlobCodec<u128, H, Seal, SealCodec>,
                IDs,
                ProcessorSessionMsgs<H, Prin, Seal>,
                ProcessorSessionRecv<H::HashID, Prin, Seal>,
                OutboundFrags
            >
        ),
        Self::SessionError
    > {
        if let Some(idx) = self.processors.get(&prin) {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|_| ProcessorSessionDispatchError::MutexPoison)?;
            let local_shutdown = match sessions.entry(prin.clone()) {
                Entry::Vacant(ent) => {
                    let local_shutdown = ShutdownFlag::new();

                    debug!(target: "processor-session-dispatch",
                           "creating session for {}",
                           prin);

                    ent.insert(ProcessorSession {
                        local_shutdown: local_shutdown.clone()
                    });

                    Ok(local_shutdown)
                }
                _ => Err(ProcessorSessionDispatchError::Exists {
                    prin: prin.clone()
                })
            }?;
            let hash = H::default();
            let recv = ProcessorSessionRecv {
                hash: PhantomData,
                sessions: self.sessions.clone(),
                state: self.state.clone()
            };
            let msgs = ProcessorSessionMsgs {
                state: self.state.clone(),
                idx: idx.clone()
            };
            let authn = PassthruMsgAuthN::default();
            let proto = LargeObjProto::create(
                self.config.clone(),
                self.state.notify(),
                recv,
                msgs,
                authn,
                hash
            )
            .map_err(|err| ProcessorSessionDispatchError::Proto { err: err })?;

            Ok((local_shutdown, self.state.notify(), proto))
        } else {
            Err(ProcessorSessionDispatchError::Unknown { prin: prin })
        }
    }
}

impl<Prin, Codec> Display for ProcessorSessionDispatchError<Prin, Codec>
where
    Prin: Display,
    Codec: Display
{
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            ProcessorSessionDispatchError::Proto { err } => err.fmt(f),
            ProcessorSessionDispatchError::Unknown { prin } => {
                write!(f, "no processor associated with {}", prin)
            }
            ProcessorSessionDispatchError::Exists { prin } => {
                write!(f, "processor session already exists for {}", prin)
            }
            ProcessorSessionDispatchError::MutexPoison => {
                write!(f, "mutex poisoned")
            }
        }
    }
}

impl<Prin> Display for ProcessorSessionRecvError<Prin>
where
    Prin: Display
{
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            ProcessorSessionRecvError::NotFound { prin } => {
                write!(f, "no processor session exists for {}", prin)
            }
            ProcessorSessionRecvError::MutexPoison => {
                write!(f, "mutex poisoned")
            }
        }
    }
}
