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
use std::sync::RwLock;
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
use constellation_component_common::xact::XactBlobBatch;
use constellation_component_common::xact::XactBatchBlobCodec;
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

pub(crate) struct ClientSessionDispatch<H, IDs, Prin, Seal, SealCodec>
where
    SealCodec: Clone + Codec<Seal>,
    SealCodec::Param: Clone + Default,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync {
    hash: PhantomData<H>,
    ids: PhantomData<IDs>,
    sessions: Arc<RwLock<HashMap<Prin, ClientSession>>>,
    state: Arc<PeerState<H::HashID, Prin, Seal>>,
    config: LargeObjProtoConfig<
        <XactBatchBlobCodec<u128, H, Seal, SealCodec>
         as Codec<XactBlobBatch<u128, H::HashID, Seal>>>::Param,
        IDs::Config
    >
}

#[derive(Clone)]
pub(crate) struct ClientSessionRecv<H, Prin, Seal>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync {
    hash: PhantomData<H>,
    state: Arc<PeerState<H, Prin, Seal>>,
    sessions: Arc<RwLock<HashMap<Prin, ClientSession>>>
}

#[derive(Clone)]
pub(crate) struct ClientSessionMsgs<H, Prin, Seal>
where
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    H: HashAlgo,
    H::HashID: Clone + Display + Eq + Hash + HashID {
    state: Arc<PeerState<H::HashID, Prin, Seal>>,
    subscriptions: Vec<H::HashID>,
    notify: Notify,
    prin: Prin,
    hash: H
}

struct ClientSession {
    local_shutdown: ShutdownFlag
}

#[derive(Debug)]
pub(crate) enum ClientSessionDispatchError<Prin, Codec> {
    Proto {
        err: LargeObjProtoCreateError<Codec>
    },
    Exists {
        prin: Prin
    },
    MutexPoison
}

#[derive(Debug)]
pub(crate) enum ClientSessionRecvError<Prin> {
    NotFound { prin: Prin },
    MutexPoison
}

unsafe impl<H, IDs, Prin, Seal, SealCodec> Send
    for ClientSessionDispatch<H, IDs, Prin, Seal, SealCodec>
where
    SealCodec: Clone + Codec<Seal>,
    SealCodec::Param: Clone + Default,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
}

unsafe impl<H, IDs, Prin, Seal, SealCodec> Sync
    for ClientSessionDispatch<H, IDs, Prin, Seal, SealCodec>
where
    SealCodec: Clone + Codec<Seal>,
    SealCodec::Param: Clone + Default,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
}

unsafe impl<H, Prin, Seal> Send for ClientSessionRecv<H, Prin, Seal>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
}

unsafe impl<H, Prin, Seal> Sync for ClientSessionRecv<H, Prin, Seal>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
}

impl<H, Prin, Seal> LargeObjMsgs<H, XactBlobBatch<u128, H::HashID, Seal>>
    for ClientSessionMsgs<H, Prin, Seal>
where
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    H: Clone + HashAlgo,
    H::HashID: Clone + Display + Eq + Hash + HashID {
    type AddMsgsError<Encode>
        = WithMutexPoison<LargeObjProtoAddOutboundError<H::HashID, Encode>>
    where
        Encode: Display + ScopedError;

    fn add_msgs<WrapperCodec, F>(
        &mut self,
        sender: &mut LargeObjSender<H, XactBlobBatch<u128, H::HashID, Seal>,
                                    WrapperCodec, F>
    ) -> Result<Option<Instant>, Self::AddMsgsError<WrapperCodec::EncodeError>>
    where
        WrapperCodec: Clone + Codec<XactBlobBatch<u128, H::HashID, Seal>>,
        WrapperCodec::Param: Default,
        F: Frags {
        let prin = self.prin.clone();
        let mut subscriptions = self.subscriptions.drain(..).collect();
        let (notifies, next) = self
            .state.get_client_msgs(&mut subscriptions, &prin)?;
        let batch = XactBlobBatch::new(vec![], vec![], notifies);

        self.subscriptions = subscriptions.drain().collect();

        sender.add_outbound(&batch)
            .map_err(|err| WithMutexPoison::Inner { error: err })?;

        Ok(next)
    }
}

impl<Prin> ScopedError for ClientSessionRecvError<Prin> {
    fn scope(&self) -> ErrorScope {
        match self {
            ClientSessionRecvError::NotFound { .. } => ErrorScope::Session,
            ClientSessionRecvError::MutexPoison => ErrorScope::Unrecoverable
        }
    }
}

impl Drop for ClientSession {
    fn drop(&mut self) {
        trace!(target: "client-session",
               "signaling local shutdown");

        self.local_shutdown.set()
    }
}

impl<Prin, H, Seal> AuthNMsgRecv<Prin, XactBlobBatch<u128, H, Seal>>
    for ClientSessionRecv<H, Prin, Seal>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
    /// Errors that can occur reporting messages.
    type RecvError = ClientSessionRecvError<Prin>;

    /// Receive an authenticated message.
    fn recv_auth_msg(
        &mut self,
        prin: &Prin,
        msg: XactBlobBatch<u128, H, Seal>
    ) -> Result<(), Self::RecvError> {
        let guard = self
            .sessions
            .read()
            .map_err(|_| ClientSessionRecvError::MutexPoison)?;
        // This is a placeholder for eventual authorization.
        let _ = guard
            .get(prin)
            .ok_or(ClientSessionRecvError::NotFound { prin: prin.clone() })?;
        let (committed, reqs, notifies) = msg.take();

        for req in reqs.into_iter() {
            // XXX will need to authorize the seal here.
            let (_, req) = req.take();
            let (class, version, instance, hash, effects, payload) = req.take();

            self.state.add_xact(prin, class, version,
                                instance, hash, effects, payload)
                .map_err(|_| ClientSessionRecvError::MutexPoison)?;
        }

        // We shouldn't be getting these at all at this point.  They
        // might make sense at some point as a forwarding mechanism,
        // and with consensus seals, it's not entirely unreasonable to
        // do that.

        for _ in committed.into_iter() {
            warn!(target: "client-session-recv",
                  "discarding unauthorized committed round message");
        }

        // These might make sense as a forwarding mechanism, but
        // unlikely.

        for notify in notifies.into_iter() {
            warn!(target: "client-session-recv",
                  "discarding notify for {}",
                  notify.hash())
        }

        Ok(())
    }
}

impl<H, IDs, Prin, Seal, SealCodec>
    ClientSessionDispatch<H, IDs, Prin, Seal, SealCodec>
where
    SealCodec: Clone + Codec<Seal>,
    SealCodec::Param: Clone + Default,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
    pub(crate) fn new(
        config: LargeObjProtoConfig<
            <XactBatchBlobCodec<u128, H, Seal, SealCodec>
             as Codec<XactBlobBatch<u128, H::HashID, Seal>>>::Param,
            IDs::Config
        >,
        state: Arc<PeerState<H::HashID, Prin, Seal>>,
    ) -> Self {
        let sessions = Arc::new(RwLock::new(HashMap::new()));

        ClientSessionDispatch {
            hash: PhantomData,
            ids: PhantomData,
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
        ClientSessionMsgs<H, Prin, Seal>,
        ClientSessionRecv<H::HashID, Prin, Seal>,
        Prin
    > for ClientSessionDispatch<H, IDs, Prin, Seal, SealCodec>
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
    type SessionError = ClientSessionDispatchError<
        Prin,
        <XactBatchBlobCodec<u128, H, Seal, SealCodec>
         as Codec<XactBlobBatch<u128, H::HashID, Seal>>>::CreateError
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
                ClientSessionMsgs<H, Prin, Seal>,
                ClientSessionRecv<H::HashID, Prin, Seal>,
                OutboundFrags
            >
        ),
        Self::SessionError
    > {
        let mut guard = self
            .sessions
            .write()
            .map_err(|_| ClientSessionDispatchError::MutexPoison)?;
        let (local_shutdown, notify) = match guard.entry(prin.clone()) {
            Entry::Vacant(ent) => {
                let local_shutdown = ShutdownFlag::new();
                let notify = Notify::new();

                debug!(target: "client-session-dispatch",
                       "creating session for {}",
                       prin);

                ent.insert(ClientSession {
                    local_shutdown: local_shutdown.clone()
                });

                Ok((local_shutdown, notify))
            }
            _ => Err(ClientSessionDispatchError::Exists { prin: prin.clone() })
        }?;
        let hash = H::default();
        let recv = ClientSessionRecv {
            hash: PhantomData,
            sessions: self.sessions.clone(),
            state: self.state.clone()
        };
        // XXX use a size hint here.
        let subscriptions = Vec::new();
        let msgs = ClientSessionMsgs {
            subscriptions: subscriptions,
            state: self.state.clone(),
            notify: notify.clone(),
            hash: hash.clone(),
            prin: prin
        };
        let authn = PassthruMsgAuthN::default();
        let proto = LargeObjProto::create(
            self.config.clone(),
            notify.clone(),
            recv,
            msgs,
            authn,
            hash
        )
        .map_err(|err| ClientSessionDispatchError::Proto { err: err })?;

        Ok((local_shutdown, notify, proto))
    }
}

impl<Prin, Codec> Display for ClientSessionDispatchError<Prin, Codec>
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
            ClientSessionDispatchError::Proto { err } => err.fmt(f),
            ClientSessionDispatchError::Exists { prin } => {
                write!(f, "client session already exists for {}", prin)
            }
            ClientSessionDispatchError::MutexPoison => {
                write!(f, "mutex poisoned")
            }
        }
    }
}

impl<Prin> Display for ClientSessionRecvError<Prin>
where
    Prin: Display
{
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            ClientSessionRecvError::NotFound { prin } => {
                write!(f, "no client session exists for {}", prin)
            }
            ClientSessionRecvError::MutexPoison => write!(f, "mutex poisoned")
        }
    }
}
