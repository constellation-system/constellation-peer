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
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_common::codec::Encoder;
use constellation_common::config::Create;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_common::hashid::HashID;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_component_common::bus::dispatch::SessionDispatch;
use constellation_component_common::xact::XactBatchBlobCodec;
use constellation_component_common::xact::XactBlobBatch;
use constellation_streams::config::LargeObjProtoConfig;
use constellation_streams::frags::Frags;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::large_obj::LargeObjProtoAddOutboundError;
use constellation_streams::large_obj::LargeObjProtoCreateError;
use constellation_streams::large_obj::LargeObjSender;
use log::debug;
use log::error;
use log::trace;
use log::warn;

use crate::state::PeerState;
use crate::state::ConsensusRoundID;
use crate::types::ClientMsgTypes;
use crate::types::ClientSessionDispatchTypes;
use crate::types::LargeObjSessionTypes;
use crate::types::SealTypes;

pub(crate) struct ClientSessionDispatch<Types>
where
    Types: LargeObjSessionTypes {
    sessions: Arc<Mutex<HashMap<Types::Prin, ClientSession<Types::HashID>>>>,
    state: Arc<PeerState<Types>>,
}

#[derive(Clone)]
pub(crate) struct ClientSessionRecv<Types>
where
    Types: SealTypes {
    state: Arc<PeerState<Types>>,
    sessions: Arc<Mutex<HashMap<Types::Prin, ClientSession<Types::HashID>>>>
}

#[derive(Clone)]
pub(crate) struct ClientSessionMsgs<Types>
where
    Types: SealTypes {
    state: Arc<PeerState<Types>>,
    subscriptions: Arc<Mutex<HashSet<Types::HashID>>>,
    prin: Types::Prin
}

struct ClientSession<H>
where
    H: Clone + Display + Hash + HashID + Eq + Send {
    subscriptions: Arc<Mutex<HashSet<H>>>,
    local_shutdown: ShutdownFlag
}

#[derive(Debug)]
pub(crate) enum ClientSessionDispatchError<Prin, Encoder, Decoder, IDs> {
    Proto {
        err: LargeObjProtoCreateError<Encoder, Decoder, IDs>
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

impl<Types> LargeObjMsgs<
    Types::Hash,
    XactBlobBatch<ConsensusRoundID, Types::HashID, Types::Seal>
> for ClientSessionMsgs<Types>
where
    Types: SealTypes {
    type AddMsgsError<Encode>
        = WithMutexPoison<LargeObjProtoAddOutboundError<Encode>>
    where
        Encode: Debug + Display + ScopedError;

    fn add_msgs<WrapperCodec, F>(
        &mut self,
        sender: &mut LargeObjSender<
            Types::Hash,
            XactBlobBatch<ConsensusRoundID, Types::HashID, Types::Seal>,
            WrapperCodec,
            F
        >
    ) -> Result<Option<Instant>, Self::AddMsgsError<WrapperCodec::EncodeError>>
    where
        WrapperCodec: Clone + Create
            + Encoder<XactBlobBatch<ConsensusRoundID, Types::HashID,
                                    Types::Seal>>,
        WrapperCodec::Config: Default,
        F: Frags {
        let prin = self.prin.clone();
        let mut subscriptions = self
            .subscriptions
            .lock()
            .map_err(|_| WithMutexPoison::MutexPoison)?;
        let (notifies, next) =
            self.state.get_client_msgs(&mut subscriptions, &prin)?;

        if !notifies.is_empty() {
            let batch = XactBlobBatch::new(vec![], vec![], notifies);

            sender
                .add_outbound(&batch)
                .map_err(|err| WithMutexPoison::Inner { err: err })?;

            Ok(next)
        } else {
            Ok(None)
        }
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

impl<H> Drop for ClientSession<H>
where
    H: Clone + Display + Hash + HashID + Eq + Send
{
    fn drop(&mut self) {
        trace!(target: "client-session",
               "signaling local shutdown");

        if let Err(err) = self.local_shutdown.set() {
            error!(target: "client-session",
                   "Error setting shutdown flag: {}",
                   err)
        }
    }
}

impl<Types> AuthNMsgRecv<
    Types::Prin,
    XactBlobBatch<ConsensusRoundID, Types::HashID, Types::Seal>,
    Types::ClientAuthMsg
>
    for ClientSessionRecv<Types>
where
    Types: ClientMsgTypes {
    /// Errors that can occur reporting messages.
    type RecvError = ClientSessionRecvError<Types::Prin>;

    /// Receive an authenticated message.
    fn recv_auth_msg(
        &mut self,
        msg: Types::ClientAuthMsg
    ) -> Result<(), Self::RecvError> {
        let (prin, msg) = msg.take();

        debug!(target: "client-session-recv",
               "received batch from {}",
               prin);

        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| ClientSessionRecvError::MutexPoison)?;
        // This is a placeholder for eventual authorization.
        let session = sessions
            .get_mut(prin)
            .ok_or(ClientSessionRecvError::NotFound { prin: prin.clone() })?;
        let (committed, reqs, notifies) = msg.take();

        for req in reqs.into_iter() {
            trace!(target: "client-session-recv",
                   "processing uncommitted request from {}",
                   prin);

            // XXX will need to authorize the seal here.
            let (seal, req) = req.take();
            let (class, version, instance, hash, effects, payload) = req.take();

            session
                .subscriptions
                .lock()
                .map_err(|_| ClientSessionRecvError::MutexPoison)?
                .insert(hash.clone());
            self.state
                .add_xact(
                    prin, class, version, instance, hash, effects, payload,
                    seal
                )
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

impl<Types> ClientSessionDispatch<Types>
where
    Types: LargeObjSessionTypes {
    pub(crate) fn new(
        state: Arc<PeerState<Types>>
    ) -> Self {
        let sessions = Arc::new(Mutex::new(HashMap::new()));

        ClientSessionDispatch {
            sessions: sessions,
            state: state
        }
    }
}

impl<Types> SessionDispatch<Types::ClientSessionDispTypes>
    for ClientSessionDispatch<Types>
where
    Types: ClientSessionDispatchTypes
{
    type SessionError = ClientSessionDispatchError<
        Types::Prin,
        <XactBatchBlobCodec<u128, Types::Hash, Types::Seal, Types::SealCodec> as Create>::CreateError,
        <XactBatchBlobCodec<u128, Types::Hash, Types::Seal, Types::SealCodec> as Create>::CreateError,
        Types::IDsCreateError
    >;

    fn session(
        &self,
        prin: &Types::Prin,
        shutdown: ShutdownFlag,
        notify: Notify
    ) -> Result<
        (
            ShutdownFlag,
            ClientSessionMsgs<Types>,
            ClientSessionRecv<Types>
        ),
        Self::SessionError
    > {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| ClientSessionDispatchError::MutexPoison)?;
        let (local_shutdown, subscriptions) = match sessions.entry(prin.clone())
        {
            Entry::Vacant(ent) => {
                let local_shutdown = ShutdownFlag::new(shutdown);
                // XXX use a size hint here.
                let subscriptions = Arc::new(Mutex::new(HashSet::new()));

                debug!(target: "client-session-dispatch",
                       "creating session for {}",
                       prin);

                ent.insert(ClientSession {
                    local_shutdown: local_shutdown.clone(),
                    subscriptions: subscriptions.clone()
                });

                Ok((local_shutdown, subscriptions))
            }
            _ => Err(ClientSessionDispatchError::Exists { prin: prin.clone() })
        }?;
        let hash = Types::Hash::default();
        let recv = ClientSessionRecv {
            sessions: self.sessions.clone(),
            state: self.state.clone()
        };
        let msgs = ClientSessionMsgs {
            subscriptions: subscriptions,
            state: self.state.clone(),
            prin: prin
        };

        Ok((local_shutdown, msgs, recv))
    }
}

impl<Prin, Encoder, Decoder, IDs> ScopedError
    for ClientSessionDispatchError<Prin, Encoder, Decoder, IDs>
where
    Encoder: ScopedError,
    Decoder: ScopedError,
    IDs: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            ClientSessionDispatchError::Proto { err } => err.scope(),
            ClientSessionDispatchError::Exists { .. } |
            ClientSessionDispatchError::MutexPoison =>
                ErrorScope::Unrecoverable
        }
    }
}

impl<Prin, Encoder, Decoder, IDs> Display
    for ClientSessionDispatchError<Prin, Encoder, Decoder, IDs>
where
    Prin: Display,
    Encoder: Display,
    Decoder: Display,
    IDs: Display
{
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
