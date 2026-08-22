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
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_common::codec::Encoder;
use constellation_common::config::Create;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_component_common::bus::dispatch::SessionDispatch;
use constellation_component_common::xact::XactBatchBlobCodec;
use constellation_component_common::xact::XactBlobBatch;
use constellation_component_common::xact::XactNotifyState;
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
use crate::state::ProcessorIdx;
use crate::state::ConsensusRoundID;
use crate::types::ProcessorMsgTypes;
use crate::types::ProcessorSessionDispatchTypes;
use crate::types::LargeObjSessionTypes;
use crate::types::SealTypes;

pub(crate) struct ProcessorSessionDispatch<Types>
where Types: LargeObjSessionTypes {
    sessions: Arc<Mutex<HashMap<Types::Prin, ProcessorSession>>>,
    state: Arc<PeerState<Types>>,
    processors: HashMap<Types::Prin, ProcessorIdx>
}

#[derive(Clone)]
pub(crate) struct ProcessorSessionRecv<Types>
where
    Types: SealTypes {
    state: Arc<PeerState<Types>>,
    sessions: Arc<Mutex<HashMap<Types::Prin, ProcessorSession>>>
}

#[derive(Clone)]
pub(crate) struct ProcessorSessionMsgs<Types>
where
    Types: SealTypes {
    state: Arc<PeerState<Types>>,
    idx: ProcessorIdx
}

struct ProcessorSession {
    local_shutdown: ShutdownFlag
}

#[derive(Debug)]
pub(crate) enum ProcessorSessionDispatchError<Prin, Encoder, Decoder, IDs> {
    Proto {
        err: LargeObjProtoCreateError<Encoder, Decoder, IDs>
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

impl<Types> LargeObjMsgs<Types::Hash,
                         XactBlobBatch<ConsensusRoundID, Types::HashID,
                                       Types::Seal>>
    for ProcessorSessionMsgs<Types>
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
        let (committed, reqs, next) =
            self.state.get_processor_msgs(self.idx.clone())?;

        if !committed.is_empty() || !reqs.is_empty() {
            let batch = XactBlobBatch::new(committed, reqs, vec![]);

            sender
                .add_outbound(&batch)
                .map_err(|err| WithMutexPoison::Inner { err: err })?;

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
    Types::ProcessorAuthMsg
> for ProcessorSessionRecv<Types>
where
    Types: ProcessorMsgTypes {
    /// Errors that can occur reporting messages.
    type RecvError = ProcessorSessionRecvError<Types::Prin>;

    /// Receive an authenticated message.
    fn recv_auth_msg(
        &mut self,
        msg: Types::ProcessorAuthMsg
    ) -> Result<(), Self::RecvError> {
        let (prin, msg) = msg.take();

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

impl<Types> ProcessorSessionDispatch<Types>
where
    Types: LargeObjSessionTypes {
    pub(crate) fn new(
        processors: HashMap<Types::Prin, ProcessorIdx>,
        state: Arc<PeerState<Types>>
    ) -> Self {
        let sessions = Arc::new(Mutex::new(HashMap::new()));

        ProcessorSessionDispatch {
            processors: processors,
            sessions: sessions,
            state: state
        }
    }
}

impl<Types> SessionDispatch<Types::ProcessorSessionDispTypes>
    for ProcessorSessionDispatch<Types>
where
    Types: ProcessorSessionDispatchTypes
{
    type SessionError = ProcessorSessionDispatchError<
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
            ProcessorSessionMsgs<Types>,
            ProcessorSessionRecv<Types>
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
                    let local_shutdown = ShutdownFlag::new(shutdown);

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
            let hash = Types::Hash::default();
            let recv = ProcessorSessionRecv {
                sessions: self.sessions.clone(),
                state: self.state.clone()
            };
            let msgs = ProcessorSessionMsgs {
                state: self.state.clone(),
                idx: idx.clone()
            };

            Ok((local_shutdown, msgs, recv))
        } else {
            Err(ProcessorSessionDispatchError::Unknown { prin: prin })
        }
    }
}

impl<Prin, Encoder, Decoder, IDs> ScopedError
    for ProcessorSessionDispatchError<Prin, Encoder, Decoder, IDs>
where
    Encoder: ScopedError,
    Decoder: ScopedError,
    IDs: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            ProcessorSessionDispatchError::Proto { err } => err.scope(),
            ProcessorSessionDispatchError::Unknown { .. } |
            ProcessorSessionDispatchError::Exists { .. } |
            ProcessorSessionDispatchError::MutexPoison =>
                ErrorScope::Unrecoverable
        }
    }
}

impl<Prin, Encoder, Decoder, IDs> Display
    for ProcessorSessionDispatchError<Prin, Encoder, Decoder, IDs>
where
    Prin: Display,
    Encoder: Display,
    Decoder: Display,
    IDs: Display
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
