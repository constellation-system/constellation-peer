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
use std::convert::Infallible;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::PassthruMsgAuthN;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_common::hashid::SHA3ID;
use constellation_common::ids::IDGen;
use constellation_common::net::PrivateMsgs;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_component_common::bus::large_obj::dispatch::SessionDispatch;
use constellation_component_common::xact::XactBatch;
use constellation_component_common::xact::XactBatchCodec;
use constellation_streams::frags::OutboundFrags;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::large_obj::LargeObjMsg;
use constellation_streams::large_obj::LargeObjProto;
use log::debug;
use log::trace;

pub(crate) struct ClientSessionDispatch<H, IDs, Prin>
where
    H: HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync {
    hash: PhantomData<H>,
    ids: PhantomData<IDs>,
    sessions: Arc<RwLock<HashMap<Prin, ClientSession>>>
}

#[derive(Clone)]
pub(crate) struct ClientSessionRecv<H, Prin>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash {
    hash: PhantomData<H>,
    sessions: Arc<RwLock<HashMap<Prin, ClientSession>>>
}

pub(crate) struct ClientMsgs {
    notify: Notify
}

struct ClientSession {
    local_shutdown: ShutdownFlag
}

#[derive(Debug)]
pub(crate) enum ClientSessionDispatchError<Prin> {
    Exists { prin: Prin },
    MutexPoison
}

#[derive(Debug)]
pub(crate) enum ClientSessionRecvError<Prin> {
    NotFound { prin: Prin },
    MutexPoison
}

unsafe impl<H, IDs, Prin> Send for ClientSessionDispatch<H, IDs, Prin>
where
    H: HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
}

unsafe impl<H, IDs, Prin> Sync for ClientSessionDispatch<H, IDs, Prin>
where
    H: HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
}

unsafe impl<H, Prin> Send for ClientSessionRecv<H, Prin>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash
{
}

unsafe impl<H, Prin> Sync for ClientSessionRecv<H, Prin>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash
{
}

impl PrivateMsgs<LargeObjMsg<SHA3ID>> for ClientMsgs {
    /// Type of errors that can occur when collecting messages.
    type MsgsError = Infallible;

    fn msgs(
        &mut self
    ) -> Result<
        (Option<Vec<LargeObjMsg<SHA3ID>>>, Option<Instant>),
        Self::MsgsError
    > {
        let now = Instant::now();
        let when = now + Duration::from_secs(1);
        let out = vec![LargeObjMsg::finish(1)];

        Ok((Some(out), Some(when)))
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

impl<Prin, H> AuthNMsgRecv<Prin, XactBatch<H>> for ClientSessionRecv<H, Prin>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash
{
    /// Errors that can occur reporting messages.
    type RecvError = ClientSessionRecvError<Prin>;

    /// Receive an authenticated message.
    fn recv_auth_msg(
        &mut self,
        prin: &Prin,
        _msg: XactBatch<H>
    ) -> Result<(), Self::RecvError> {
        let guard = self
            .sessions
            .read()
            .map_err(|_| ClientSessionRecvError::MutexPoison)?;

        let _ = guard
            .get(prin)
            .ok_or(ClientSessionRecvError::NotFound { prin: prin.clone() })?;

        debug!(target: "client-session-recv",
               "received message from {}",
               prin);
        Ok(())
    }
}

impl<H, IDs, Prin> ClientSessionDispatch<H, IDs, Prin>
where
    H: Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
    pub(crate) fn new() -> Self {
        let sessions = Arc::new(RwLock::new(HashMap::new()));

        ClientSessionDispatch {
            hash: PhantomData,
            ids: PhantomData,
            sessions: sessions
        }
    }
}

impl<H, IDs, Prin>
    SessionDispatch<
        LargeObjMsg<H::HashID>,
        LargeObjProto<
            H::HashID,
            XactBatch<H::HashID>,
            XactBatch<H::HashID>,
            PassthruMsgAuthN<XactBatch<H::HashID>, Prin>,
            (),
            XactBatchCodec<H>,
            IDs,
            ClientSessionRecv<H::HashID, Prin>,
            OutboundFrags
        >,
        Prin,
        LargeObjProto<
            H::HashID,
            XactBatch<H::HashID>,
            XactBatch<H::HashID>,
            PassthruMsgAuthN<XactBatch<H::HashID>, Prin>,
            (),
            XactBatchCodec<H>,
            IDs,
            ClientSessionRecv<H::HashID, Prin>,
            OutboundFrags
        >
    > for ClientSessionDispatch<H, IDs, Prin>
where
    H: Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
    type SessionError = ClientSessionDispatchError<Prin>;

    fn session(
        &self,
        prin: Prin
    ) -> Result<
        (
            ShutdownFlag,
            Notify,
            LargeObjProto<
                H::HashID,
                XactBatch<H::HashID>,
                XactBatch<H::HashID>,
                PassthruMsgAuthN<XactBatch<H::HashID>, Prin>,
                (),
                XactBatchCodec<H>,
                IDs,
                ClientSessionRecv<H::HashID, Prin>,
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
            _ => Err(ClientSessionDispatchError::Exists { prin: prin })
        }?;
        let recv = ClientSessionRecv {
            sessions: self.sessions.clone()
        };
        let msgs = ClientMsgs {
            notify: notify.clone()
        };

        Ok((local_shutdown, msgs, notify, recv))
    }
}

impl<Prin> Display for ClientSessionDispatchError<Prin>
where
    Prin: Display
{
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
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
