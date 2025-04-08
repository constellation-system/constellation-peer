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
use std::iter::once;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::PassthruMsgAuthN;
use constellation_common::codec::Codec;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_common::ids::IDGen;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_component_common::bus::large_obj::dispatch::SessionDispatch;
use constellation_component_common::xact::XactBatch;
use constellation_component_common::xact::XactBatchCodec;
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

pub(crate) struct ClientSessionDispatch<H, IDs, Prin>
where
    H: Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync {
    hash: PhantomData<H>,
    ids: PhantomData<IDs>,
    sessions: Arc<RwLock<HashMap<Prin, ClientSession>>>,
    config: LargeObjProtoConfig<
        <XactBatchCodec<H> as Codec<XactBatch<H::HashID>>>::Param,
        IDs::Config
    >
}

#[derive(Clone)]
pub(crate) struct ClientSessionRecv<H, Prin>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash {
    hash: PhantomData<H>,
    sessions: Arc<RwLock<HashMap<Prin, ClientSession>>>
}

#[derive(Clone)]
pub(crate) struct ClientSessionMsgs<H>
where
    H: HashAlgo {
    notify: Notify,
    when: Instant,
    count: u64,
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

unsafe impl<H, IDs, Prin> Send for ClientSessionDispatch<H, IDs, Prin>
where
    H: Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
}

unsafe impl<H, IDs, Prin> Sync for ClientSessionDispatch<H, IDs, Prin>
where
    H: Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
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

impl<H> LargeObjMsgs<H, XactBatch<H::HashID>> for ClientSessionMsgs<H>
where
    H: Clone + HashAlgo,
    H::HashID: Clone + Display + Hash + HashID + Eq
{
    type AddMsgsError<Encode>
        = LargeObjProtoAddOutboundError<H::HashID, Encode>
    where
        Encode: Display + ScopedError;

    fn add_msgs<WrapperCodec, F>(
        &mut self,
        sender: &mut LargeObjSender<H, XactBatch<H::HashID>, WrapperCodec, F>
    ) -> Result<Option<Instant>, Self::AddMsgsError<WrapperCodec::EncodeError>>
    where
        WrapperCodec: Clone + Codec<XactBatch<H::HashID>>,
        WrapperCodec::Param: Default,
        F: Frags {
        let now = Instant::now();

        if now >= self.when {
            debug!(target: "peer-clinet-msgs",
                   "generating outgoing batch, seqnum {}",
                   self.count);

            let batch = XactBatch::create(
                &self.hash,
                self.count,
                once(vec![0x11; 10000])
            );

            sender.add_outbound(&batch)?;
            self.count += 1;

            self.when = now + Duration::from_secs(5);
        }

        Ok(Some(self.when))
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
        msg: XactBatch<H>
    ) -> Result<(), Self::RecvError> {
        let guard = self
            .sessions
            .read()
            .map_err(|_| ClientSessionRecvError::MutexPoison)?;

        let _ = guard
            .get(prin)
            .ok_or(ClientSessionRecvError::NotFound { prin: prin.clone() })?;
        let (seqnum, reqs) = msg.take();

        debug!(target: "client-session-recv",
               "received message from {}, batch {} with {} reqs",
               prin, seqnum, reqs.len());

        for req in reqs {
            debug!(target: "client-session-recv",
                   "req {}: {:?}",
                   req.hash(), req.data());
        }

        Ok(())
    }
}

impl<H, IDs, Prin> ClientSessionDispatch<H, IDs, Prin>
where
    H: Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
    pub(crate) fn new(
        config: LargeObjProtoConfig<
            <XactBatchCodec<H> as Codec<XactBatch<H::HashID>>>::Param,
            IDs::Config
        >
    ) -> Self {
        let sessions = Arc::new(RwLock::new(HashMap::new()));

        ClientSessionDispatch {
            hash: PhantomData,
            ids: PhantomData,
            sessions: sessions,
            config: config
        }
    }
}

impl<H, IDs, Prin>
    SessionDispatch<
        H,
        XactBatch<H::HashID>,
        XactBatch<H::HashID>,
        PassthruMsgAuthN<XactBatch<H::HashID>, Prin>,
        XactBatchCodec<H>,
        IDs,
        ClientSessionMsgs<H>,
        ClientSessionRecv<H::HashID, Prin>,
        Prin
    > for ClientSessionDispatch<H, IDs, Prin>
where
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
    type SessionError = ClientSessionDispatchError<
        Prin,
        <XactBatchCodec<H> as Codec<XactBatch<H::HashID>>>::CreateError
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
                XactBatch<H::HashID>,
                XactBatch<H::HashID>,
                PassthruMsgAuthN<XactBatch<H::HashID>, Prin>,
                (),
                XactBatchCodec<H>,
                IDs,
                ClientSessionMsgs<H>,
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
        let hash = H::default();
        let recv = ClientSessionRecv {
            hash: PhantomData,
            sessions: self.sessions.clone()
        };
        let msgs = ClientSessionMsgs {
            notify: notify.clone(),
            when: Instant::now(),
            hash: hash.clone(),
            count: 0
        };
        let authn = PassthruMsgAuthN::default();
        let proto =
            LargeObjProto::create(self.config.clone(), recv, msgs, authn, hash)
                .map_err(|err| ClientSessionDispatchError::Proto {
                    err: err
                })?;

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
