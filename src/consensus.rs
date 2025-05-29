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
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_common::codec::Codec;
use constellation_common::error::MutexPoison;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_component_common::consensus_ctl::ConsensusCtl;
use constellation_component_common::xact::XactConsensusSeal;
use constellation_streams::frags::Frags;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::large_obj::LargeObjProtoAddOutboundError;
use constellation_streams::large_obj::LargeObjSender;
use log::debug;
use log::warn;

use crate::state::PeerState;

#[derive(Clone)]
pub(crate) struct ConsensusRecv<H, Prin, Seal>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Seal: Clone {
    hash: PhantomData<H>,
    state: Arc<PeerState<H, Prin, Seal>>
}

#[derive(Clone)]
pub(crate) struct ConsensusMsgs<H, Prin, Seal>
where
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    H: HashAlgo,
    H::HashID: Clone + Display + Eq + Hash + HashID,
    Seal: Clone {
    state: Arc<PeerState<H::HashID, Prin, Seal>>
}

unsafe impl<H, Prin, Seal> Send for ConsensusRecv<H, Prin, Seal>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Seal: Clone
{
}

unsafe impl<H, Prin, Seal> Sync for ConsensusRecv<H, Prin, Seal>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Seal: Clone
{
}

impl<H, Prin, Seal> LargeObjMsgs<H, ConsensusCtl<u128, H::HashID, Seal>>
    for ConsensusMsgs<H, Prin, Seal>
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
            ConsensusCtl<u128, H::HashID, Seal>,
            WrapperCodec,
            F
        >
    ) -> Result<Option<Instant>, Self::AddMsgsError<WrapperCodec::EncodeError>>
    where
        WrapperCodec: Clone + Codec<ConsensusCtl<u128, H::HashID, Seal>>,
        WrapperCodec::Param: Default,
        F: Frags {
        let (submit, next) = self.state.get_consensus_msgs()?;

        if let Some(submit) = submit {
            let submit = ConsensusCtl::Submit(submit);

            sender
                .add_outbound(&submit)
                .map_err(|err| WithMutexPoison::Inner { error: err })?;

            Ok(next)
        } else {
            Ok(None)
        }
    }
}

impl<Prin, H, Seal> AuthNMsgRecv<Prin, ConsensusCtl<u128, H, Seal>>
    for ConsensusRecv<H, Prin, Seal>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Seal: Clone
{
    /// Errors that can occur reporting messages.
    type RecvError = MutexPoison;

    /// Receive an authenticated message.
    fn recv_auth_msg(
        &mut self,
        prin: &Prin,
        msg: ConsensusCtl<u128, H, Seal>
    ) -> Result<(), Self::RecvError> {
        if let ConsensusCtl::Round(round) = msg {
            debug!(target: "processor-session-recv",
                   "received consensus round from {}",
                   prin);

            let (round, hashes, seals) = round.take();
            let seal = match seals {
                Some(seals) => XactConsensusSeal::new(hashes, seals),
                None => XactConsensusSeal::new(hashes, vec![])
            };

            self.state
                .add_consensus_seal(round, seal)
                .map_err(|_| MutexPoison)?;
        } else {
            warn!(target: "processor-session-recv",
                   "unexpected consensus submit from {}",
                   prin);
        }

        Ok(())
    }
}

impl<H, Prin, Seal> ConsensusMsgs<H, Prin, Seal>
where
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    H: Clone + HashAlgo,
    H::HashID: Clone + Display + Eq + Hash + HashID,
    Seal: Clone
{
    #[inline]
    pub(crate) fn new(state: Arc<PeerState<H::HashID, Prin, Seal>>) -> Self {
        ConsensusMsgs { state: state }
    }
}

impl<Prin, H, Seal> ConsensusRecv<H, Prin, Seal>
where
    H: Clone + Display + Hash + HashID + Eq + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Seal: Clone
{
    #[inline]
    pub(crate) fn new(state: Arc<PeerState<H, Prin, Seal>>) -> Self {
        ConsensusRecv {
            hash: PhantomData,
            state: state
        }
    }
}
