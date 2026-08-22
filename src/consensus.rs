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

use std::fmt::Debug;
use std::fmt::Display;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_common::codec::Encoder;
use constellation_common::config::Create;
use constellation_common::error::MutexPoison;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_component_common::consensus_ctl::ConsensusCtl;
use constellation_component_common::xact::XactConsensusSeal;
use constellation_streams::frags::Frags;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::large_obj::LargeObjProtoAddOutboundError;
use constellation_streams::large_obj::LargeObjSender;
use log::debug;
use log::warn;

use crate::state::PeerState;
use crate::state::ConsensusRoundID;
use crate::types::ConsensusMsgTypes;
use crate::types::SealTypes;

#[derive(Clone)]
pub(crate) struct ConsensusRecv<Types>
where
    Types: SealTypes {
    state: Arc<PeerState<Types>>
}

#[derive(Clone)]
pub(crate) struct ConsensusMsgs<Types>
where
    Types: SealTypes {
    state: Arc<PeerState<Types>>
}

impl<Types> LargeObjMsgs<
    Types::Hash,
    ConsensusCtl<ConsensusRoundID, Types::HashID, Types::Seal>
> for ConsensusMsgs<Types>
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
            ConsensusCtl<ConsensusRoundID, Types::HashID, Types::Seal>,
            WrapperCodec,
            F
        >
    ) -> Result<Option<Instant>, Self::AddMsgsError<WrapperCodec::EncodeError>>
    where
        WrapperCodec: Clone + Create
        + Encoder<ConsensusCtl<ConsensusRoundID, Types::HashID, Types::Seal>>,
        WrapperCodec::Config: Default,
        F: Frags {
        let (submit, next) = self.state.get_consensus_msgs()?;

        if let Some(submit) = submit {
            let submit = ConsensusCtl::Submit(submit);

            sender
                .add_outbound(&submit)
                .map_err(|err| WithMutexPoison::Inner { err: err })?;

            Ok(next)
        } else {
            Ok(None)
        }
    }
}

impl<Types> AuthNMsgRecv<
    Types::Prin,
    ConsensusCtl<ConsensusRoundID, Types::HashID, Types::Seal>,
    Types::ConsensusAuthMsg
> for ConsensusRecv<Types>
where
    Types: ConsensusMsgTypes
{
    /// Errors that can occur reporting messages.
    type RecvError = MutexPoison;

    /// Receive an authenticated message.
    fn recv_auth_msg(
        &mut self,
        msg: Types::ConsensusAuthMsg
    ) -> Result<(), Self::RecvError> {
        let (prin, msg) = msg.take();

        if let ConsensusCtl::Round(round) = msg {
            debug!(target: "consensus-session-recv",
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
            warn!(target: "consensus-session-recv",
                   "unexpected consensus submit from {}",
                   prin);
        }

        Ok(())
    }
}

impl<Types> ConsensusMsgs<Types>
where
    Types: SealTypes {
    #[inline]
    pub(crate) fn new(
        state: Arc<PeerState<Types>>
    ) -> Self {
        ConsensusMsgs { state: state }
    }
}

impl<Types> ConsensusRecv<Types>
where
    Types: SealTypes {
    #[inline]
    pub(crate) fn new(
        state: Arc<PeerState<Types>>
    ) -> Self {
        ConsensusRecv {
            state: state
        }
    }
}
