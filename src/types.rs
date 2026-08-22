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
use std::hash::Hash;

use constellation_auth::authn::AuthNed;
use constellation_common::config::Create;
use constellation_common::codec::Decoder;
use constellation_common::codec::Encoder;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_component_common::bus::dispatch::SessionDispatchTypes;
use constellation_component_common::consensus_ctl::ConsensusCtl;
use constellation_component_common::xact::XactBlobBatch;
use constellation_streams::large_obj::LargeObjID;

use crate::state::ConsensusRoundID;
use crate::clients::ClientSessionMsgs;
use crate::clients::ClientSessionRecv;
use crate::processors::ProcessorSessionMsgs;
use crate::processors::ProcessorSessionRecv;

pub trait SealTypes {
    type Prin: Clone + Debug + Display + Eq + Hash + Send + Sync;
    type Seal: Clone;
    type SealCodecConfig: Clone + Default;
    type SealCreateError: Debug + Display + ScopedError;
    type SealCodec: Decoder<Self::Seal> + Encoder<Self::Seal>
        + Create<Config = Self::SealCodecConfig,
                 CreateError = Self::SealCreateError>;
    type HashID: Clone + Display + Hash + HashID + Eq + Send;
    type Hash: Clone + Default + HashAlgo<HashID = Self::HashID> + Send;
}

pub trait LargeObjSessionTypes: SealTypes {
    type IDsConfig: Clone  + Default;
    type IDsCreateError: Debug + Display + ScopedError;
    type IDs: Iterator<Item = LargeObjID>
        + Create<Config = Self::IDsConfig,
                 CreateError = Self::IDsCreateError> + Send;
}

pub trait ConsensusMsgTypes: LargeObjSessionTypes {
    type ConsensusAuthMsg: AuthNed<
        Self::Prin,
        ConsensusCtl<ConsensusRoundID, Self::HashID, Self::Seal>
    >;
    type ConsensusCodecConfig: Clone + Default;
    type ConsensusCodecCreateError: Debug + Display + ScopedError;
    type ConsensusCodec: Clone
        + Create<Config = Self::ConsensusCodecConfig,
                 CreateError = Self::CtlCodecCreateError>
        + Decoder<ConsensusCtl<ConsensusRoundID, Self::HashID, Self::Seal>>
        + Encoder<ConsensusCtl<ConsensusRoundID, Self::HashID, Self::Seal>>;
}

pub trait ClientMsgTypes: LargeObjSessionTypes {
    type ClientAuthMsg: AuthNed<
        Self::Prin,
        XactBlobBatch<ConsensusRoundID, Self::HashID, Self::Seal>,
    >;
    type ClientCodecCreateError: Debug + Display + ScopedError;
    type ClientCodec: Clone
        + Create<CreateError = Self::ClientCodecCreateError>
        + Decoder<XactBlobBatch<ConsensusRoundID, Self::HashID, Self::Seal>>
        + Encoder<XactBlobBatch<ConsensusRoundID, Self::HashID, Self::Seal>>;
}

pub trait ClientSessionDispatchTypes: ClientMsgTypes {
    type ClientSessionDispTypes: SessionDispatchTypes<
        SessionPrin = Self::Prin,
        MsgPrin = Self::Prin,
        InMsg = XactBlobBatch<ConsensusRoundID, Self::HashID, Self::Seal>,
        OutMsg = XactBlobBatch<ConsensusRoundID, Self::HashID, Self::Seal>,
        AuthNMsg = Self::ClientAuthMsg,
        Recv = ClientSessionRecv<Self>,
        Msgs = ClientSessionMsgs<Self>
    >;
}

pub trait ProcessorMsgTypes: LargeObjSessionTypes {
    type ProcessorAuthMsg: AuthNed<
        Self::Prin,
        XactBlobBatch<ConsensusRoundID, Self::HashID, Self::Seal>,
    >;
    type ProcessorCodecCreateError: Debug + Display + ScopedError;
    type ProcessorCodec: Clone
        + Create<CreateError = Self::ClientCodecCreateError>
        + Decoder<XactBlobBatch<ConsensusRoundID, Self::HashID, Self::Seal>>
        + Encoder<XactBlobBatch<ConsensusRoundID, Self::HashID, Self::Seal>>;
}

pub trait ProcessorSessionDispatchTypes: ProcessorMsgTypes {
    type ProcessorSessionDispTypes: SessionDispatchTypes<
        SessionPrin = Self::Prin,
        MsgPrin = Self::Prin,
        InMsg = XactBlobBatch<ConsensusRoundID, Self::HashID, Self::Seal>,
        OutMsg = XactBlobBatch<ConsensusRoundID, Self::HashID, Self::Seal>,
        AuthNMsg = Self::ProcessorAuthMsg,
        Recv = ProcessorSessionRecv<Self>,
        Msgs = ProcessorSessionMsgs<Self>
    >;
}
