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

use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;

use constellation_auth::authn::AuthNedDestruct;
use constellation_channels::config::ThreadedNSNameCachesConfig;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_common::config::Create;
use constellation_common::codec::Decoder;
use constellation_common::codec::Encoder;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_component_common::bus::types::DispatchLargeObjBusTypes;
use constellation_component_common::bus::types::MulticastLargeObjBusTypes;
use constellation_component_common::bus::types::SessionDispatchTypes;
use constellation_component_common::consensus_ctl::ConsensusCtl;
use constellation_component_common::xact::XactBlobBatch;

use crate::state::ConsensusRoundID;
use crate::clients::ClientSessionMsgs;
use crate::clients::ClientSessionRecv;
use crate::consensus::ConsensusMsgs;
use crate::consensus::ConsensusRecv;
use crate::processors::ProcessorSessionMsgs;
use crate::processors::ProcessorSessionRecv;

pub trait ConsensusBaseTypes {
    type ConsensusPrin: Clone + Debug + Display + Eq + Hash + Send + Sync;
    type ConsensusSeal: Clone + Send;
    type ConsensusSealCodecConfig: Clone + Default;
    type ConsensusSealCreateError: Debug + Display + ScopedError;
    type ConsensusSealCodec: Decoder<Self::ConsensusSeal>
        + Encoder<Self::ConsensusSeal>
        + Create<Config = Self::ConsensusSealCodecConfig,
                 CreateError = Self::ConsensusSealCreateError> + Send;
    type ConsensusHashID: Clone + Debug + Display + Hash + HashID + Eq + Send;
    type ConsensusHash: Clone + Default
        + HashAlgo<HashID = Self::ConsensusHashID> + Send;
    type ConsensusIDsCreateError: Debug + Display + ScopedError;
}

pub trait ConsensusMsgTypes: ConsensusBaseTypes {
    type ConsensusAuthMsg: AuthNedDestruct<
        Self::ConsensusPrin,
        ConsensusCtl<ConsensusRoundID, Self::ConsensusHashID,
                     Self::ConsensusSeal>
    >;
}

pub trait ClientsMsgTypes: ConsensusBaseTypes {
    type ClientsPrin: Clone + Debug + Display + Eq + Hash + Send + Sync;
    type ClientsAuthMsg: AuthNedDestruct<
        Self::ClientsPrin,
        XactBlobBatch<ConsensusRoundID,
                      Self::ConsensusHashID,
                      Self::ConsensusSeal>,
    >;
}

pub trait ClientsSessionDispatchTypes: ClientsMsgTypes + Sized {
    type ClientsSessionDispTypes: SessionDispatchTypes<
        SessionPrin = Self::ClientsPrin,
        Recv = ClientSessionRecv<Self>,
        Msgs = ClientSessionMsgs<Self>
    >;
}

pub trait ProcessorsMsgTypes: ConsensusBaseTypes {
    type ProcessorsPrin: Clone + Debug + Display + Eq + Hash + Send + Sync;
    type ProcessorsAuthMsg: AuthNedDestruct<
        Self::ProcessorsPrin,
        XactBlobBatch<ConsensusRoundID,
                      Self::ConsensusHashID,
                      Self::ConsensusSeal>,
    >;
    type ProcessorsCodecCreateError: Debug + Display + ScopedError;
    type ProcessorsCodec: Clone
        + Create
        + Decoder<XactBlobBatch<ConsensusRoundID,
                                Self::ConsensusHashID,
                                Self::ConsensusSeal>>
        + Encoder<XactBlobBatch<ConsensusRoundID,
                                Self::ConsensusHashID,
                                Self::ConsensusSeal>>;
}

pub trait ProcessorSessionDispatchTypes: ClientsMsgTypes
    + ProcessorsMsgTypes + Sized {
    type ProcessorSessionDispTypes: SessionDispatchTypes<
        SessionPrin = Self::ProcessorsPrin,
        Recv = ProcessorSessionRecv<Self>,
        Msgs = ProcessorSessionMsgs<Self>
    >;
}

pub trait PeerComponentTypes: ConsensusMsgTypes
    + ClientsSessionDispatchTypes
    + ProcessorSessionDispatchTypes
    + Sized {
    type Ctx: 'static + Clone + NSNameCachesCtx + Send;
    type Addr: Clone + Debug + Display + Eq + Hash + Send;

    type ClientsSessAuthConfig: Clone + Send;
    type ClientsMsgAuthConfig;
    type ClientsLargeObjMsgAuthConfig: Send;
    type ClientsHashConfig: Default;
    type ClientsIDsConfig: Default;
    type ClientsUDPConfig: Clone + Default + Send;
    type ClientsUnixConfig: Clone + Default + Send;
    type ClientsEpochsConfig: Default;
    type ClientsCreateError: Debug + Display + Send;
    type ClientsBusTypes: 'static + DispatchLargeObjBusTypes<
        Self::Ctx,
        HashConfig = Self::ClientsHashConfig,
        EpochsConfig = Self::ClientsEpochsConfig,
        EncoderConfig = (),
        DecoderConfig = (),
        IDsConfig = Self::ClientsIDsConfig,
        UDPConfig = Self::ClientsUDPConfig,
        UnixConfig = Self::ClientsUnixConfig,
        SessionAuthConfig = Self::ClientsSessAuthConfig,
        MsgAuthConfig = Self::ClientsMsgAuthConfig,
        LargeObjMsgAuthConfig = Self::ClientsLargeObjMsgAuthConfig,
        DispTypes = Self::ClientsSessionDispTypes
    > + Create<CreateError = Self::ClientsCreateError>;
    type ProcessorsSessAuthConfig: Clone + Send;
    type ProcessorsMsgAuthConfig;
    type ProcessorsLargeObjMsgAuthConfig: Send;
    type ProcessorsHashConfig: Default;
    type ProcessorsIDsConfig: Default;
    type ProcessorsUDPConfig: Clone + Default + Send;
    type ProcessorsUnixConfig: Clone + Default + Send;
    type ProcessorsEpochsConfig: Default;
    type ProcessorsCreateError: Debug + Display;
    type ProcessorsBusTypes: 'static + DispatchLargeObjBusTypes<
        Self::Ctx,
        HashConfig = Self::ProcessorsHashConfig,
        EpochsConfig = Self::ProcessorsEpochsConfig,
        EncoderConfig = (),
        DecoderConfig = (),
        IDsConfig = Self::ProcessorsIDsConfig,
        UDPConfig = Self::ProcessorsUDPConfig,
        UnixConfig = Self::ProcessorsUnixConfig,
        MsgAuthConfig = Self::ProcessorsMsgAuthConfig,
        SessionAuthConfig = Self::ProcessorsSessAuthConfig,
        LargeObjMsgAuthConfig = Self::ProcessorsLargeObjMsgAuthConfig,
        DispTypes = Self::ProcessorSessionDispTypes
    > + Create<CreateError = Self::ProcessorsCreateError>;
    type ConsensusSessAuthConfig: Clone + Send;
    type ConsensusMsgAuthConfig: Send;
    type ConsensusMsgAuthCreateError: Debug + Display;
    type ConsensusLargeObjMsgAuthConfig: Send;
    type ConsensusHashConfig: Default;
    type ConsensusHashCreateError: Debug + Display;
    type ConsensusIDsConfig: Default;
    type ConsensusUDPConfig: Clone + Default + Send;
    type ConsensusUnixConfig: Clone + Default + Send;
    type ConsensusEpochsConfig: Default + Send;
    type ConsensusCreateError: Debug + Display;
    type ConsensusBusTypes: 'static + MulticastLargeObjBusTypes<
        Self::Ctx,
        SessionPrin = Self::ConsensusPrin,
        Msgs = ConsensusMsgs<Self>,
        Recv = ConsensusRecv<Self>,
        Hash = Self::ConsensusHash,
        HashID = Self::ConsensusHashID,
        HashConfig = Self::ConsensusHashConfig,
        EpochsConfig = Self::ConsensusEpochsConfig,
        EncoderConfig = (),
        EncoderCreateError = Infallible,
        DecoderConfig = (),
        DecoderCreateError = Infallible,
        IDsConfig = Self::ConsensusIDsConfig,
        ResolverConfig = ThreadedNSNameCachesConfig,
        UDPConfig = Self::ConsensusUDPConfig,
        UnixConfig = Self::ConsensusUnixConfig,
        MsgAuthConfig = Self::ConsensusMsgAuthConfig,
        SessionAuthConfig = Self::ConsensusSessAuthConfig,
        LargeObjMsgAuthConfig = Self::ConsensusLargeObjMsgAuthConfig,
        HashCreateError = Self::ConsensusHashCreateError,
        MsgAuthCreateError = Self::ConsensusMsgAuthCreateError,
        IDsCreateError = Self::ConsensusIDsCreateError,
    > + Create<CreateError = Self::ConsensusCreateError> + Send;
}
