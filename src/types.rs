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
use constellation_component_common::consensus_ctl::ConsensusCtl;
use constellation_component_common::xact::XactBlobBatch;
use serde::Deserialize;
use serde::Serialize;

use crate::state::ConsensusRoundID;
use crate::clients::ClientsSessionDispatchTypes;
use crate::consensus::ConsensusMsgs;
use crate::consensus::ConsensusRecv;
use crate::processors::ProcessorsSessionDispatchTypes;

pub trait ConsensusBaseTypes {
    type ConsensusPrin: Clone + Debug + Display + Eq + Hash + for<'a> Deserialize<'a> + Serialize + Send + Sync;
    type ConsensusSeal: Clone + Send;
    type ConsensusSealCodecConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize;
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

pub trait ProcessorsMsgTypes: ConsensusBaseTypes {
    type ProcessorsPrin: Clone + Debug + Display + Eq + Hash + for<'a> Deserialize<'a> + Serialize + Send + Sync;
    type ProcessorsAuthMsg: AuthNedDestruct<
        Self::ProcessorsPrin,
        XactBlobBatch<ConsensusRoundID,
                      Self::ConsensusHashID,
                      Self::ConsensusSeal>,
    >;
}

pub trait PeerComponentTypes: ConsensusMsgTypes
    + ClientsMsgTypes
    + ProcessorsMsgTypes
    + Sized {
    type Ctx: 'static + Clone + NSNameCachesCtx + Send;
    type Addr: Clone + Debug + Display + Eq + Hash + Send;
    type ClientsSessAuthConfig: Clone + for<'a> Deserialize<'a> + Serialize + Send;
    type ClientsMsgAuthConfig: for<'a> Deserialize<'a> + Serialize;
    type ClientsLargeObjMsgAuthConfig: for<'a> Deserialize<'a> + Serialize + Send;
    type ClientsHashConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type ClientsIDsConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type ClientsUDPConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize + Send;
    type ClientsUnixConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize + Send;
    type ClientsEpochsConfig: Default + for<'a> Deserialize<'a> + Serialize;
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
        DispTypes = ClientsSessionDispatchTypes<Self>
    > + Create<CreateError = Self::ClientsCreateError>;
    type ProcessorsSessAuthConfig: Clone + for<'a> Deserialize<'a> + Serialize + Send;
    type ProcessorsMsgAuthConfig: for<'a> Deserialize<'a> + Serialize;
    type ProcessorsLargeObjMsgAuthConfig: for<'a> Deserialize<'a> + Serialize + Send;
    type ProcessorsHashConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type ProcessorsIDsConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type ProcessorsUDPConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize + Send;
    type ProcessorsUnixConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize + Send;
    type ProcessorsEpochsConfig: Default + for<'a> Deserialize<'a> + Serialize;
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
        DispTypes = ProcessorsSessionDispatchTypes<Self>
    > + Create<CreateError = Self::ProcessorsCreateError>;
    type ConsensusSessAuthConfig: Clone + for<'a> Deserialize<'a> + Serialize + Send;
    type ConsensusMsgAuthConfig: for<'a> Deserialize<'a> + Serialize + Send;
    type ConsensusMsgAuthCreateError: Debug + Display;
    type ConsensusLargeObjMsgAuthConfig: for<'a> Deserialize<'a> + Serialize + Send;
    type ConsensusHashConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type ConsensusHashCreateError: Debug + Display;
    type ConsensusIDsConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type ConsensusUDPConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize + Send;
    type ConsensusUnixConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize + Send;
    type ConsensusEpochsConfig: Default + for<'a> Deserialize<'a> + Serialize + Send;
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

pub struct StandalonePeerTypes;

#[cfg(feature = "standalone")]
use constellation_auth::authn::BasicAuthNed;
#[cfg(feature = "standalone")]
use constellation_common::hashid::SHA3Algo;
#[cfg(feature = "standalone")]
use constellation_common::hashid::SHA3ID;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::test::TestSeal;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::test::TestSealCodec;

#[cfg(feature = "standalone")]
impl ConsensusBaseTypes for StandalonePeerTypes {
    type ConsensusPrin = String;
    type ConsensusSeal = TestSeal;
    type ConsensusSealCodecConfig = ();
    type ConsensusSealCreateError = Infallible;
    type ConsensusSealCodec = TestSealCodec;
    type ConsensusHashID = SHA3ID;
    type ConsensusHash = SHA3Algo;
    type ConsensusIDsCreateError = Infallible;
}

#[cfg(feature = "standalone")]
impl ConsensusMsgTypes for StandalonePeerTypes {
    type ConsensusAuthMsg =
        BasicAuthNed<String, ConsensusCtl<ConsensusRoundID, SHA3ID, TestSeal>>;
}

#[cfg(feature = "standalone")]
impl ClientsMsgTypes for StandalonePeerTypes {
    type ClientsPrin = String;
    type ClientsAuthMsg = BasicAuthNed<
        String,
        XactBlobBatch<ConsensusRoundID,
                      Self::ConsensusHashID,
                      Self::ConsensusSeal>,
    >;
}

#[cfg(feature = "standalone")]
impl ProcessorsMsgTypes for StandalonePeerTypes {
    type ProcessorsPrin = String;
    type ProcessorsAuthMsg = BasicAuthNed<
        String,
        XactBlobBatch<ConsensusRoundID,
                      Self::ConsensusHashID,
                      Self::ConsensusSeal>,
    >;
}
