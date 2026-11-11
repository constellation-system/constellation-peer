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
use crate::processors::ProcessorSessionMsgs;
use crate::processors::ProcessorSessionRecv;
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
    type ClientsSessAuthConfig: Clone + for<'a> Deserialize<'a> + Serialize + Send;
    type ClientsMsgAuthConfig: for<'a> Deserialize<'a> + Serialize;
    type ClientsLargeObjMsgAuthConfig: for<'a> Deserialize<'a> + Serialize + Send;
    type ClientsHashConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type ClientsIDsConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type ClientsUDPConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize + Send;
    type ClientsUnixConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize + Send;
    type ClientsEpochsConfig: Default + for<'a> Deserialize<'a> + Serialize;
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
    >;
    type ProcessorsSessAuthConfig: Clone + for<'a> Deserialize<'a> + Serialize + Send;
    type ProcessorsMsgAuthConfig: for<'a> Deserialize<'a> + Serialize;
    type ProcessorsLargeObjMsgAuthConfig: for<'a> Deserialize<'a> + Serialize + Send;
    type ProcessorsHashConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type ProcessorsIDsConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type ProcessorsUDPConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize + Send;
    type ProcessorsUnixConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize + Send;
    type ProcessorsEpochsConfig: Default + for<'a> Deserialize<'a> + Serialize;
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
    >;
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
    type ConsensusResolverConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize + Send;
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
        ResolverConfig = Self::ConsensusResolverConfig,
        IDsConfig = Self::ConsensusIDsConfig,
        UDPConfig = Self::ConsensusUDPConfig,
        UnixConfig = Self::ConsensusUnixConfig,
        MsgAuthConfig = Self::ConsensusMsgAuthConfig,
        SessionAuthConfig = Self::ConsensusSessAuthConfig,
        LargeObjMsgAuthConfig = Self::ConsensusLargeObjMsgAuthConfig,
        HashCreateError = Self::ConsensusHashCreateError,
        MsgAuthCreateError = Self::ConsensusMsgAuthCreateError,
        IDsCreateError = Self::ConsensusIDsCreateError,
    > + Send;
}

#[cfg(feature = "standalone")]
use std::net::SocketAddr;

#[cfg(feature = "standalone")]
use constellation_auth::authn::BasicAuthNed;
#[cfg(feature = "standalone")]
use constellation_auth::authn::PassthruMsgAuthN;
#[cfg(feature = "standalone")]
use constellation_auth::authn::basic::BasicAuthN;
#[cfg(feature = "standalone")]
use constellation_auth::config::BasicAuthNConfig;
#[cfg(feature = "standalone")]
use constellation_channels::config::CompoundFarChannelXfrmPeerAddr;
#[cfg(feature = "standalone")]
use constellation_channels::config::CompoundFarEndpoint;
#[cfg(feature = "standalone")]
use constellation_channels::config::ResolverConfig;
#[cfg(feature = "standalone")]
use constellation_channels::far::compound::CompoundFlow;
#[cfg(feature = "standalone")]
use constellation_channels::far::types::CompoundFarChannelsBasicAuthNedLargeObjChan;
#[cfg(feature = "standalone")]
use constellation_channels::resolve::MixedResolver;
#[cfg(feature = "standalone")]
use constellation_common::hashid::SHA3Algo;
#[cfg(feature = "standalone")]
use constellation_common::hashid::SHA3ID;
#[cfg(feature = "standalone")]
use constellation_common::ids::AscendingCount;
#[cfg(feature = "standalone")]
use constellation_common::net::PassthruDatagramXfrm;
#[cfg(feature = "standalone")]
use constellation_common::net::PassthruDatagramXfrmParam;
#[cfg(feature = "standalone")]
use constellation_common::unix::UnixSocketPath;
#[cfg(feature = "standalone")]
use constellation_component_common::consensus_ctl::ConsensusCtlCodec;
#[cfg(feature = "standalone")]
use constellation_component_common::consensus_ctl::ConsensusCtlRoundEncodeError;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::XactBatchCodecCreateError;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::XactBatchCodecEncodeError;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::XactCommittedRoundCodecCreateError;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::XactCommittedRoundCodecEncodeError;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::XactNotifyCodecCreateError;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::XactNotifyCodecEncodeError;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::XactReqCodecCreateError;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::XactReqCodecEncodeError;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::XactSealedCodecCreateError;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::XactSealedCodecError;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::XactBlobBatchCodec;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::test::TestSeal;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::test::TestSealCodec;
#[cfg(feature = "standalone")]
use constellation_streams::large_obj::LargeObjID;
#[cfg(feature = "standalone")]
use constellation_streams::large_obj::LargeObjMsg;

#[cfg(feature = "standalone")]
use crate::component::StandaloneCtx;
#[cfg(feature = "standalone")]
use crate::clients::ClientSessionMsgs;
#[cfg(feature = "standalone")]
use crate::clients::ClientSessionRecv;
#[cfg(feature = "standalone")]
use crate::clients::ClientSessionRecvError;
#[cfg(feature = "standalone")]
use crate::processors::ProcessorSessionRecvError;

#[cfg(feature = "standalone")]
#[derive(Clone)]
pub struct StandalonePeerTypes;

#[cfg(feature = "standalone")]
pub struct StandaloneClientsBusTypes;

#[cfg(feature = "standalone")]
pub struct StandaloneProcessorsBusTypes;

#[cfg(feature = "standalone")]
pub struct StandaloneConsensusBusTypes;

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

#[cfg(feature = "standalone")]
impl DispatchLargeObjBusTypes<StandaloneCtx> for StandaloneClientsBusTypes {
    type AuthNChan = CompoundFarChannelsBasicAuthNedLargeObjChan<
        String,
        SHA3Algo,
        PassthruDatagramXfrm<UnixSocketPath>,
        PassthruDatagramXfrm<SocketAddr>
    >;
    type AuthNMsg = BasicAuthNed<String, XactBlobBatch<ConsensusRoundID, SHA3ID, TestSeal>>;
    type AuthNSession = BasicAuthNed<
        String,
        CompoundFlow<
            PassthruDatagramXfrm<UnixSocketPath>,
            PassthruDatagramXfrm<SocketAddr>
        >
    >;
    type Decoder = XactBlobBatchCodec<ConsensusRoundID, SHA3Algo, TestSeal, TestSealCodec>;
    type DecoderConfig = ();
    type DecoderCreateError = XactBatchCodecCreateError<
        XactSealedCodecCreateError<
            Infallible,
            XactReqCodecCreateError<Infallible, Infallible>
        >,
        XactCommittedRoundCodecCreateError<
            Infallible,
            XactReqCodecCreateError<Infallible, Infallible>
        >,
        XactNotifyCodecCreateError<Infallible, Infallible>
    >;
    type DispTypes = ClientsSessionDispatchTypes<StandalonePeerTypes>;
    type EncodeError = XactBatchCodecEncodeError<
        asn1rs::io::per::err::Error,
        XactSealedCodecError<
            asn1rs::io::per::err::Error,
            Infallible,
            XactReqCodecEncodeError<
                Infallible,
                Infallible,
                asn1rs::io::per::err::Error,
            >
        >,
        XactCommittedRoundCodecEncodeError<
            asn1rs::io::per::err::Error,
            Infallible,
            XactReqCodecEncodeError<
                Infallible,
                Infallible,
                asn1rs::io::per::err::Error,
            >
        >,
        XactNotifyCodecEncodeError<
            asn1rs::io::per::err::Error,
            Infallible,
            Infallible
        >
    >;
    type Encoder = XactBlobBatchCodec<ConsensusRoundID, SHA3Algo, TestSeal, TestSealCodec>;
    type EncoderConfig = ();
    type EncoderCreateError = XactBatchCodecCreateError<
        XactSealedCodecCreateError<
            Infallible,
            XactReqCodecCreateError<Infallible, Infallible>
        >,
        XactCommittedRoundCodecCreateError<
            Infallible,
            XactReqCodecCreateError<Infallible, Infallible>
        >,
        XactNotifyCodecCreateError<Infallible, Infallible>
    >;
    type Epoch = u128;
    type Epochs = AscendingCount<u128>;
    type EpochsConfig = ();
    type EpochsCreateError = Infallible;
    type Hash = SHA3Algo;
    type HashConfig = ();
    type HashCreateError = Infallible;
    type HashID = SHA3ID;
    type IDs = AscendingCount<LargeObjID>;
    type IDsConfig = ();
    type IDsCreateError = Infallible;
    type InMsg = XactBlobBatch<ConsensusRoundID, SHA3ID, TestSeal>;
    type LargeObjAuthNMsg = BasicAuthNed<String, LargeObjMsg<SHA3ID>>;
    type LargeObjMsgAuth = PassthruMsgAuthN<LargeObjMsg<SHA3ID>, String>;
    type LargeObjMsgAuthConfig = ();
    type LargeObjMsgAuthCreateError = Infallible;
    type LargeObjWrapper = LargeObjMsg<SHA3ID>;
    type MsgAuth =
        PassthruMsgAuthN<XactBlobBatch<ConsensusRoundID, SHA3ID, TestSeal>,
                         String>;
    type MsgAuthConfig = ();
    type MsgAuthCreateError = Infallible;
    type MsgAuthError = Infallible;
    type MsgPrin = String;
    type Msgs = ClientSessionMsgs<StandalonePeerTypes>;
    type Origin = CompoundFarEndpoint;
    type OutMsg = XactBlobBatch<ConsensusRoundID, SHA3ID, TestSeal>;
    type Recv = ClientSessionRecv<StandalonePeerTypes>;
    type RecvError = ClientSessionRecvError<String>;
    type Resolver =
        MixedResolver<CompoundFarChannelXfrmPeerAddr, CompoundFarEndpoint>;
    type ResolverConfig = ResolverConfig;
    type SessionAuth = BasicAuthN<String>;
    type SessionAuthConfig = BasicAuthNConfig<String>;
    type SessionAuthNError = Infallible;
    type SessionPrin = String;
    type UDP = PassthruDatagramXfrm<SocketAddr>;
    type UDPConfig = PassthruDatagramXfrmParam;
    type UDPError = Infallible;
    type Unix = PassthruDatagramXfrm<UnixSocketPath>;
    type UnixConfig = PassthruDatagramXfrmParam;
    type UnixError = Infallible;
    type Wrapper = XactBlobBatch<ConsensusRoundID, SHA3ID, TestSeal>;
}

#[cfg(feature = "standalone")]
impl DispatchLargeObjBusTypes<StandaloneCtx> for StandaloneProcessorsBusTypes {
    type AuthNChan = CompoundFarChannelsBasicAuthNedLargeObjChan<
        String,
        SHA3Algo,
        PassthruDatagramXfrm<UnixSocketPath>,
        PassthruDatagramXfrm<SocketAddr>
    >;
    type AuthNMsg = BasicAuthNed<String, XactBlobBatch<ConsensusRoundID, SHA3ID, TestSeal>>;
    type AuthNSession = BasicAuthNed<
        String,
        CompoundFlow<
            PassthruDatagramXfrm<UnixSocketPath>,
            PassthruDatagramXfrm<SocketAddr>
        >
    >;
    type Decoder = XactBlobBatchCodec<ConsensusRoundID, SHA3Algo, TestSeal, TestSealCodec>;
    type DecoderConfig = ();
    type DecoderCreateError = XactBatchCodecCreateError<
        XactSealedCodecCreateError<
            Infallible,
            XactReqCodecCreateError<Infallible, Infallible>
        >,
        XactCommittedRoundCodecCreateError<
            Infallible,
            XactReqCodecCreateError<Infallible, Infallible>
        >,
        XactNotifyCodecCreateError<Infallible, Infallible>
    >;
    type DispTypes = ProcessorsSessionDispatchTypes<StandalonePeerTypes>;
    type EncodeError = XactBatchCodecEncodeError<
        asn1rs::io::per::err::Error,
        XactSealedCodecError<
            asn1rs::io::per::err::Error,
            Infallible,
            XactReqCodecEncodeError<
                Infallible,
                Infallible,
                asn1rs::io::per::err::Error,
            >
        >,
        XactCommittedRoundCodecEncodeError<
            asn1rs::io::per::err::Error,
            Infallible,
            XactReqCodecEncodeError<
                Infallible,
                Infallible,
                asn1rs::io::per::err::Error,
            >
        >,
        XactNotifyCodecEncodeError<
            asn1rs::io::per::err::Error,
            Infallible,
            Infallible
        >
    >;
    type Encoder = XactBlobBatchCodec<ConsensusRoundID, SHA3Algo, TestSeal, TestSealCodec>;
    type EncoderConfig = ();
    type EncoderCreateError = XactBatchCodecCreateError<
        XactSealedCodecCreateError<
            Infallible,
            XactReqCodecCreateError<Infallible, Infallible>
        >,
        XactCommittedRoundCodecCreateError<
            Infallible,
            XactReqCodecCreateError<Infallible, Infallible>
        >,
        XactNotifyCodecCreateError<Infallible, Infallible>
    >;
    type Epoch = u128;
    type Epochs = AscendingCount<u128>;
    type EpochsConfig = ();
    type EpochsCreateError = Infallible;
    type Hash = SHA3Algo;
    type HashConfig = ();
    type HashCreateError = Infallible;
    type HashID = SHA3ID;
    type IDs = AscendingCount<LargeObjID>;
    type IDsConfig = ();
    type IDsCreateError = Infallible;
    type InMsg = XactBlobBatch<ConsensusRoundID, SHA3ID, TestSeal>;
    type LargeObjAuthNMsg = BasicAuthNed<String, LargeObjMsg<SHA3ID>>;
    type LargeObjMsgAuth = PassthruMsgAuthN<LargeObjMsg<SHA3ID>, String>;
    type LargeObjMsgAuthConfig = ();
    type LargeObjMsgAuthCreateError = Infallible;
    type LargeObjWrapper = LargeObjMsg<SHA3ID>;
    type MsgAuth =
        PassthruMsgAuthN<XactBlobBatch<ConsensusRoundID, SHA3ID, TestSeal>,
                         String>;
    type MsgAuthConfig = ();
    type MsgAuthCreateError = Infallible;
    type MsgAuthError = Infallible;
    type MsgPrin = String;
    type Msgs = ProcessorSessionMsgs<StandalonePeerTypes>;
    type Origin = CompoundFarEndpoint;
    type OutMsg = XactBlobBatch<ConsensusRoundID, SHA3ID, TestSeal>;
    type Recv = ProcessorSessionRecv<StandalonePeerTypes>;
    type RecvError = ProcessorSessionRecvError<String>;
    type Resolver =
        MixedResolver<CompoundFarChannelXfrmPeerAddr, CompoundFarEndpoint>;
    type ResolverConfig = ResolverConfig;
    type SessionAuth = BasicAuthN<String>;
    type SessionAuthConfig = BasicAuthNConfig<String>;
    type SessionAuthNError = Infallible;
    type SessionPrin = String;
    type UDP = PassthruDatagramXfrm<SocketAddr>;
    type UDPConfig = PassthruDatagramXfrmParam;
    type UDPError = Infallible;
    type Unix = PassthruDatagramXfrm<UnixSocketPath>;
    type UnixConfig = PassthruDatagramXfrmParam;
    type UnixError = Infallible;
    type Wrapper = XactBlobBatch<ConsensusRoundID, SHA3ID, TestSeal>;
}

#[cfg(feature = "standalone")]
impl MulticastLargeObjBusTypes<StandaloneCtx> for StandaloneConsensusBusTypes {
    type Hash = SHA3Algo;
    type HashConfig = ();
    type HashCreateError = Infallible;
    type HashID = SHA3ID;
    type IDs = AscendingCount<LargeObjID>;
    type IDsConfig = ();
    type IDsCreateError = Infallible;
    type AuthNChan = CompoundFarChannelsBasicAuthNedLargeObjChan<
        String,
        SHA3Algo,
        PassthruDatagramXfrm<UnixSocketPath>,
        PassthruDatagramXfrm<SocketAddr>
    >;
    type AuthNMsg = BasicAuthNed<String, ConsensusCtl<ConsensusRoundID, SHA3ID, TestSeal>>;
    type AuthNSession = BasicAuthNed<
        String,
        CompoundFlow<
            PassthruDatagramXfrm<UnixSocketPath>,
            PassthruDatagramXfrm<SocketAddr>
        >
    >;
    type Decoder = ConsensusCtlCodec<ConsensusRoundID, SHA3Algo, TestSeal, TestSealCodec>;
    type DecoderConfig = ();
    type DecoderCreateError = Infallible;
    type EncodeError =
        ConsensusCtlRoundEncodeError<asn1rs::io::per::err::Error, Infallible>;
    type Encoder = ConsensusCtlCodec<ConsensusRoundID, SHA3Algo, TestSeal, TestSealCodec>;
    type EncoderConfig = ();
    type EncoderCreateError = Infallible;
    type Epoch = u128;
    type Epochs = AscendingCount<u128>;
    type EpochsConfig = ();
    type InMsg = ConsensusCtl<ConsensusRoundID, SHA3ID, TestSeal>;
    type LargeObjAuthNMsg = BasicAuthNed<String, LargeObjMsg<SHA3ID>>;
    type LargeObjMsgAuth = PassthruMsgAuthN<LargeObjMsg<SHA3ID>, String>;
    type LargeObjMsgAuthConfig = ();
    type LargeObjWrapper = LargeObjMsg<SHA3ID>;
    type MsgAuth = PassthruMsgAuthN<ConsensusCtl<ConsensusRoundID, SHA3ID, TestSeal>, String>;
    type MsgAuthConfig = ();
    type MsgAuthCreateError = Infallible;
    type MsgAuthError = Infallible;
    type MsgPrin = String;
    type Msgs = ConsensusMsgs<StandalonePeerTypes>;
    type Origin = CompoundFarEndpoint;
    type OutMsg = ConsensusCtl<ConsensusRoundID, SHA3ID, TestSeal>;
    type Recv = ConsensusRecv<StandalonePeerTypes>;
    type Resolver =
        MixedResolver<CompoundFarChannelXfrmPeerAddr, CompoundFarEndpoint>;
    type ResolverConfig = ResolverConfig;
    type SessionAuth = BasicAuthN<String>;
    type SessionAuthConfig = BasicAuthNConfig<String>;
    type SessionAuthNError = Infallible;
    type SessionPrin = String;
    type UDP = PassthruDatagramXfrm<SocketAddr>;
    type UDPConfig = PassthruDatagramXfrmParam;
    type UDPError = Infallible;
    type Unix = PassthruDatagramXfrm<UnixSocketPath>;
    type UnixConfig = PassthruDatagramXfrmParam;
    type UnixError = Infallible;
    type Wrapper = ConsensusCtl<ConsensusRoundID, SHA3ID, TestSeal>;
}

#[cfg(feature = "standalone")]
impl PeerComponentTypes for StandalonePeerTypes {
    type Ctx = StandaloneCtx;
    type ClientsSessAuthConfig = BasicAuthNConfig<String>;
    type ClientsMsgAuthConfig = ();
    type ClientsLargeObjMsgAuthConfig = ();
    type ClientsHashConfig = ();
    type ClientsIDsConfig = ();
    type ClientsUDPConfig = PassthruDatagramXfrmParam;
    type ClientsUnixConfig = PassthruDatagramXfrmParam;
    type ClientsEpochsConfig = ();
    type ClientsBusTypes = StandaloneClientsBusTypes;
    type ProcessorsSessAuthConfig = BasicAuthNConfig<String>;
    type ProcessorsMsgAuthConfig = ();
    type ProcessorsLargeObjMsgAuthConfig = ();
    type ProcessorsHashConfig =();
    type ProcessorsIDsConfig = ();
    type ProcessorsUDPConfig = PassthruDatagramXfrmParam;
    type ProcessorsUnixConfig = PassthruDatagramXfrmParam;
    type ProcessorsEpochsConfig = ();
    type ProcessorsBusTypes = StandaloneProcessorsBusTypes;
    type ConsensusSessAuthConfig = BasicAuthNConfig<String>;
    type ConsensusMsgAuthConfig = ();
    type ConsensusMsgAuthCreateError = Infallible;
    type ConsensusLargeObjMsgAuthConfig = ();
    type ConsensusHashConfig = ();
    type ConsensusHashCreateError = Infallible;
    type ConsensusIDsConfig = ();
    type ConsensusUDPConfig = PassthruDatagramXfrmParam;
    type ConsensusUnixConfig = PassthruDatagramXfrmParam;
    type ConsensusEpochsConfig = ();
    type ConsensusResolverConfig = ResolverConfig;
    type ConsensusBusTypes = StandaloneConsensusBusTypes;
}
