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

use constellation_channels::config::ChannelRegistryConfig;
use constellation_channels::config::CompoundFarChannelConfig;
use constellation_channels::config::CompoundXfrmCreateParam;
use constellation_channels::config::ThreadedFlowsParams;
#[cfg(feature = "standalone")]
use constellation_channels::config::ThreadedNSNameCachesConfig;
#[cfg(feature = "standalone")]
use constellation_common::ids::AscendingCount;
#[cfg(feature = "standalone")]
use constellation_common::ids::IDGen;
use constellation_component_common::config::DispatchLargeObjBusConfig;
use constellation_streams::config::LargeObjProtoConfig;
use serde::Deserialize;
use serde::Serialize;

#[cfg(feature = "standalone")]
#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "clients")]
#[serde(rename_all = "kebab-case")]
pub struct PeerConfig<Channel, Flows, Epochs, LargeObj, Xfrm>
where
    Epochs: Default,
    Flows: Default,
    LargeObj: Default,
    Xfrm: Default {
    /// Configuration for incoming client connections.
    clients: ClientsConfig<Channel, Flows, Epochs, LargeObj, Xfrm>
}

#[cfg(feature = "standalone")]
#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "clients")]
#[serde(rename_all = "kebab-case")]
pub struct ClientsConfig<Channel, Flows, Epochs, LargeObj, Xfrm>
where
    Epochs: Default,
    Flows: Default,
    LargeObj: Default,
    Xfrm: Default {
    /// Channel registry configuration.
    #[serde(flatten)]
    registry: ChannelRegistryConfig<Channel, Flows, Xfrm>,
    /// Configuration for the dispatch comm subsystem.
    #[serde(default)]
    #[serde(flatten)]
    bus: DispatchLargeObjBusConfig<Epochs>,
    #[serde(default)]
    large_obj: LargeObj
}

pub type RegistryConfig = ChannelRegistryConfig<
    CompoundFarChannelConfig,
    ThreadedFlowsParams,
    CompoundXfrmCreateParam<(), ()>
>;

#[cfg(feature = "standalone")]
#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "peer-config")]
#[serde(rename_all = "kebab-case")]
pub struct StandaloneConfig {
    /// Name cache configuration.
    #[serde(default)]
    name_caches: ThreadedNSNameCachesConfig,
    /// Configuration for incoming client connections.
    #[serde(flatten)]
    peer: PeerConfig<
        CompoundFarChannelConfig,
        ThreadedFlowsParams,
        <AscendingCount<u128> as IDGen>::Config,
        LargeObjProtoConfig<(), ()>,
        CompoundXfrmCreateParam<(), ()>,
    >
}

impl<Channel, Flows, Epochs, LargeObj, Xfrm>
    PeerConfig<Channel, Flows, Epochs, LargeObj, Xfrm>
where
    Epochs: Default,
    Flows: Default,
    LargeObj: Default,
    Xfrm: Default
{
    #[inline]
    pub fn new(
        clients: ClientsConfig<Channel, Flows, Epochs, LargeObj, Xfrm>
    ) -> Self {
        PeerConfig { clients: clients }
    }

    #[inline]
    pub fn clients(
        &self
    ) -> &ClientsConfig<Channel, Flows, Epochs, LargeObj, Xfrm> {
        &self.clients
    }

    #[inline]
    pub fn take(
        self
    ) -> ClientsConfig<Channel, Flows, Epochs, LargeObj, Xfrm> {
        self.clients
    }
}

impl<Channel, Flows, Epochs, LargeObj, Xfrm>
    ClientsConfig<Channel, Flows, Epochs, LargeObj, Xfrm>
where
    Epochs: Default,
    Flows: Default,
    LargeObj: Default,
    Xfrm: Default
{
    #[inline]
    pub fn new(
        registry: ChannelRegistryConfig<Channel, Flows, Xfrm>,
        bus: DispatchLargeObjBusConfig<Epochs>,
        large_obj: LargeObj
    ) -> Self {
        ClientsConfig {
            large_obj: large_obj,
            registry: registry,
            bus: bus
        }
    }

    #[inline]
    pub fn registry(&self) -> &ChannelRegistryConfig<Channel, Flows, Xfrm> {
        &self.registry
    }

    #[inline]
    pub fn bus(&self) -> &DispatchLargeObjBusConfig<Epochs> {
        &self.bus
    }

    #[inline]
    pub fn large_obj(&self) -> &LargeObj {
        &self.large_obj
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        ChannelRegistryConfig<Channel, Flows, Xfrm>,
        DispatchLargeObjBusConfig<Epochs>,
        LargeObj
    ) {
        (self.registry, self.bus, self.large_obj)
    }
}

#[cfg(feature = "standalone")]
impl StandaloneConfig {
    #[inline]
    pub fn new(
        name_caches: ThreadedNSNameCachesConfig,
        peer: PeerConfig<
            CompoundFarChannelConfig,
            ThreadedFlowsParams,
            <AscendingCount<u128> as IDGen>::Config,
            LargeObjProtoConfig<(), ()>,
            CompoundXfrmCreateParam<(), ()>
        >
    ) -> Self {
        StandaloneConfig {
            name_caches: name_caches,
            peer: peer
        }
    }

    #[inline]
    pub fn name_caches(&self) -> &ThreadedNSNameCachesConfig {
        &self.name_caches
    }

    #[inline]
    pub fn peer(
        &self
    ) -> &PeerConfig<
        CompoundFarChannelConfig,
        ThreadedFlowsParams,
        <AscendingCount<u128> as IDGen>::Config,
        LargeObjProtoConfig<(), ()>,
        CompoundXfrmCreateParam<(), ()>,
    > {
        &self.peer
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        ThreadedNSNameCachesConfig,
        PeerConfig<
            CompoundFarChannelConfig,
            ThreadedFlowsParams,
            <AscendingCount<u128> as IDGen>::Config,
            LargeObjProtoConfig<(), ()>,
            CompoundXfrmCreateParam<(), ()>,
        >
    ) {
        (self.name_caches, self.peer)
    }
}
