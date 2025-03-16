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
use constellation_component_common::config::DispatchCommConfig;
use serde::Deserialize;
use serde::Serialize;

#[cfg(feature = "standalone")]
#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "clients")]
#[serde(rename_all = "kebab-case")]
pub struct PeerConfig<Channel, Flows, Epochs, Xfrm>
where
    Epochs: Default,
    Flows: Default,
    Xfrm: Default {
    /// Configuration for incoming client connections.
    clients: ClientsConfig<Channel, Flows, Epochs, Xfrm>
}

#[cfg(feature = "standalone")]
#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "clients")]
#[serde(rename_all = "kebab-case")]
pub struct ClientsConfig<Channel, Flows, Epochs, Xfrm>
where
    Epochs: Default,
    Flows: Default,
    Xfrm: Default {
    /// Channel registry configuration.
    #[serde(flatten)]
    registry: ChannelRegistryConfig<Channel, Flows, Xfrm>,
    /// Configuration for the dispatch comm subsystem.
    #[serde(default)]
    #[serde(flatten)]
    comm: DispatchCommConfig<Epochs>
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
        <AscendingCount as IDGen>::Config,
        CompoundXfrmCreateParam<(), ()>
    >
}

impl<Channel, Flows, Epochs, Xfrm> PeerConfig<Channel, Flows, Epochs, Xfrm>
where
    Epochs: Default,
    Flows: Default,
    Xfrm: Default
{
    #[inline]
    pub fn new(clients: ClientsConfig<Channel, Flows, Epochs, Xfrm>) -> Self {
        PeerConfig { clients: clients }
    }

    #[inline]
    pub fn clients(&self) -> &ClientsConfig<Channel, Flows, Epochs, Xfrm> {
        &self.clients
    }

    #[inline]
    pub fn take(self) -> ClientsConfig<Channel, Flows, Epochs, Xfrm> {
        self.clients
    }
}

impl<Channel, Flows, Epochs, Xfrm> ClientsConfig<Channel, Flows, Epochs, Xfrm>
where
    Epochs: Default,
    Flows: Default,
    Xfrm: Default
{
    #[inline]
    pub fn new(
        registry: ChannelRegistryConfig<Channel, Flows, Xfrm>,
        comm: DispatchCommConfig<Epochs>
    ) -> Self {
        ClientsConfig {
            registry: registry,
            comm: comm
        }
    }

    #[inline]
    pub fn registry(&self) -> &ChannelRegistryConfig<Channel, Flows, Xfrm> {
        &self.registry
    }

    #[inline]
    pub fn comm(&self) -> &DispatchCommConfig<Epochs> {
        &self.comm
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        ChannelRegistryConfig<Channel, Flows, Xfrm>,
        DispatchCommConfig<Epochs>
    ) {
        (self.registry, self.comm)
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
            <AscendingCount as IDGen>::Config,
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
        <AscendingCount as IDGen>::Config,
        CompoundXfrmCreateParam<(), ()>
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
            <AscendingCount as IDGen>::Config,
            CompoundXfrmCreateParam<(), ()>
        >
    ) {
        (self.name_caches, self.peer)
    }
}
