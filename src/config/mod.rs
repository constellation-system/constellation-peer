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

use std::time::Duration;

use constellation_auth::config::TestAuthNConfig;
use constellation_auth::config::TestCredConfig;
use constellation_channels::config::ChannelRegistryConfig;
use constellation_channels::config::CompoundFarChannelConfig;
use constellation_channels::config::CompoundXfrmCreateParam;
use constellation_channels::config::ThreadedFlowsParams;
#[cfg(feature = "standalone")]
use constellation_channels::config::ThreadedNSNameCachesConfig;
use constellation_common::config::VersionRangeConfig;
#[cfg(feature = "standalone")]
use constellation_common::ids::AscendingCount;
#[cfg(feature = "standalone")]
use constellation_common::ids::IDGen;
use constellation_common::retry::Retry;
use constellation_common::version::VersionRange;
use constellation_component_common::config::DispatchLargeObjBusConfig;
use constellation_streams::config::LargeObjProtoConfig;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename = "peer-config")]
#[serde(rename_all = "kebab-case")]
pub struct PeerConfig<Prin, Channel, Flows, Epochs, LargeObj, AuthN, Xfrm>
where
    Epochs: Default,
    Flows: Default,
    LargeObj: Default,
    Xfrm: Default {
    /// Configuration for incoming client connections.
    clients: ClientsConfig<Channel, Flows, Epochs, LargeObj, Xfrm>,
    /// Configuration for processors.
    processors:
        ProcessorsConfig<Prin, Channel, Flows, Epochs, LargeObj, AuthN, Xfrm>,
    /// Configuration for the peer state.
    #[serde(default)]
    #[serde(flatten)]
    state: PeerStateConfig
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename = "peer-state-config")]
#[serde(rename_all = "kebab-case")]
#[serde(default)]
pub struct PeerStateConfig {
    #[serde(default = "PeerStateConfig::default_tombstone_duration")]
    tombstone_duration: Duration,
    #[serde(default)]
    transactions_size_hint: Option<usize>,
    #[serde(default)]
    seals_size_hint: Option<usize>,
    #[serde(default = "PeerStateConfig::default_retry")]
    retry: Retry
}

#[derive(
    Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename = "class-id")]
#[serde(untagged)]
pub enum ClassIDConfig {
    /// A UUID identifying the service.
    ///
    /// This is the method used to transmit on the wire.
    UUID { uuid: Uuid },
    /// A string name identifying the service.
    ///
    /// This will be used to produce a v5 UUID for transmission on the
    /// wire.
    Name { name: String }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename = "class")]
#[serde(rename_all = "kebab-case")]
pub struct ClassConfig {
    /// Name of the service.
    #[serde(flatten)]
    id: ClassIDConfig,
    /// Instances supported by this entry.
    #[serde(default = "ClassConfig::default_instances")]
    instances: Vec<usize>,
    /// Supported versions.
    versions: Vec<VersionRangeConfig>
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename = "processor")]
#[serde(rename_all = "kebab-case")]
pub struct ProcessorConfig<Prin> {
    /// Services handled by this processor.
    classes: Vec<ClassConfig>,
    /// Principal associated with this processor
    principal: Prin
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ProcessorClassesConfig<Prin> {
    Static {
        #[serde(rename = "static")]
        stat: Vec<ProcessorConfig<Prin>>
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename = "processors-config")]
#[serde(rename_all = "kebab-case")]
pub struct ProcessorsConfig<Prin, Channel, Flows, Epochs, LargeObj, AuthN, Xfrm>
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
    large_obj: LargeObj,
    /// Processors for transaction classes.
    #[serde(flatten)]
    processors: ProcessorClassesConfig<Prin>,
    authn: AuthN
}

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
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename = "peer-config")]
#[serde(rename_all = "kebab-case")]
pub struct StandaloneConfig {
    /// Name cache configuration.
    #[serde(default)]
    name_caches: ThreadedNSNameCachesConfig,
    /// Configuration for incoming client connections.
    #[serde(flatten)]
    peer: PeerConfig<
        String,
        CompoundFarChannelConfig,
        ThreadedFlowsParams,
        <AscendingCount<u128> as IDGen>::Config,
        LargeObjProtoConfig<(), ()>,
        TestAuthNConfig<String, TestCredConfig>,
        CompoundXfrmCreateParam<(), ()>
    >
}

impl Default for PeerStateConfig {
    #[inline]
    fn default() -> Self {
        PeerStateConfig {
            tombstone_duration: PeerStateConfig::default_tombstone_duration(),
            transactions_size_hint: None,
            seals_size_hint: None,
            retry: PeerStateConfig::default_retry()
        }
    }
}

impl PeerStateConfig {
    #[inline]
    pub fn new(
        tombstone_duration: Duration,
        transactions_size_hint: Option<usize>,
        seals_size_hint: Option<usize>,
        retry: Retry
    ) -> Self {
        PeerStateConfig {
            tombstone_duration: tombstone_duration,
            transactions_size_hint: transactions_size_hint,
            seals_size_hint: seals_size_hint,
            retry: retry
        }
    }

    #[inline]
    pub fn tombstone_duration(&self) -> Duration {
        self.tombstone_duration
    }

    #[inline]
    pub fn default_tombstone_duration() -> Duration {
        Duration::from_secs(30)
    }

    #[inline]
    pub fn default_retry() -> Retry {
        Retry::TERRESTRIAL_NETWORK_DEFAULT.clone()
    }

    #[inline]
    pub fn take(self) -> (Duration, Option<usize>, Option<usize>, Retry) {
        (
            self.tombstone_duration,
            self.transactions_size_hint,
            self.seals_size_hint,
            self.retry
        )
    }
}

impl From<ClassIDConfig> for Uuid {
    #[inline]
    fn from(val: ClassIDConfig) -> Uuid {
        match val {
            ClassIDConfig::UUID { uuid } => uuid,
            ClassIDConfig::Name { name } => {
                Uuid::new_v5(&Uuid::NAMESPACE_DNS, name.as_bytes())
            }
        }
    }
}

impl ClassConfig {
    pub fn new<I, J>(
        id: ClassIDConfig,
        instances: I,
        versions: J
    ) -> Self
    where
        I: Iterator<Item = usize>,
        J: Iterator<Item = VersionRange> {
        let versions = versions.map(VersionRangeConfig::from).collect();
        let instances = instances.collect();

        ClassConfig {
            versions: versions,
            instances: instances,
            id: id
        }
    }

    #[inline]
    pub fn id(&self) -> &ClassIDConfig {
        &self.id
    }

    #[inline]
    pub fn instances(&self) -> &[usize] {
        &self.instances
    }

    #[inline]
    pub fn versions(&self) -> &[VersionRangeConfig] {
        &self.versions
    }

    #[inline]
    pub fn take(self) -> (ClassIDConfig, Vec<usize>, Vec<VersionRangeConfig>) {
        (self.id, self.instances, self.versions)
    }

    fn default_instances() -> Vec<usize> {
        vec![0]
    }
}

impl<Prin> ProcessorConfig<Prin> {
    pub fn new(
        principal: Prin,
        classes: Vec<ClassConfig>
    ) -> Self {
        ProcessorConfig {
            classes: classes,
            principal: principal
        }
    }

    #[inline]
    pub fn principal(&self) -> &Prin {
        &self.principal
    }

    #[inline]
    pub fn classes(&self) -> &[ClassConfig] {
        &self.classes
    }

    #[inline]
    pub fn take(self) -> (Prin, Vec<ClassConfig>) {
        (self.principal, self.classes)
    }
}

impl<Prin, Channel, Flows, Epochs, LargeObj, AuthN, Xfrm>
    PeerConfig<Prin, Channel, Flows, Epochs, LargeObj, AuthN, Xfrm>
where
    Epochs: Default,
    Flows: Default,
    LargeObj: Default,
    Xfrm: Default
{
    #[inline]
    pub fn new(
        clients: ClientsConfig<Channel, Flows, Epochs, LargeObj, Xfrm>,
        processors: ProcessorsConfig<
            Prin,
            Channel,
            Flows,
            Epochs,
            LargeObj,
            AuthN,
            Xfrm
        >,
        state: PeerStateConfig
    ) -> Self {
        PeerConfig {
            processors: processors,
            clients: clients,
            state: state
        }
    }

    #[inline]
    pub fn clients(
        &self
    ) -> &ClientsConfig<Channel, Flows, Epochs, LargeObj, Xfrm> {
        &self.clients
    }

    #[inline]
    pub fn processors(
        &self
    ) -> &ProcessorsConfig<Prin, Channel, Flows, Epochs, LargeObj, AuthN, Xfrm>
    {
        &self.processors
    }

    #[inline]
    pub fn state(&self) -> &PeerStateConfig {
        &self.state
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        ClientsConfig<Channel, Flows, Epochs, LargeObj, Xfrm>,
        ProcessorsConfig<Prin, Channel, Flows, Epochs, LargeObj, AuthN, Xfrm>,
        PeerStateConfig
    ) {
        (self.clients, self.processors, self.state)
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

impl<Prin, Channel, Flows, Epochs, LargeObj, AuthN, Xfrm>
    ProcessorsConfig<Prin, Channel, Flows, Epochs, LargeObj, AuthN, Xfrm>
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
        large_obj: LargeObj,
        processors: ProcessorClassesConfig<Prin>,
        authn: AuthN
    ) -> Self {
        ProcessorsConfig {
            processors: processors,
            large_obj: large_obj,
            registry: registry,
            authn: authn,
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
    pub fn processors(&self) -> &ProcessorClassesConfig<Prin> {
        &self.processors
    }

    #[inline]
    pub fn authn(&self) -> &AuthN {
        &self.authn
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        ChannelRegistryConfig<Channel, Flows, Xfrm>,
        DispatchLargeObjBusConfig<Epochs>,
        LargeObj,
        ProcessorClassesConfig<Prin>,
        AuthN
    ) {
        (
            self.registry,
            self.bus,
            self.large_obj,
            self.processors,
            self.authn
        )
    }
}

#[cfg(feature = "standalone")]
impl StandaloneConfig {
    #[inline]
    pub fn new(
        name_caches: ThreadedNSNameCachesConfig,
        peer: PeerConfig<
            String,
            CompoundFarChannelConfig,
            ThreadedFlowsParams,
            <AscendingCount<u128> as IDGen>::Config,
            LargeObjProtoConfig<(), ()>,
            TestAuthNConfig<String, TestCredConfig>,
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
        String,
        CompoundFarChannelConfig,
        ThreadedFlowsParams,
        <AscendingCount<u128> as IDGen>::Config,
        LargeObjProtoConfig<(), ()>,
        TestAuthNConfig<String, TestCredConfig>,
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
            String,
            CompoundFarChannelConfig,
            ThreadedFlowsParams,
            <AscendingCount<u128> as IDGen>::Config,
            LargeObjProtoConfig<(), ()>,
            TestAuthNConfig<String, TestCredConfig>,
            CompoundXfrmCreateParam<(), ()>
        >
    ) {
        (self.name_caches, self.peer)
    }
}

#[cfg(test)]
use std::convert::TryFrom;

#[cfg(test)]
use uuid::uuid;

#[test]
fn test_class_id_uuid() {
    let yaml = concat!("uuid: 67e55044-10b1-426f-9247-bb680e5fe0c8");
    let expected = ClassIDConfig::UUID {
        uuid: uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8")
    };
    let actual = serde_yaml::from_str(yaml).unwrap();

    assert_eq!(expected, actual)
}

#[test]
fn test_class_id_name() {
    let yaml = concat!("name: org.constellation.test");
    let expected = ClassIDConfig::Name {
        name: String::from("org.constellation.test")
    };
    let actual = serde_yaml::from_str(yaml).unwrap();

    assert_eq!(expected, actual)
}

#[test]
fn test_class_config_no_instances() {
    let version =
        VersionRangeConfig::try_from("<=1").expect("Expected success");
    let yaml = concat!(
        "name: org.constellation.test\n",
        "versions:\n",
        "  - \"<=1\"\n"
    );
    let expected = ClassConfig {
        id: ClassIDConfig::Name {
            name: String::from("org.constellation.test")
        },
        instances: vec![0],
        versions: vec![version]
    };
    let actual = serde_yaml::from_str(yaml).unwrap();

    assert_eq!(expected, actual)
}

#[test]
fn test_class_config_instances() {
    let version =
        VersionRangeConfig::try_from("<=1").expect("Expected success");
    let yaml = concat!(
        "name: org.constellation.test\n",
        "instances:\n",
        "  - 0\n",
        "  - 1\n",
        "versions:\n",
        "  - \"<=1\"\n"
    );
    let expected = ClassConfig {
        id: ClassIDConfig::Name {
            name: String::from("org.constellation.test")
        },
        instances: vec![0, 1],
        versions: vec![version]
    };
    let actual = serde_yaml::from_str(yaml).unwrap();

    assert_eq!(expected, actual)
}

#[test]
fn test_processor_config() {
    let version =
        VersionRangeConfig::try_from("<=1").expect("Expected success");
    let yaml = concat!(
        "principal: test-principal\n",
        "classes:\n",
        "  - name: org.constellation.test\n",
        "    instances:\n",
        "      - 0\n",
        "      - 1\n",
        "    versions:\n",
        "      - \"<=1\"\n"
    );
    let expected = ProcessorConfig {
        principal: "test-principal",
        classes: vec![ClassConfig {
            id: ClassIDConfig::Name {
                name: String::from("org.constellation.test")
            },
            instances: vec![0, 1],
            versions: vec![version]
        }]
    };
    let actual = serde_yaml::from_str(yaml).unwrap();

    assert_eq!(expected, actual)
}

#[test]
fn test_static_processor_configs() {
    let version = VersionRangeConfig::try_from("*").expect("Expected success");
    let yaml = concat!(
        "static:\n",
        "  - principal: test-processor\n",
        "    classes:\n",
        "      - name: org.constellation.test\n",
        "        versions:\n",
        "          - \"*\"\n"
    );
    let expected = ProcessorClassesConfig::Static {
        stat: vec![ProcessorConfig {
            principal: "test-processor",
            classes: vec![ClassConfig {
                id: ClassIDConfig::Name {
                    name: String::from("org.constellation.test")
                },
                instances: vec![0],
                versions: vec![version]
            }]
        }]
    };
    let actual = serde_yaml::from_str(yaml).unwrap();

    assert_eq!(expected, actual)
}
