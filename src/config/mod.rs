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

use constellation_channels::config::ThreadedNSNameCachesConfig;
use serde::Deserialize;
use serde::Serialize;

#[cfg(feature = "standalone")]
#[derive(Clone, Debug, Default, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "peer-config")]
#[serde(rename_all = "kebab-case")]
pub struct StandaloneConfig {
    /// Name cache configuration.
    #[serde(default)]
    name_caches: ThreadedNSNameCachesConfig,
}

#[cfg(feature = "standalone")]
impl StandaloneConfig {
    pub fn take(self) -> ThreadedNSNameCachesConfig {
        self.name_caches
    }
}
