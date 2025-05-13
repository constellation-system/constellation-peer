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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use bitvec::bitvec;
use bitvec::vec::BitVec;
use constellation_common::error::MutexPoison;
use constellation_common::hashid::HashID;
use constellation_common::retry::Retry;
use constellation_common::version::Version;
use constellation_common::version::VersionRange;
use constellation_component_common::consensus_ctl::ConsensusCtlSubmit;
use constellation_component_common::xact::XactCommittedEffects;
use constellation_component_common::xact::XactCommittedRound;
use constellation_component_common::xact::XactCommittedReq;
use constellation_component_common::xact::XactConsensusSeal;
use constellation_component_common::xact::XactEffects;
use constellation_component_common::xact::XactError;
use constellation_component_common::xact::XactLinPoint;
use constellation_component_common::xact::XactNotify;
use constellation_component_common::xact::XactNotifyState;
use constellation_component_common::xact::XactSealed;
use constellation_component_common::xact::XactUncommittedHashReq;
use log::debug;
use log::error;
use log::trace;
use log::warn;
use uuid::Uuid;

use crate::config::PeerStateConfig;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProcessorIdx(usize);

/// Stage of a pending transaction.
enum PendingXactStage {
    /// Pre-commit phase.
    PreCommit {
        // XXX this will need retry information for sending to processors.

        /// Whether or not the transaction has been dispatched to a
        /// processor.
        dispatched: bool,
    },
    /// Submitted to consensus.
    Commit {
        submitted: bool,
    },
    /// Post-commit, submitted for processing.
    Process {
        dispatched: bool,
        /// Linearization point of the transaction.
        lin_point: XactLinPoint<u128>,
    }
}

/// Type of reporting to be done.
enum PeerReportKind {
    /// Full result reporting.
    Result,
    /// Notify-only reporting.
    Notify
}

/// Reporting entry for transactions.
struct PeerReport {
    /// Kind of reporting to be done.
    kind: PeerReportKind,
    /// When the next message should be sent.
    when: Option<Instant>,
    /// Number of retries.
    nretries: usize
}

enum PeerXactState {
    /// Submitted to a processor pre-commit.
    Pending {
        /// Processor to use.
        processor: ProcessorIdx,
        /// Current stage of the transaction.
        stage: PendingXactStage,
        /// Class of transactions.
        class: Uuid,
        /// Version of the transaction class.
        version: Version,
        /// Instance of the transaction class, if applicable.
        instance: u64,
        /// Transaction effects.
        effects: XactEffects<u128, Vec<u8>>,
        /// Transaction payload.
        payload: Vec<u8>,
    },
    /// Request is complete.
    Complete {
        /// Result from the transaction.
        res: Vec<u8>,
        /// Linearization point of the transaction.
        lin_point: XactLinPoint<u128>,
        /// When to expire the entry.
        expire: Instant
    },
    /// Request had an error.
    Error {
        /// Error from the transaction.
        error: XactError<Vec<u8>>,
        /// When to expire the entry.
        expire: Instant
    }
}

struct PeerXact<Prin>
where
    Prin: Clone + Display + Eq + Hash + Send + Sync {
    /// Reporting information.
    reporting: HashMap<Prin, PeerReport>,
    /// State of the transaction.
    state: PeerXactState,
}

#[derive(Clone)]
pub struct InstanceEntry {
    /// Local processor ID.
    processor: ProcessorIdx,
    /// Supported versions.
    versions: Vec<VersionRange>,
}

pub struct ProcessorEntry {
    /// Local processor IDs for all specific instances.
    instances: HashMap<u64, InstanceEntry>
}

#[derive(Clone)]
struct ConsensusSealRetry {
    nretries: usize,
    when: Instant
}

struct ConsensusSealEntry<H, Seal>
where
    H: Clone + Display + Eq + Hash + HashID {
    /// Consensus seal data.
    seal: XactConsensusSeal<H, Seal>,
    /// Which transactions have been completed.
    completed: BitVec,
    /// Which transactions are presently known to the peer.
    known: BitVec,
    /// Retry information, indexed by [ProcessorIdx].
    retries: HashMap<ProcessorIdx, ConsensusSealRetry>
}

pub(crate) struct PeerState<H, Prin, Seal>
where
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    H: Clone + Display + Eq + Hash + HashID {
    /// Current state of transactions.
    xacts: Mutex<HashMap<H, PeerXact<Prin>>>,
    /// Map from service information to local processor IDs.
    processors: HashMap<Uuid, ProcessorEntry>,
    /// Period at which tombstones should be expired.
    tombstone_duration: Duration,
    /// Pending consensus seals.
    seals: Mutex<HashMap<u128, ConsensusSealEntry<H, Seal>>>,
    /// Record of missing transactions in rounds.
    missing: Mutex<HashMap<H, u128>>,
    /// Retry configuration.
    retry: Retry
}

/// Actions to be done for [get_client_msg](PeerXact::get_client_msg).
enum DeleteAction {
    /// Retain everything.
    Retain,
    /// Drop the subscription, but retain the entry.
    Unsubscribe,
    /// Expire the entry.
    Expire
}

/// Used to indicate what to do when recording a result or error.
enum ResultState {
    /// The result can be reported.
    Ok,
    /// The message is just ignored.
    Ignore,
    /// Something went wrong, and an internal error should be reported.
    Error
}

impl InstanceEntry {
    #[inline]
    pub fn new<I>(
        processor: ProcessorIdx,
        versions: I,
    ) -> Self
    where I: Iterator<Item = VersionRange> {
        InstanceEntry {
            processor: processor,
            versions: versions.collect(),
        }
    }
}

impl ProcessorEntry {
    #[inline]
    pub fn new(
        instances: HashMap<u64, InstanceEntry>
    ) -> Self {
        ProcessorEntry {
            instances: instances
        }
    }
}

impl From<usize> for ProcessorIdx {
    #[inline]
    fn from(val: usize) -> Self {
        ProcessorIdx(val)
    }
}

impl From<ProcessorIdx> for usize {
    #[inline]
    fn from(val: ProcessorIdx) -> Self {
        val.0
    }
}

impl<Prin> PeerXact<Prin>
where
    Prin: Clone + Display + Eq + Hash + Send + Sync {
    /// Reset the reporting times.
    fn reset_reporting(&mut self) {
        let now = Instant::now();

        for ent in self.reporting.values_mut() {
            ent.when = Some(now);
            ent.nretries = 0;
        }
    }

    /// Generate a notify message.
    ///
    /// Returns whether or not to expire this entry.
    fn get_notify_msg<H>(
        &self,
        hash: &H,
        now: Instant,
        full: bool,
        notifies: &mut Vec<XactNotify<u128, H, Vec<u8>, Vec<u8>>>
    ) -> bool
    where
        H: Clone + Display + HashID {
        debug!(target: "peer-state",
               "generating notify message for {}",
               hash);

        let (state, delete) = match &self.state {
            PeerXactState::Pending { stage, effects, .. } => match stage {
                PendingXactStage::PreCommit { dispatched: false, .. } =>
                    (XactNotifyState::Accept, false),
                PendingXactStage::PreCommit {
                    dispatched: true,
                    ..
                } => match effects {
                    XactEffects::HardNone { when } =>
                        (XactNotifyState::PrecommitDispatch {
                            when: when.clone(),
                        }, false),
                    _ => (XactNotifyState::PrecommitDispatch {
                        when: None,
                    }, false)
                }
                PendingXactStage::Commit { submitted: false }  =>
                    (XactNotifyState::Accept, false),
                PendingXactStage::Commit { submitted: true }  =>
                    (XactNotifyState::Consensus, false),
                PendingXactStage::Process { dispatched: false, lin_point } =>
                    (XactNotifyState::Commit { when: lin_point.clone() },
                     false),
                PendingXactStage::Process { dispatched: true, lin_point } =>
                    (XactNotifyState::Dispatch { when: lin_point.clone() },
                     false),
            }
            PeerXactState::Complete { expire, lin_point, res } => if full {
                (XactNotifyState::Success {
                    result: Some(res.clone()),
                    when: lin_point.clone()
                }, *expire <= now)
            } else {
                (XactNotifyState::Success {
                    when: lin_point.clone(),
                    result: None,
                }, *expire <= now)
            },
            PeerXactState::Error { expire, error } => if full {
                (XactNotifyState::Error {
                    error: Some(error.clone()),
                }, *expire <= now)
            } else {
                (XactNotifyState::Error {
                    error: None,
                }, *expire <= now)
            }
        };

        notifies.push(XactNotify::new(hash.clone(), state));

        delete
    }

    /// Generate messages for a specific client.
    ///
    /// This returns whether or not to delete the entry.
    fn get_client_msg<H>(
        &mut self,
        hash: &H,
        now: Instant,
        client: &Prin,
        notifies: &mut Vec<XactNotify<u128, H, Vec<u8>, Vec<u8>>>
    ) -> (DeleteAction, Option<Instant>)
    where
        H: Clone + Display + HashID {
        if let Some(report) = self.reporting.get_mut(client) {
            if let Some(when) = report.when {
                // XXX manage retry state correctly.  We don't yet
                // have acknowledgements worked out here.
                report.when = None;

                let next = report.when;
                let expire = if when <= now {
                    match report.kind {
                        PeerReportKind::Result => self
                            .get_notify_msg(hash, now, true, notifies),
                        PeerReportKind::Notify => self
                            .get_notify_msg(hash, now, false, notifies),
                    }
                } else {
                    false
                };
                let action = if expire {
                    DeleteAction::Expire
                } else {
                    DeleteAction::Retain
                };

                (action, next)
            } else {
                (DeleteAction::Retain, None)
            }
        } else {
            warn!(target: "peer-state",
                  "principal {} not present in reporting for {}",
                  client, hash);

            (DeleteAction::Unsubscribe, None)
        }
    }

}

impl<H, Prin, Seal> PeerState<H, Prin, Seal>
where
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    H: Clone + Display + Eq + Hash + HashID {
    #[inline]
    pub(crate) fn new(
        config: PeerStateConfig,
        processors: HashMap<Uuid, ProcessorEntry>
    ) -> Self {
        let (tombstone_duration, xacts_size_hint, seals_size_hint, retry) =
            config.take();
        let (xacts, missing) = match xacts_size_hint {
            Some(size) => (HashMap::with_capacity(size),
                           HashMap::with_capacity(size)),
            None => (HashMap::new(), HashMap::new())
        };
        let seals = match seals_size_hint {
            Some(size) => HashMap::with_capacity(size),
            None => HashMap::new()
        };
        let processors = processors;
        let missing = Mutex::new(missing);
        let xacts = Mutex::new(xacts);
        let seals = Mutex::new(seals);

        PeerState {
            tombstone_duration: tombstone_duration,
            processors: processors,
            missing: missing,
            seals: seals,
            xacts: xacts,
            retry: retry
        }
    }

    pub(crate) fn get_consensus_msgs(
        &self,
    ) -> Result<
        (Vec<ConsensusCtlSubmit<H>>, Option<Instant>),
        MutexPoison
    > {
        trace!(target: "peer-state",
               "checking for commit-stage transactions");

        let mut xacts = self.xacts.lock().map_err(|_| MutexPoison)?;
        let msgs = Vec::with_capacity(xacts.len());

        for (hash, ent) in xacts.iter_mut() {
            match &mut ent.state {
                PeerXactState::Pending {
                    stage: PendingXactStage::Commit { submitted },
                    ..
                } => {
                    // XXX do retry information here.

                    trace!(target: "peer-state",
                           "submitting transaction {} to consensus",
                           hash);

                    *submitted = true
                }
                _ => {
                    trace!(target: "peer-state",
                           "checking for transaction {}",
                           hash);
                }
            }
        }

        Ok((msgs, None))
    }

    pub(crate) fn get_processor_msgs(
        &self,
        target: ProcessorIdx
    ) -> Result<
        (Vec<XactCommittedRound<u128, H, Seal, Vec<u8>, Vec<u8>>>,
         Vec<XactSealed<(), XactUncommittedHashReq<u128, H, Vec<u8>, Vec<u8>>>>,
         Option<Instant>),
        MutexPoison
    > {
        // Scan pre-commit requests.
        let mut xacts = self.xacts.lock().map_err(|_| MutexPoison)?;
        let mut reqs = Vec::with_capacity(xacts.len());

        trace!(target: "peer-state",
               "checking for pre-commit transactions for {}",
               target);

        for (hash, ent) in xacts.iter_mut() {
            match &mut ent.state {
                PeerXactState::Pending {
                    stage: PendingXactStage::PreCommit { dispatched, .. },
                    processor,
                    class,
                    version,
                    instance,
                    effects,
                    payload,
                    ..
                } if processor == &target => {
                    // XXX do retry information here.

                    debug!(target: "peer-state",
                           "submitting pre-commit transaction {} to {}",
                           hash, target);

                    let req = XactUncommittedHashReq::new(
                        hash.clone(),
                        class.clone(),
                        version.clone(),
                        *instance,
                        payload.clone(),
                        effects.clone()
                    );

                    reqs.push(XactSealed::new((), req));
                    *dispatched = true
                }
                _ => {
                    trace!(target: "peer-state",
                           "checking for transaction {}",
                           hash);
                }
            }
        }

        // Scan consensus seals.
        let mut seals = self.seals.lock().map_err(|_| MutexPoison)?;
        let mut rounds = Vec::with_capacity(seals.len());

        for (round, seal) in seals.iter_mut() {
            if let Some(retry) = seal.retries.get_mut(&target) {
                // XXX do retry here

                let mask = !seal.completed.clone() & &seal.known;

                // See if there are actually any transactions known to
                // us to submit.
                if mask.any() {
                    let hashes = &seal.seal.hashes();
                    // There are transactions we can submit.
                    let mut reqs = Vec::with_capacity(mask.count_ones());

                    for i in mask.iter_ones() {
                        let hash = &hashes[i];

                        if let Some(xact) = xacts.get_mut(hash) {
                            match &mut xact.state {
                                // This is what we expect
                                PeerXactState::Pending {
                                    stage: PendingXactStage::Process {
                                        dispatched,
                                        ..
                                    },
                                    processor,
                                    class,
                                    version,
                                    instance,
                                    payload,
                                    effects,
                                    ..
                                } => if processor == &target {
                                    trace!(target: "peer-state",
                                           "adding transaction {}",
                                           hash);
                                    let effects = match effects {
                                        XactEffects::Effects {
                                            effects,
                                            hard
                                        } => Some(XactCommittedEffects::new(
                                            *hard,
                                            effects.clone()
                                        )),
                                        XactEffects::SoftNone => None,
                                        // This shouldn't happen, but
                                        // it's not fatal.
                                        XactEffects::HardNone { .. } => {
                                            error!(target: "peer-state",
                                                   concat!("transaction {} ",
                                                           "in process stage ",
                                                           "is hard no-effect"),
                                                   hash);
                                            None
                                        }
                                    };

                                    reqs.push(XactCommittedReq::new(
                                        class.clone(),
                                        version.clone(),
                                        *instance,
                                        i,
                                        payload.clone(),
                                        effects
                                    ));

                                    *dispatched = true;
                                } else {
                                    trace!(target: "peer-state",
                                           concat!("skipping transaction {} ",
                                                   "targeted at {}"),
                                           hash, processor);
                                }
                                // Transaction is in the wrong stage.
                                PeerXactState::Pending { .. } => {
                                    error!(target: "peer-state",
                                           concat!("transaction {} isn't in ",
                                                   "process stage"),
                                           hash);
                                },
                                // Transaction is in the wrong state.
                                _ => {
                                    error!(target: "peer-state",
                                           concat!("transaction {} isn't in ",
                                                   "pending state"),
                                           hash);
                                }
                            }

                        } else {
                            // This shouldn't happen.
                            error!(target: "peer-state",
                                   "unknown transaction {} in for round {}",
                                   hash, round);
                        }
                    }

                    debug!(target: "peer-state",
                           "submitting round {} to {}",
                           round, target);

                    rounds.push(XactCommittedRound::new(
                        *round,
                        None,
                        reqs
                    ));
                } else {
                    // There are transactions left, but we don't have them yet.

                    trace!(target: "peer-state",
                           "no transactions available for seal for round {}",
                           round);
                }
            }
        }

        Ok((rounds, reqs, None))
    }

    /// Collect outbound messages for clients.
    pub(crate) fn get_client_msgs(
        &self,
        hashes: &mut HashSet<H>,
        client: &Prin,
    ) -> Result<(Vec<XactNotify<u128, H, Vec<u8>, Vec<u8>>>,
                 Option<Instant>),
                MutexPoison> {
        let mut notifies = Vec::with_capacity(hashes.len());
        // XXX it's fairly inefficient to generate these vectors.
        let mut deletes = Vec::with_capacity(hashes.len());
        let mut min: Option<Instant> = None;
        let now = Instant::now();

        trace!(target: "peer-state",
               "generating client messages");

        // Go through the subscription hashes and check those
        // transactions.
        for hash in hashes.iter() {
            trace!(target: "peer-state",
                   "checking for transaction {}",
                   hash);

            if let Some(xact) = self.xacts.lock().map_err(|_| MutexPoison)?
                .get_mut(hash) {
                trace!(target: "peer-state",
                       "generating message for transaction {}",
                       hash);

                let (action, next) = xact
                    .get_client_msg(hash, now, client, &mut notifies);

                match action {
                    DeleteAction::Unsubscribe => {
                        trace!(target: "peer-state",
                               "marking transaction {} for deletion",
                               hash);

                        deletes.push((hash.clone(), false))
                    }
                    DeleteAction::Expire => {
                        trace!(target: "peer-state",
                               "marking transaction {} for deletion",
                               hash);

                        deletes.push((hash.clone(), true))
                    },
                    _ => {}
                }

                min = match (min, next) {
                    (Some(min), Some(next)) => Some(min.min(next)),
                    (Some(min), _) => Some(min),
                    (_, Some(next)) => Some(next),
                    _ => None
                }
            } else {
                trace!(target: "peer-state",
                       "marking transaction {} for deletion",
                       hash);

                deletes.push((hash.clone(), false))
            }
        }

        // Delete everything we found needed to be deleted.
        for (hash, delete) in deletes {
            if delete {
                let mut guard = self.xacts.lock().map_err(|_| MutexPoison)?;
                let _ = guard.remove(&hash);

                debug!(target: "peer-state",
                       "expired transaction {}",
                       hash);
            }

            hashes.remove(&hash);

            debug!(target: "peer-state",
                   "unsubscribed {} from transaction {}",
                   client, hash);
        }

        Ok((notifies, min))
    }

    fn get_processor(
        &self,
        class: &Uuid,
        version: &Version,
        instance: u64,
    ) -> Result<ProcessorIdx, XactError<Vec<u8>>> {
        match self.processors.get(class) {
            Some(ent) => match ent.instances.get(&instance) {
                Some(ent) => if ent.versions.iter()
                    .any(|range| range.contains(version)) {
                    Ok(ent.processor.clone())
                } else {
                    Err(XactError::UnknownVersion)
                },
                None => {
                    return Err(XactError::UnknownInstance)
                }
            }
            None => Err(XactError::UnknownClass)
        }
    }

    pub(crate) fn add_xact(
        &self,
        client: &Prin,
        class: Uuid,
        version: Version,
        instance: u64,
        hash: H,
        effects: XactEffects<u128, Vec<u8>>,
        payload: Vec<u8>
    ) -> Result<(), MutexPoison> {
        let report = PeerReport {
            kind: PeerReportKind::Result,
            when: Some(Instant::now()),
            nretries: 0
        };

        // Try looking up the processor.
        match self.get_processor(&class, &version, instance) {
            Ok(idx) => match self.xacts.lock().map_err(|_| MutexPoison)?
                .entry(hash.clone()) {
                Entry::Occupied(mut ent) => {
                    // The entry already exists; subscribe this client to it.
                    ent.get_mut().reporting.insert(client.clone(), report);
                }
                Entry::Vacant(ent) => if let Some(round) = self
                    .missing
                    .lock()
                    .map_err(|_| MutexPoison)?
                    .remove(&hash) {
                    // The transaction is already in a committed round.

                    trace!(target: "peer-state",
                           "transaction {} is in missing set",
                           hash);

                    // Try looking up the round of the missing entry.
                    if let Some(seal_ent) = self.seals.lock()
                        .map_err(|_| MutexPoison)?
                        .get_mut(&round) {
                        // Now find the hash's index within the seal.
                        if let Some(idx) = seal_ent
                            .seal
                            .hashes()
                            .iter()
                            .position(|actual| &hash == actual) {
                            // Add the entry and set the known flag.
                            let stage = PendingXactStage::Process {
                                lin_point: XactLinPoint::new(round, idx),
                                dispatched: false
                            };
                            let state = PeerXactState::Pending {
                                processor: ProcessorIdx::from(idx),
                                stage: stage,
                                class: class,
                                version: version,
                                instance: instance,
                                effects: effects,
                                payload: payload
                            };
                            let mut reporting = HashMap::new();

                            debug!(target: "peer-state",
                                   "creating transaction {} in process stage",
                                   hash);

                            reporting.insert(client.clone(), report);
                            ent.insert(PeerXact {
                                reporting: reporting,
                                state: state
                            });

                            debug!(target: "peer-state",
                                   "setting missing transaction {} to known",
                                   hash);

                            seal_ent.known.set(idx, true);
                        } else {
                            // This should never happen.
                            error!(target: "peer-state",
                                   "transaction {} is in not in round {}",
                                   hash, round);
                        }
                    } else {
                        // This should never happen.
                        error!(target: "peer-state",
                               "seal for round {} not found",
                               round);
                    }
                } else {
                    // The round is unknown to us.
                    let stage = match &effects {
                        // If there are effects, we go straight to
                        // consensus.
                        XactEffects::Effects { .. } =>
                            PendingXactStage::Commit {
                                submitted: false
                            },
                        // Otherwise, we try to process the transaction
                        // without a commit.
                        XactEffects::HardNone { .. } |
                        XactEffects::SoftNone =>
                            PendingXactStage::PreCommit {
                                dispatched: false
                            }
                    };
                    let state = PeerXactState::Pending {
                        processor: idx,
                        stage: stage,
                        class: class,
                        version: version,
                        instance: instance,
                        effects: effects,
                        payload: payload
                    };
                    let mut reporting = HashMap::new();

                    reporting.insert(client.clone(), report);
                    ent.insert(PeerXact {
                        reporting: reporting,
                        state: state
                    });
                }
            }
            Err(err) => match self.xacts.lock().map_err(|_| MutexPoison)?
                .entry(hash.clone()) {
                // Only this case should ever happen.
                Entry::Vacant(ent) => {
                    let expire = Instant::now() +
                        self.tombstone_duration;
                    let state = PeerXactState::Error {
                        expire: expire,
                        error: err
                    };
                    let mut reporting = HashMap::new();

                    reporting.insert(client.clone(), report);
                    ent.insert(PeerXact {
                        reporting: reporting,
                    state: state
                    });
                }
                // Something is very, very wrong if we get here.
                Entry::Occupied(_) => {
                    error!(target: "peer-state",
                           concat!("processor not found for class {}, ",
                                   "version {}, instance {}, ",
                                   "but transaction {} exists"),
                           class, version, instance, hash);
                }
            }
        }

        Ok(())
    }

    pub(crate) fn add_result(
        &self,
        hash: H,
        lin_point: XactLinPoint<u128>,
        res: Vec<u8>
    ) -> Result<(), MutexPoison> {
        if let Some(xact) = self.xacts.lock().map_err(|_| MutexPoison)?
            .get_mut(&hash) {
            // Consistency check with state.
            let check = match &xact.state {
                // This shouldn't happen, as it shouldn't have been
                // sent to the processor.
                PeerXactState::Pending {
                    stage: PendingXactStage::Commit { .. },
                    ..
                } => {
                    warn!(target: "peer-state",
                          concat!("got normal result for transaction {} ",
                                  "in commit stage"),
                          hash);

                    ResultState::Error
                }
                // This shouldn't happen, as it shouldn't have been
                // sent to the processor.
                PeerXactState::Pending {
                    stage: PendingXactStage::PreCommit {
                        dispatched: false,
                        ..
                    },
                    ..
                } => {
                    warn!(target: "peer-state",
                          concat!("got normal result for transaction {} ",
                                  "in commit stage"),
                          hash);

                    ResultState::Error
                }
                // This shouldn't happen, as it shouldn't have been
                // sent to the processor.
                PeerXactState::Pending {
                    stage: PendingXactStage::Process { dispatched: false, .. },
                    ..
                } => {
                    warn!(target: "peer-state",
                          concat!("got normal result for transaction {} ",
                                  "in commit stage"),
                          hash);

                    ResultState::Error
                }
                // Linearization point does not match.  Report it but
                // allow the transaction to be reported.
                PeerXactState::Pending {
                    stage: PendingXactStage::PreCommit { .. },
                    effects: XactEffects::HardNone { when: Some(requested) },
                    ..
                } if requested != &lin_point => {
                    warn!(target: "peer-state",
                          concat!("actual linearization point {} does ",
                                  "not match requested {} "),
                          lin_point, requested);

                    ResultState::Error
                }
                PeerXactState::Pending {
                    stage: PendingXactStage::Process {
                        lin_point: requested,
                        ..
                    },
                    ..
                } if requested != &lin_point => {
                    warn!(target: "peer-state",
                          concat!("actual linearization point {} does ",
                                  "not match requested {} "),
                          lin_point, requested);

                    ResultState::Error
                }
                // This indicates inconsistency with the processor.
                PeerXactState::Error { .. } => {
                    warn!(target: "peer-state",
                          "got normal result for errored transaction {}",
                          hash);

                    ResultState::Error
                },
                // This indicates inconsistency with the processor,
                // but there is noting we can do, as we're already in
                // the complete state.
                PeerXactState::Complete {
                    lin_point: requested,
                    ..
                } if requested != &lin_point => {
                    warn!(target: "peer-state",
                          concat!("actual linearization point {} does ",
                                  "not match requested {} "),
                          lin_point, requested);

                    ResultState::Ignore
                }
                // This is ok and can happen from a delayed message.
                PeerXactState::Complete { .. } => {
                    debug!(target: "peer-state",
                           "disregarding result for completed transaction {}",
                           hash);

                    ResultState::Ignore
                }
                _ => ResultState::Ok
            };

            match check {
                ResultState::Ok => {
                    debug!(target: "peer-state",
                           "setting transaction {} to completed",
                           hash);

                    // XXX should wait until all notifications are done to
                    // expire.
                    let expire = Instant::now() + self.tombstone_duration;

                    xact.state = PeerXactState::Complete {
                        lin_point: lin_point,
                        expire: expire,
                        res: res
                    }
                }
                ResultState::Error => {
                    debug!(target: "peer-state",
                           "setting transaction {} to internal error",
                           hash);

                    // XXX should wait until all notifications are done to
                    // expire.
                    let expire = Instant::now() + self.tombstone_duration;

                    xact.state = PeerXactState::Error {
                        error: XactError::Internal,
                        expire: expire
                    }
                }
                ResultState::Ignore => {}
            }
        } else {
            error!(target: "peer-state",
                   "cannot find transaction {}",
                   hash);
        }

        Ok(())
    }

    pub(crate) fn add_error (
        &self,
        hash: H,
        error: XactError<Vec<u8>>,
    ) -> Result<(), MutexPoison> {
        if let Some(xact) = self.xacts.lock().map_err(|_| MutexPoison)?
            .get_mut(&hash) {
            // Consistency check with state.
            let check = match &xact.state {
                // This shouldn't happen, as it shouldn't have been
                // sent to the processor.
                PeerXactState::Pending {
                    stage: PendingXactStage::Commit { .. },
                    ..
                } => {
                    warn!(target: "peer-state",
                          concat!("got normal result for transaction {} ",
                                  "in commit stage"),
                          hash);

                    ResultState::Error
                }
                // This shouldn't happen, as it shouldn't have been
                // sent to the processor.
                PeerXactState::Pending {
                    stage: PendingXactStage::PreCommit {
                        dispatched: false,
                        ..
                    },
                    ..
                } => {
                    warn!(target: "peer-state",
                          concat!("got normal result for transaction {} ",
                                  "in commit stage"),
                          hash);

                    ResultState::Error
                }
                // This shouldn't happen, as it shouldn't have been
                // sent to the processor.
                PeerXactState::Pending {
                    stage: PendingXactStage::Process { dispatched: false, .. },
                    ..
                } => {
                    warn!(target: "peer-state",
                          concat!("got normal result for transaction {} ",
                                  "in commit stage"),
                          hash);

                    ResultState::Error
                }
                // This indicates inconsistency with the processor.
                PeerXactState::Complete { .. } => {
                    warn!(target: "peer-state",
                          "got normal result for completed transaction {}",
                          hash);

                    ResultState::Error
                }
                // This indicates inconsistency with the processor,
                // but there is nothing we can do, because it's
                // already in the error state.
                PeerXactState::Error {
                    error: actual,
                    ..
                } if actual != &error => {
                    warn!(target: "peer-state",
                           "inconsistent error for transaction {}",
                           hash);

                    ResultState::Ignore
                },
                // This is ok and can happen from a delayed message.
                PeerXactState::Error { .. } => {
                    debug!(target: "peer-state",
                           "disregarding result for errored transaction {}",
                           hash);

                    ResultState::Ignore
                },
                _ => ResultState::Ok
            };

            match check {
                ResultState::Ok => {
                    debug!(target: "peer-state",
                           "setting transaction {} to completed",
                           hash);

                    // XXX should wait until all notifications are done to
                    // expire.
                    let expire = Instant::now() + self.tombstone_duration;

                    xact.state = PeerXactState::Error {
                        expire: expire,
                        error: error
                    }
                }
                ResultState::Error => {
                    debug!(target: "peer-state",
                           "setting transaction {} to internal error",
                           hash);

                    // XXX should wait until all notifications are done to
                    // expire.
                    let expire = Instant::now() + self.tombstone_duration;

                    xact.state = PeerXactState::Error {
                        error: XactError::Internal,
                        expire: expire
                    }
                }
                ResultState::Ignore => {}
            }
        } else {
            error!(target: "peer-state",
                   "cannot find transaction {}",
                   hash);
        }

        Ok(())
    }

    pub(crate) fn add_consensus_seal(
        &self,
        round: u128,
        seal: XactConsensusSeal<H, Seal>
    ) -> Result<(), MutexPoison> {
        let nents = seal.nhashes();
        let completed = bitvec![0; nents];
        let mut known = bitvec![1; nents];
        let mut retries = HashMap::with_capacity(nents);
        let mut xacts = self.xacts.lock().map_err(|_| MutexPoison)?;
        let mut seals = self.seals.lock().map_err(|_| MutexPoison)?;
        let now = Instant::now();

        if let Entry::Vacant(ent) = seals.entry(round) {
            // Go through all the transactions in the seal and update
            // their states.
            for (i, hash) in seal.hashes().iter().enumerate() {
                // See if we know about the transaction.
                if let Some(ent) = xacts.get_mut(hash) {
                    // Make sure it's in the right stage.
                    if let PeerXactState::Pending { processor, stage, ..} =
                        &mut ent.state {
                        if let Entry::Vacant(ent) =
                            retries.entry(processor.clone()) {
                            ent.insert(ConsensusSealRetry {
                                nretries: 0,
                                when: now
                            });
                        }

                        if !matches!(stage, PendingXactStage::Commit { submitted: true }) {
                            error!(target: "peer-state",
                                   concat!("consensus seal references ",
                                           "transaction {} in wrong stage"),
                                   hash);
                        }

                        debug!(target: "peer-state",
                               "setting transaction {} to dispatch stage",
                               hash);

                        *stage = PendingXactStage::Process {
                            lin_point: XactLinPoint::new(round, i),
                            dispatched: false
                        };
                    } else {
                        // This should never happen, but we can skip it.
                        error!(target: "peer-state",
                               concat!("consensus seal for round {} ",
                                       "references completed transaction {}"),
                               round, hash);
                    }
                } else {
                    // This is ok; we might not have received the
                    // transaction yet.
                    debug!(target: "peer-state",
                          "consensus seal references missing transaction {}",
                          hash);

                    known.set(i, false);
                    self.missing
                        .lock()
                        .map_err(|_| MutexPoison)?
                        .insert(hash.clone(), round);
                }
            }

            ent.insert(ConsensusSealEntry {
                completed: completed,
                retries: retries,
                known: known,
                seal: seal
            });
        } else {
            debug!(target: "peer-state",
                   "consensus seal for round {} already processed",
                   round);
        }

        Ok(())
    }
}

impl Display for ProcessorIdx {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "processor #{}", self.0)
    }
}
