use super::{Election, ElectionData};
use crate::{config::NetworkParams, representatives::PeeredRepInfo, transport::MessageFlooder};
use rsnano_core::{BlockHash, Root};
use rsnano_messages::{ConfirmReq, Message, Publish};
use rsnano_network::{Channel, ChannelId, Network, TrafficType};
use std::{
    cmp::max,
    collections::HashMap,
    sync::{atomic::Ordering, Arc, MutexGuard, RwLock},
};

/// This struct accepts elections that need further votes before they can be confirmed and bundles them in to single confirm_req packets
pub struct ConfirmationSolicitor {
    /// Global maximum amount of block broadcasts
    max_block_broadcasts: usize,
    /// Maximum amount of requests to be sent per election, bypassed if an existing vote is for a different hash
    max_election_requests: usize,
    /// Maximum amount of directed broadcasts to be sent per election
    max_election_broadcasts: usize,
    representative_requests: Vec<PeeredRepInfo>,
    representative_broadcasts: Vec<PeeredRepInfo>,
    requests: HashMap<ChannelId, (Arc<Channel>, Vec<(BlockHash, Root)>)>,
    prepared: bool,
    rebroadcasted: usize,
    message_flooder: MessageFlooder,
}

impl ConfirmationSolicitor {
    pub fn new(
        network_params: &NetworkParams,
        network: &RwLock<Network>,
        message_flooder: MessageFlooder,
    ) -> Self {
        let max_election_broadcasts = max(network.read().unwrap().fanout(1.0) / 2, 1);
        Self {
            max_block_broadcasts: if network_params.network.is_dev_network() {
                4
            } else {
                30
            },
            max_election_requests: 50,
            max_election_broadcasts,
            prepared: false,
            representative_requests: Vec::new(),
            representative_broadcasts: Vec::new(),
            requests: HashMap::new(),
            rebroadcasted: 0,
            message_flooder,
        }
    }

    /// Prepare object for batching election confirmation requests
    pub fn prepare(&mut self, representatives: &[PeeredRepInfo]) {
        debug_assert!(!self.prepared);
        self.requests.clear();
        self.rebroadcasted = 0;
        self.representative_requests = representatives.to_vec();
        self.representative_broadcasts = representatives.to_vec();
        self.prepared = true;
    }

    /// Broadcast the winner of an election if the broadcast limit has not been reached. Returns false if the broadcast was performed
    pub fn broadcast(&mut self, guard: &MutexGuard<ElectionData>) -> Result<(), ()> {
        debug_assert!(self.prepared);
        self.rebroadcasted += 1;
        if self.rebroadcasted >= self.max_block_broadcasts {
            return Err(());
        }

        let winner_block = guard.status.winner.as_ref().unwrap();
        let hash = winner_block.hash();
        let winner = Message::Publish(Publish::new_forward(winner_block.clone().into()));
        let mut count = 0;
        // Directed broadcasting to principal representatives
        for i in &self.representative_broadcasts {
            if count >= self.max_election_broadcasts {
                break;
            }
            let should_broadcast = if let Some(existing) = guard.last_votes.get(&i.rep_key) {
                existing.hash != hash
            } else {
                count += 1;
                true
            };
            if should_broadcast {
                self.message_flooder
                    .try_send(&i.channel, &winner, TrafficType::BlockBroadcast);
            }
        }
        // Random flood for block propagation
        // TODO: Avoid broadcasting to the same peers that were already broadcasted to
        self.message_flooder
            .flood(&winner, TrafficType::BlockBroadcast, 0.5);
        Ok(())
    }

    /// Add an election that needs to be confirmed. Returns false if successfully added
    pub fn add(&mut self, election: &Election, guard: &MutexGuard<ElectionData>) -> bool {
        debug_assert!(self.prepared);
        let mut error = true;
        let mut count = 0;
        let winner = guard.status.winner.as_ref().unwrap();
        let hash = winner.hash();
        let mut to_remove = Vec::new();
        for rep in &self.representative_requests {
            if count >= self.max_election_requests {
                break;
            }
            let mut full_queue = false;
            let existing = guard.last_votes.get(&rep.rep_key);
            let exists = existing.is_some();
            let is_final = if let Some(existing) = existing {
                !election.is_quorum.load(Ordering::SeqCst) || existing.timestamp == u64::MAX
            } else {
                false
            };
            let different = if let Some(existing) = existing {
                existing.hash != hash
            } else {
                false
            };
            if !exists || !is_final || different {
                let should_drop = rep.channel.should_drop(TrafficType::ConfirmationRequests);

                if !should_drop {
                    let rep_channel = rep.channel.clone();
                    let (_, request_queue) = self
                        .requests
                        .entry(rep_channel.channel_id())
                        .or_insert_with(|| (rep_channel, Vec::new()));
                    request_queue.push((winner.hash(), winner.root()));
                    if !different {
                        count += 1;
                    }
                    error = false;
                } else {
                    full_queue = true;
                }
            }
            if full_queue {
                to_remove.push(rep.rep_key);
            }
        }

        if !to_remove.is_empty() {
            self.representative_requests
                .retain(|i| !to_remove.contains(&i.rep_key));
        }

        error
    }

    /// Dispatch bundled requests to each channel
    pub fn flush(&mut self) {
        debug_assert!(self.prepared);
        for (channel, requests) in self.requests.values() {
            let mut roots_hashes = Vec::new();
            for root_hash in requests {
                roots_hashes.push(root_hash.clone());
                if roots_hashes.len() == ConfirmReq::HASHES_MAX {
                    let req = Message::ConfirmReq(ConfirmReq::new(roots_hashes));
                    self.message_flooder.try_send(
                        &channel,
                        &req,
                        TrafficType::ConfirmationRequests,
                    );
                    roots_hashes = Vec::new();
                }
            }
            if !roots_hashes.is_empty() {
                let req = Message::ConfirmReq(ConfirmReq::new(roots_hashes));
                self.message_flooder
                    .try_send(channel, &req, TrafficType::ConfirmationRequests);
            }
        }
        self.prepared = false;
    }
}
