use super::VoteProcessorQueue;
use crate::transport::MessageFlooder;
use rsnano_core::{Vote, VoteSource};
use rsnano_messages::{ConfirmAck, Message};
use rsnano_network::TrafficType;
use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

/// Broadcast a vote to PRs and some non-PRs
pub struct VoteBroadcaster {
    vote_processor_queue: Arc<VoteProcessorQueue>,
    message_flooder: Mutex<MessageFlooder>,
}

impl VoteBroadcaster {
    pub fn new(
        vote_processor_queue: Arc<VoteProcessorQueue>,
        message_flooder: MessageFlooder,
    ) -> Self {
        Self {
            vote_processor_queue,
            message_flooder: Mutex::new(message_flooder),
        }
    }

    /// Broadcast vote to PRs and some non-PRs
    pub fn broadcast(&self, vote: Arc<Vote>) {
        let ack = Message::ConfirmAck(ConfirmAck::new_with_own_vote(vote.deref().clone()));

        self.message_flooder
            .lock()
            .unwrap()
            .flood_prs_and_some_non_prs(&ack, TrafficType::Vote, 2.0);

        self.vote_processor_queue
            .vote2(vote, None, VoteSource::Live);
    }
}
