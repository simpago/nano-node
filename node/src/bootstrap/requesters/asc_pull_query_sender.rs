use std::sync::{Arc, Mutex};

use crate::{
    bootstrap::{
        state::{BootstrapState, QueryType, RunningQuery},
        AscPullQuerySpec, BootstrapConfig,
    },
    stats::{DetailType, StatType, Stats},
    transport::MessageSender,
};
use rand::{thread_rng, RngCore};
use rsnano_messages::{AscPullReq, Message};
use rsnano_network::TrafficType;
use rsnano_nullable_clock::SteadyClock;

pub(crate) struct AscPullQuerySender {
    pub message_sender: MessageSender,
    pub clock: Arc<SteadyClock>,
    pub config: BootstrapConfig,
    pub stats: Arc<Stats>,
}

impl AscPullQuerySender {
    pub fn send(&mut self, spec: AscPullQuerySpec, state: &mut BootstrapState) {
        let id = thread_rng().next_u64();
        let now = self.clock.now();
        let query = RunningQuery::from_spec(id, &spec, now, self.config.request_timeout);

        let request = AscPullReq {
            id,
            req_type: spec.req_type,
        };

        state.running_queries.insert(query);
        let message = Message::AscPullReq(request);

        let sent =
            self.message_sender
                .try_send(&spec.channel, &message, TrafficType::BootstrapRequests);

        if sent {
            self.stats.inc(StatType::Bootstrap, DetailType::Request);
            let query_type = QueryType::from(&message);
            self.stats
                .inc(StatType::BootstrapRequest, query_type.into());
        } else {
            self.stats
                .inc(StatType::Bootstrap, DetailType::RequestFailed);
        }

        if sent {
            // After the request has been sent, the peer has a limited time to respond
            let response_cutoff = now + self.config.request_timeout;
            state.set_response_cutoff(id, response_cutoff);
        } else {
            state.remove_query(id);
        }
        if sent && spec.cooldown_account {
            state.candidate_accounts.timestamp_set(&spec.account, now);
        }
    }
}
