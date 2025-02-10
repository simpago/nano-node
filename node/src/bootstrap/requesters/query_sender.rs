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
use std::sync::Arc;

/// Sends an AscPullReq message
pub(crate) struct QuerySender {
    pub message_sender: MessageSender,
    pub clock: Arc<SteadyClock>,
    pub config: BootstrapConfig,
    pub stats: Arc<Stats>,
}

impl QuerySender {
    pub fn send(&mut self, spec: AscPullQuerySpec, state: &mut BootstrapState) -> Option<u64> {
        let id = thread_rng().next_u64();
        let now = self.clock.now();
        let mut query = RunningQuery::from_spec(id, &spec, now, self.config.request_timeout);

        let message = Message::AscPullReq(AscPullReq {
            id,
            req_type: spec.req_type,
        });

        let sent =
            self.message_sender
                .try_send(&spec.channel, &message, TrafficType::BootstrapRequests);

        if sent {
            self.stats.inc(StatType::Bootstrap, DetailType::Request);
            let query_type = QueryType::from(&message);
            self.stats
                .inc(StatType::BootstrapRequest, query_type.into());

            // After the request has been sent, the peer has a limited time to respond
            query.response_cutoff = now + self.config.request_timeout;
            state.running_queries.insert(query);

            if spec.cooldown_account {
                state
                    .candidate_accounts
                    .set_last_request(&spec.account, now);
            }

            Some(id)
        } else {
            self.stats
                .inc(StatType::Bootstrap, DetailType::RequestFailed);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_message() {
        let message_sender = MessageSender::new_null();
        let send_tracker = message_sender.track();
        let clock = Arc::new(SteadyClock::new_null());
        let mut query_sender = QuerySender {
            message_sender,
            clock,
            config: Default::default(),
            stats: Arc::new(Stats::default()),
        };

        let spec = AscPullQuerySpec::new_test_instance();
        let channel_id = spec.channel.channel_id();
        let mut state = BootstrapState::new_test_instance();

        let id = query_sender.send(spec, &mut state);
        assert!(id.is_some());

        let output = send_tracker.output();
        assert_eq!(output.len(), 1, "no message sent!");
        assert_eq!(output[0].channel_id, channel_id);
        assert_eq!(output[0].traffic_type, TrafficType::BootstrapRequests);
        let Message::AscPullReq(_) = &output[0].message else {
            panic!("no asc pull req!")
        };
    }

    #[test]
    fn insert_into_running_queries() {
        let message_sender = MessageSender::new_null();
        let clock = Arc::new(SteadyClock::new_null());
        let now = clock.now();
        let mut query_sender = QuerySender {
            message_sender,
            clock,
            config: Default::default(),
            stats: Arc::new(Stats::default()),
        };

        let spec = AscPullQuerySpec::new_test_instance();
        let mut state = BootstrapState::new_test_instance();

        let id = query_sender.send(spec, &mut state).unwrap();

        assert_eq!(state.running_queries.len(), 1);
        assert!(state.running_queries.contains(id));
        assert_eq!(
            state.running_queries.get(id).unwrap().response_cutoff,
            now + query_sender.config.request_timeout
        );
    }
}
