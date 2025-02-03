use super::{try_send_serialized_message, MessageSender};
use crate::{representatives::OnlineReps, stats::Stats};
use rsnano_messages::{Message, MessageSerializer};
use rsnano_network::{Channel, Network, TrafficType};
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, RwLock},
};

/// Floods messages to PRs and non PRs
#[derive(Clone)]
pub struct MessageFlooder {
    online_reps: Arc<Mutex<OnlineReps>>,
    network: Arc<RwLock<Network>>,
    stats: Arc<Stats>,
    message_serializer: MessageSerializer,
    sender: MessageSender,
}

impl MessageFlooder {
    pub fn new(
        online_reps: Arc<Mutex<OnlineReps>>,
        network: Arc<RwLock<Network>>,
        stats: Arc<Stats>,
        sender: MessageSender,
    ) -> Self {
        Self {
            online_reps,
            network,
            stats,
            message_serializer: sender.get_serializer(),
            sender,
        }
    }

    pub(crate) fn new_null() -> Self {
        Self::new(
            Arc::new(Mutex::new(OnlineReps::default())),
            Arc::new(RwLock::new(Network::new_test_instance())),
            Arc::new(Stats::default()),
            MessageSender::new_null(),
        )
    }

    pub(crate) fn flood_prs_and_some_non_prs(
        &mut self,
        message: &Message,
        traffic_type: TrafficType,
        scale: f32,
    ) {
        let peered_prs = self.online_reps.lock().unwrap().peered_principal_reps();
        for rep in peered_prs {
            self.sender.try_send(rep.channel_id, &message, traffic_type);
        }

        let mut channels;
        let fanout;
        {
            let network = self.network.read().unwrap();
            fanout = network.fanout(scale);
            channels = network.random_list(usize::MAX, 0)
        }

        self.remove_no_pr(&mut channels, fanout);
        for peer in channels {
            self.sender
                .try_send(peer.channel_id(), &message, traffic_type);
        }
    }

    fn remove_no_pr(&self, channels: &mut Vec<Arc<Channel>>, count: usize) {
        {
            let reps = self.online_reps.lock().unwrap();
            channels.retain(|c| !reps.is_pr(c.channel_id()));
        }
        channels.truncate(count);
    }

    pub fn flood(&mut self, message: &Message, traffic_type: TrafficType, scale: f32) {
        let buffer = self.message_serializer.serialize(message);
        let channels;
        {
            let network = self.network.read().unwrap();
            channels = network.random_fanout_realtime(scale);
        }

        for channel in channels {
            try_send_serialized_message(&channel, &self.stats, buffer, message, traffic_type);
        }
    }
}

impl Deref for MessageFlooder {
    type Target = MessageSender;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl DerefMut for MessageFlooder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sender
    }
}
