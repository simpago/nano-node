use crate::message_collection::MessageCollection;
use rsnano_core::Amount;
use rsnano_ledger::RepWeightCache;
use rsnano_messages::TelemetryData;
use rsnano_network::{Channel, ChannelDirection, ChannelId};
use rsnano_node::representatives::PeeredRep;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddrV6,
    sync::{Arc, RwLock},
};

pub(crate) struct ChannelModel {
    pub channel_id: ChannelId,
    pub remote_addr: SocketAddrV6,
    pub direction: ChannelDirection,
    pub telemetry: Option<TelemetryData>,
    pub rep_weight: Amount,
    pub rep_state: RepState,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepState {
    PrincipalRep,
    Rep,
    NoRep,
}

pub(crate) struct Channels {
    channel_map: HashMap<ChannelId, ChannelModel>,
    sorted_channels: Vec<(ChannelId, SocketAddrV6)>,
    selected: Option<ChannelId>,
    selected_index: Option<usize>,
    messages: Arc<RwLock<MessageCollection>>,
}

impl Channels {
    pub(crate) fn new(messages: Arc<RwLock<MessageCollection>>) -> Self {
        Self {
            sorted_channels: Vec::new(),
            channel_map: HashMap::new(),
            selected: None,
            selected_index: None,
            messages,
        }
    }

    pub(crate) fn update(
        &mut self,
        channels: Vec<Arc<Channel>>,
        telemetries: HashMap<SocketAddrV6, TelemetryData>,
        reps: Vec<PeeredRep>,
        rep_weights: &RepWeightCache,
        min_rep_weight: Amount,
    ) {
        let mut inserted = false;
        {
            let mut pending: HashSet<ChannelId> = self.channel_map.keys().cloned().collect();

            for info in channels {
                if let Some(channel) = self.channel_map.get_mut(&info.channel_id()) {
                    channel.telemetry = telemetries.get(&channel.remote_addr).cloned();
                    pending.remove(&info.channel_id());
                } else {
                    self.channel_map.insert(
                        info.channel_id(),
                        ChannelModel {
                            channel_id: info.channel_id(),
                            remote_addr: info.peer_addr(),
                            direction: info.direction(),
                            telemetry: None,
                            rep_weight: Amount::zero(),
                            rep_state: RepState::NoRep,
                        },
                    );
                    inserted = true;
                }
            }

            for key in pending {
                self.channel_map.remove(&key);
            }
        }

        if inserted {
            self.sorted_channels = self
                .channel_map
                .values()
                .map(|c| (c.channel_id, c.remote_addr))
                .collect();
            self.sorted_channels.sort_by_key(|c| c.1);
        }

        let weights = rep_weights.read();
        for rep in reps {
            if let Some(channel) = self.channel_map.get_mut(&rep.channel_id()) {
                channel.rep_weight = weights.get(&rep.account).cloned().unwrap_or_default();
                channel.rep_state = if channel.rep_weight > min_rep_weight {
                    RepState::PrincipalRep
                } else if channel.rep_weight > Amount::zero() {
                    RepState::Rep
                } else {
                    RepState::NoRep
                };
            }
        }

        // Recalculate selected index
        if let Some(channel_id) = self.selected {
            match self
                .sorted_channels
                .iter()
                .enumerate()
                .find(|(_, (id, _))| *id == channel_id)
            {
                Some((i, _)) => self.selected_index = Some(i),
                None => {
                    self.selected = None;
                    self.selected_index = None;
                }
            }
        }
    }

    pub(crate) fn get(&self, index: usize) -> Option<&ChannelModel> {
        let (channel_id, _) = self.sorted_channels.get(index)?;
        self.channel_map.get(channel_id)
    }

    pub fn len(&self) -> usize {
        self.sorted_channels.len()
    }

    pub(crate) fn select_index(&mut self, index: usize) {
        let Some((channel_id, _)) = self.sorted_channels.get(index) else {
            return;
        };
        if self.selected == Some(*channel_id) {
            self.selected = None;
            self.selected_index = None;
            self.messages.write().unwrap().filter_channel(None)
        } else {
            self.selected = Some(*channel_id);
            self.selected_index = Some(index);
            self.messages
                .write()
                .unwrap()
                .filter_channel(Some(*channel_id))
        }
    }

    pub(crate) fn selected_index(&self) -> Option<usize> {
        self.selected_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_collection::{MessageCollection, MessageFilter};
    use std::{collections::HashMap, sync::RwLock};

    #[test]
    fn when_channel_selected_should_set_message_filter() {
        let messages = Arc::new(RwLock::new(MessageCollection::default()));
        let mut channels = Channels::new(messages.clone());
        channels.update(
            vec![Arc::new(Channel::new_test_instance())],
            HashMap::new(),
            Vec::new(),
            &RepWeightCache::new(),
            Amount::zero(),
        );
        channels.select_index(0);
        let guard = messages.read().unwrap();
        assert_eq!(
            guard.current_filter(),
            &MessageFilter::channel(channels.sorted_channels[0].0)
        );
    }

    #[test]
    fn when_channel_deselected_should_clear_message_filter() {
        let messages = Arc::new(RwLock::new(MessageCollection::default()));
        let mut channels = Channels::new(messages.clone());
        channels.update(
            vec![Arc::new(Channel::new_test_instance())],
            HashMap::new(),
            Vec::new(),
            &RepWeightCache::new(),
            Amount::zero(),
        );
        channels.select_index(0);
        channels.select_index(0);
        let guard = messages.read().unwrap();
        assert_eq!(guard.current_filter(), &MessageFilter::all());
    }
}
