use super::WebsocketListener;
use rsnano_core::{
    Account, Amount, BlockHash, BlockType, SavedBlock, Vote, VoteCode, VoteWithWeightInfo,
};
use rsnano_messages::TelemetryData;
use rsnano_node::{
    bootstrap::{BootstrapCallbackData, BootstrapInitiator, BootstrapStarted, BootstrapStopped},
    config::WebsocketConfig,
    consensus::{
        ActiveElections, ElectionStatus, ElectionStatusType, ProcessLiveDispatcher, VoteProcessor,
    },
    wallets::Wallets,
    Telemetry,
};
use rsnano_websocket_messages::{new_block_arrived_message, OutgoingMessageEnvelope, Topic};
use serde::{Deserialize, Serialize};
use std::{
    net::{IpAddr, SocketAddr, SocketAddrV6},
    sync::Arc,
    time::UNIX_EPOCH,
};
use tracing::error;

pub fn create_websocket_server(
    config: WebsocketConfig,
    wallets: Arc<Wallets>,
    tokio: tokio::runtime::Handle,
    active_elections: &ActiveElections,
    telemetry: &Telemetry,
    vote_processor: &VoteProcessor,
    process_live_dispatcher: &ProcessLiveDispatcher,
    bootstrap_initiator: &BootstrapInitiator,
) -> Option<Arc<WebsocketListener>> {
    if !config.enabled {
        return None;
    }

    let Ok(address) = config.address.parse::<IpAddr>() else {
        error!(address = config.address, "invalid websocket IP address");
        return None;
    };

    let endpoint = SocketAddr::new(address, config.port);
    let server = Arc::new(WebsocketListener::new(endpoint, wallets, tokio.clone()));

    let server_w = Arc::downgrade(&server);
    active_elections.on_election_ended(Box::new(
        move |status: &ElectionStatus,
              votes: &Vec<VoteWithWeightInfo>,
              account: Account,
              amount: Amount,
              is_state_send: bool,
              is_state_epoch: bool| {
            if let Some(server) = server_w.upgrade() {
                debug_assert!(status.election_status_type != ElectionStatusType::Ongoing);

                if server.any_subscriber(Topic::Confirmation) {
                    let block = status.winner.as_ref().unwrap();
                    let subtype = if is_state_send {
                        "send"
                    } else if block.block_type() == BlockType::State {
                        if block.is_change() {
                            "change"
                        } else if is_state_epoch {
                            "epoch"
                        } else {
                            "receive"
                        }
                    } else {
                        ""
                    };

                    server.broadcast_confirmation(block, &account, &amount, subtype, status, votes);
                }
            }
        },
    ));

    let server_w = Arc::downgrade(&server);
    active_elections.on_active_started(Box::new(move |hash| {
        if let Some(server) = server_w.upgrade() {
            if server.any_subscriber(Topic::StartedElection) {
                server.broadcast(&started_election(&hash));
            }
        }
    }));

    let server_w = Arc::downgrade(&server);
    active_elections.on_active_stopped(Box::new(move |hash| {
        if let Some(server) = server_w.upgrade() {
            if server.any_subscriber(Topic::StoppedElection) {
                server.broadcast(&stopped_election(&hash));
            }
        }
    }));

    let server_w = Arc::downgrade(&server);
    telemetry.on_telemetry_processed(Box::new(move |data, peer_addr| {
        if let Some(server) = server_w.upgrade() {
            if server.any_subscriber(Topic::Telemetry) {
                server.broadcast(&telemetry_received(data, *peer_addr));
            }
        }
    }));

    let server_w = Arc::downgrade(&server);
    vote_processor.add_vote_processed_callback(Box::new(
        move |vote, _channel, _source, vote_code| {
            if let Some(server) = server_w.upgrade() {
                if server.any_subscriber(Topic::Vote) {
                    server.broadcast(&vote_received(vote, vote_code));
                }
            }
        },
    ));

    let server_w: std::sync::Weak<WebsocketListener> = Arc::downgrade(&server);
    process_live_dispatcher.add_new_unconfirmed_block_callback(Arc::new(
        move |block: &SavedBlock| {
            if let Some(server) = server_w.upgrade() {
                if server.any_subscriber(Topic::NewUnconfirmedBlock) {
                    server.broadcast(&new_block_arrived_message(block));
                }
            }
        },
    ));

    let server_w: std::sync::Weak<WebsocketListener> = Arc::downgrade(&server);
    bootstrap_initiator.on_bootstrap_started(Arc::new(move |bootstrap_callback_data| {
        if let Some(server) = server_w.upgrade() {
            if server.any_subscriber(Topic::Bootstrap) {
                server.broadcast(&bootstrap_started(bootstrap_callback_data));
            }
        }
    }));

    let server_w: std::sync::Weak<WebsocketListener> = Arc::downgrade(&server);
    bootstrap_initiator.on_bootstrap_stopped(Arc::new(move |bootstrap_callback_data| {
        if let Some(server) = server_w.upgrade() {
            if server.any_subscriber(Topic::Bootstrap) {
                server.broadcast(&bootstrap_stopped(bootstrap_callback_data));
            }
        }
    }));

    Some(server)
}

fn telemetry_received(data: &TelemetryData, endpoint: SocketAddrV6) -> OutgoingMessageEnvelope {
    OutgoingMessageEnvelope::new(
        Topic::Telemetry,
        TelemetryReceived {
            block_count: data.block_count.to_string(),
            cemented_count: data.cemented_count.to_string(),
            unchecked_count: data.unchecked_count.to_string(),
            account_count: data.account_count.to_string(),
            bandwidth_cap: data.bandwidth_cap.to_string(),
            peer_count: data.peer_count.to_string(),
            protocol_version: data.protocol_version.to_string(),
            uptime: data.uptime.to_string(),
            genesis_block: data.genesis_block.to_string(),
            major_version: data.major_version.to_string(),
            minor_version: data.minor_version.to_string(),
            patch_version: data.patch_version.to_string(),
            pre_release_version: data.pre_release_version.to_string(),
            maker: data.maker.to_string(),
            timestamp: data
                .timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .to_string(),
            active_difficulty: format!("{:016x}", data.active_difficulty),
            node_id: data.node_id.to_string(),
            signature: data.signature.encode_hex(),
            address: endpoint.ip().to_string(),
            port: endpoint.port().to_string(),
        },
    )
}

#[derive(Serialize, Deserialize)]
pub struct TelemetryReceived {
    pub block_count: String,
    pub cemented_count: String,
    pub unchecked_count: String,
    pub account_count: String,
    pub bandwidth_cap: String,
    pub peer_count: String,
    pub protocol_version: String,
    pub uptime: String,
    pub genesis_block: String,
    pub major_version: String,
    pub minor_version: String,
    pub patch_version: String,
    pub pre_release_version: String,
    pub maker: String,
    pub timestamp: String,
    pub active_difficulty: String,
    pub node_id: String,
    pub signature: String,
    pub address: String,
    pub port: String,
}

fn started_election(hash: &BlockHash) -> OutgoingMessageEnvelope {
    OutgoingMessageEnvelope::new(
        Topic::StartedElection,
        StartedElection {
            hash: hash.to_string(),
        },
    )
}

#[derive(Serialize)]
struct StartedElection {
    hash: String,
}

fn stopped_election(hash: &BlockHash) -> OutgoingMessageEnvelope {
    OutgoingMessageEnvelope::new(
        Topic::StoppedElection,
        StoppedElection {
            hash: hash.to_string(),
        },
    )
}

#[derive(Serialize)]
struct StoppedElection {
    hash: String,
}

pub fn vote_received(vote: &Vote, code: VoteCode) -> OutgoingMessageEnvelope {
    OutgoingMessageEnvelope::new(
        Topic::Vote,
        VoteReceived {
            account: Account::from(vote.voting_account).encode_account(),
            signature: vote.signature.encode_hex(),
            sequence: vote.timestamp().to_string(),
            timestamp: vote.timestamp().to_string(),
            duration: vote.duration_bits().to_string(),
            blocks: vote.hashes.iter().map(|h| h.to_string()).collect(),
            vote_type: code.as_str().to_string(),
        },
    )
}

#[derive(Serialize, Deserialize)]
pub struct VoteReceived {
    pub account: String,
    pub signature: String,
    pub sequence: String,
    pub timestamp: String,
    pub duration: String,
    pub blocks: Vec<String>,
    #[serde(rename = "type")]
    pub vote_type: String,
}

fn bootstrap_stopped(bootstrap_callback_data: &BootstrapCallbackData) -> OutgoingMessageEnvelope {
    OutgoingMessageEnvelope::new(
        Topic::Bootstrap,
        BootstrapStopped {
            reason: "exited".to_owned(),
            id: bootstrap_callback_data.id.clone(),
            mode: bootstrap_callback_data.mode.as_str().to_string(),
            total_blocks: bootstrap_callback_data.total_blocks.to_string(),
            duration: bootstrap_callback_data.duration.as_secs().to_string(),
        },
    )
}

fn bootstrap_started(bootstrap_callback_data: &BootstrapCallbackData) -> OutgoingMessageEnvelope {
    OutgoingMessageEnvelope::new(
        Topic::Bootstrap,
        BootstrapStarted {
            reason: "started".to_owned(),
            id: bootstrap_callback_data.id.clone(),
            mode: bootstrap_callback_data.mode.as_str().to_string(),
        },
    )
}
