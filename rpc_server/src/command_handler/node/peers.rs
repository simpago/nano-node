use crate::command_handler::RpcCommandHandler;
use rsnano_rpc_messages::{DetailedPeers, PeerInfo, PeersArgs, PeersDto, SimplePeers};
use std::{collections::HashMap, net::SocketAddrV6};

impl RpcCommandHandler {
    pub(crate) fn peers(&self, args: PeersArgs) -> PeersDto {
        let peer_details = args.peer_details.unwrap_or_default().inner();
        let mut peers: HashMap<SocketAddrV6, PeerInfo> = HashMap::new();

        self.node
            .network
            .read()
            .unwrap()
            .shuffled_channels()
            .iter()
            .for_each(|channel| {
                peers.insert(
                    channel.peer_addr(),
                    PeerInfo {
                        protocol_version: channel.protocol_version().into(),
                        node_id: channel.node_id().map(|i| i.to_string()).unwrap_or_default(),
                        connection_type: "tcp".to_string(),
                        peering: channel.peering_addr_or_peer_addr(),
                    },
                );
            });

        if peer_details {
            PeersDto::Detailed(DetailedPeers { peers })
        } else {
            PeersDto::Simple(SimplePeers {
                peers: peers
                    .drain()
                    .map(|(k, v)| (k, v.protocol_version))
                    .collect(),
            })
        }
    }
}
