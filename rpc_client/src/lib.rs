use crate::AccountBalanceResponse;
use anyhow::{anyhow, Ok, Result};
use reqwest::Client;
pub use reqwest::Url;
use rsnano_core::{
    Account, Amount, BlockHash, HashOrAccount, JsonBlock, PublicKey, RawKey, WalletId, WorkNonce,
};
use rsnano_rpc_messages::*;
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;

pub struct NanoRpcClient {
    url: Url,
    client: Client,
}

impl NanoRpcClient {
    pub fn new(url: Url) -> Self {
        Self {
            url,
            client: reqwest::ClientBuilder::new()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
        }
    }

    pub async fn telemetry(&self, args: TelemetryArgs) -> Result<TelemetryResponse> {
        self.request(&RpcCommand::telemetry(args)).await
    }

    pub async fn account_get(&self, key: PublicKey) -> Result<AccountResponse> {
        self.request(&RpcCommand::account_get(key)).await
    }

    pub async fn work_peers(&self) -> Result<WorkPeersDto> {
        self.request(&RpcCommand::work_peers()).await
    }

    pub async fn work_peer_add(&self, args: AddressWithPortArgs) -> Result<SuccessResponse> {
        self.request(&RpcCommand::work_peer_add(args)).await
    }

    pub async fn work_peers_clear(&self) -> Result<SuccessResponse> {
        self.request(&RpcCommand::WorkPeersClear).await
    }

    pub async fn block_create(&self, args: BlockCreateArgs) -> Result<BlockCreateResponse> {
        self.request(&RpcCommand::block_create(args)).await
    }

    pub async fn republish(&self, args: impl Into<RepublishArgs>) -> Result<BlockHashesResponse> {
        let cmd = RpcCommand::republish(args.into());
        self.request(&cmd).await
    }

    pub async fn work_generate(
        &self,
        args: impl Into<WorkGenerateArgs>,
    ) -> Result<WorkGenerateDto> {
        let cmd = RpcCommand::work_generate(args.into());
        self.request(&cmd).await
    }

    pub async fn ledger(&self, args: LedgerArgs) -> Result<LedgerResponse> {
        self.request(&RpcCommand::ledger(args)).await
    }

    pub async fn confirmation_info(
        &self,
        args: impl Into<ConfirmationInfoArgs>,
    ) -> Result<ConfirmationInfoDto> {
        let cmd = RpcCommand::ConfirmationInfo(args.into());
        self.request(&cmd).await
    }

    pub async fn unchecked_keys(
        &self,
        key: HashOrAccount,
        count: Option<u64>,
    ) -> Result<UncheckedKeysResponse> {
        let cmd = RpcCommand::unchecked_keys(key, count);
        self.request(&cmd).await
    }

    pub async fn unchecked_get(&self, hash: BlockHash) -> Result<UncheckedGetResponse> {
        self.request(&RpcCommand::unchecked_get(hash)).await
    }

    pub async fn unchecked(&self, count: u64) -> Result<UncheckedResponse> {
        self.request(&RpcCommand::unchecked(count)).await
    }

    pub async fn representatives_online(
        &self,
        args: RepresentativesOnlineArgs,
    ) -> Result<RepresentativesOnlineResponse> {
        let detailed = args.weight.unwrap_or(false.into()).inner();
        let cmd = RpcCommand::RepresentativesOnline(args);
        let result = self.request_raw(&cmd).await?;
        if detailed {
            let detailed: DetailedRepresentativesOnline = serde_json::from_value(result)?;
            Ok(RepresentativesOnlineResponse::Detailed(detailed))
        } else {
            let simple: SimpleRepresentativesOnline = serde_json::from_value(result)?;
            Ok(RepresentativesOnlineResponse::Simple(simple))
        }
    }

    pub async fn receivable_exists(
        &self,
        args: impl Into<ReceivableExistsArgs>,
    ) -> Result<ExistsResponse> {
        let cmd = RpcCommand::receivable_exists(args);
        let result = self.request_raw(&cmd).await?;
        Ok(serde_json::from_value(result)?)
    }

    pub async fn receivable(&self, args: impl Into<ReceivableArgs>) -> Result<ReceivableResponse> {
        let args = args.into();
        let source: bool = args.source.unwrap_or_default().into();
        let min_version: bool = args.min_version.unwrap_or_default().into();
        let sort: bool = args.sorting.unwrap_or_default().into();
        let simple =
            args.threshold.unwrap_or_default().is_zero() && !source && !min_version && !sort;

        let cmd = RpcCommand::Receivable(args);
        let result = self.request_raw(&cmd).await?;
        if simple {
            let blocks = serde_json::from_value::<ReceivableSimple>(result)?;
            Ok(ReceivableResponse::Simple(blocks))
        } else if source || min_version {
            let blocks = serde_json::from_value::<ReceivableSource>(result)?;
            Ok(ReceivableResponse::Source(blocks))
        } else {
            let blocks = serde_json::from_value::<ReceivableThreshold>(result)?;
            Ok(ReceivableResponse::Threshold(blocks))
        }
    }

    pub async fn accounts_receivable(
        &self,
        args: impl Into<AccountsReceivableArgs>,
    ) -> Result<AccountsReceivableResponse> {
        let args = args.into();

        let threshold = args.threshold.unwrap_or_default();
        let source = unwrap_bool_or_false(args.source);
        let simple = threshold.is_zero() && !source && !unwrap_bool_or_false(args.sorting);
        let cmd = RpcCommand::AccountsReceivable(args);
        let result = self.request_raw(&cmd).await?;
        if simple {
            Ok(AccountsReceivableResponse::Simple(serde_json::from_value(
                result,
            )?))
        } else if source {
            Ok(AccountsReceivableResponse::Source(serde_json::from_value(
                result,
            )?))
        } else {
            Ok(AccountsReceivableResponse::Threshold(
                serde_json::from_value(result)?,
            ))
        }
    }

    pub async fn wallet_ledger(
        &self,
        args: impl Into<WalletLedgerArgs>,
    ) -> Result<WalletLedgerResponse> {
        let cmd = RpcCommand::wallet_ledger(args.into());
        self.request(&cmd).await
    }

    pub async fn wallet_history(
        &self,
        args: impl Into<WalletHistoryArgs>,
    ) -> Result<WalletHistoryResponse> {
        let cmd = RpcCommand::wallet_history(args.into());
        self.request(&cmd).await
    }

    pub async fn wallet_republish(
        &self,
        wallet: WalletId,
        count: u64,
    ) -> Result<BlockHashesResponse> {
        let cmd = RpcCommand::wallet_republish(wallet, count);
        self.request(&cmd).await
    }

    pub async fn search_receivable(&self, wallet: WalletId) -> Result<StartedResponse> {
        let cmd = RpcCommand::search_receivable(wallet);
        self.request(&cmd).await
    }

    pub async fn wallet_representative_set(
        &self,
        args: WalletRepresentativeSetArgs,
    ) -> Result<SetResponse> {
        let cmd = RpcCommand::wallet_representative_set(args);
        self.request(&cmd).await
    }

    pub async fn wallet_receivable(
        &self,
        args: WalletReceivableArgs,
    ) -> Result<AccountsReceivableResponse> {
        let cmd = RpcCommand::WalletReceivable(args);
        self.request(&cmd).await
    }

    pub async fn bootstrap_lazy(
        &self,
        args: impl Into<BootstrapLazyArgs>,
    ) -> Result<BootstrapLazyResponse> {
        let cmd = RpcCommand::BootstrapLazy(args.into());
        self.request(&cmd).await
    }

    pub async fn bootstrap_any(&self, args: BootstrapAnyArgs) -> Result<SuccessResponse> {
        self.request(&RpcCommand::BootstrapAny(args)).await
    }

    pub async fn bootstrap(&self, args: BootstrapArgs) -> Result<SuccessResponse> {
        self.request(&RpcCommand::Bootstrap(args)).await
    }

    pub async fn work_cancel(&self, hash: BlockHash) -> Result<SuccessResponse> {
        let cmd = RpcCommand::work_cancel(hash);
        self.request(&cmd).await
    }

    pub async fn process(&self, args: impl Into<ProcessArgs>) -> Result<HashRpcMessage> {
        let cmd = RpcCommand::process(args.into());
        self.request(&cmd).await
    }

    pub async fn sign(&self, args: impl Into<SignArgs>) -> Result<SignResponse> {
        self.request(&RpcCommand::sign(args.into())).await
    }

    pub async fn account_history(
        &self,
        args: impl Into<AccountHistoryArgs>,
    ) -> Result<AccountHistoryResponse> {
        let cmd = RpcCommand::account_history(args.into());
        self.request(&cmd).await
    }

    pub async fn account_balance(
        &self,
        args: impl Into<AccountBalanceArgs>,
    ) -> Result<AccountBalanceResponse> {
        let cmd = RpcCommand::AccountBalance(args.into());
        self.request(&cmd).await
    }

    pub async fn account_create(
        &self,
        args: impl Into<AccountCreateArgs>,
    ) -> Result<AccountResponse> {
        let cmd = RpcCommand::account_create(args.into());
        self.request(&cmd).await
    }

    pub async fn accounts_create(
        &self,
        wallet: WalletId,
        count: u64,
    ) -> Result<AccountsRpcMessage> {
        self.accounts_create_args(AccountsCreateArgs::build(wallet, count).finish())
            .await
    }

    pub async fn accounts_create_args(
        &self,
        args: impl Into<AccountsCreateArgs>,
    ) -> Result<AccountsRpcMessage> {
        let cmd = RpcCommand::AccountsCreate(args.into());
        self.request(&cmd).await
    }

    pub async fn account_remove(&self, wallet: WalletId, account: Account) -> Result<RemovedDto> {
        let cmd = RpcCommand::account_remove(wallet, account);
        self.request(&cmd).await
    }

    pub async fn account_move(
        &self,
        wallet: WalletId,
        source: WalletId,
        account: Vec<Account>,
    ) -> Result<MovedResponse> {
        let cmd = RpcCommand::account_move(wallet, source, account);
        self.request(&cmd).await
    }

    pub async fn account_list(&self, wallet: WalletId) -> Result<AccountsRpcMessage> {
        self.request(&RpcCommand::account_list(wallet)).await
    }

    pub async fn wallet_create(&self, seed: Option<RawKey>) -> Result<WalletCreateResponse> {
        self.request(&RpcCommand::wallet_create(seed)).await
    }

    pub async fn wallet_contains(
        &self,
        wallet: WalletId,
        account: Account,
    ) -> Result<ExistsResponse> {
        let cmd = RpcCommand::wallet_contains(wallet, account);
        self.request(&cmd).await
    }

    pub async fn wallet_destroy(&self, wallet: WalletId) -> Result<DestroyedResponse> {
        self.request(&RpcCommand::wallet_destroy(wallet)).await
    }

    pub async fn wallet_lock(&self, wallet: WalletId) -> Result<LockedResponse> {
        self.request(&RpcCommand::wallet_lock(wallet)).await
    }

    pub async fn wallet_locked(&self, wallet: WalletId) -> Result<LockedResponse> {
        self.request(&RpcCommand::wallet_locked(wallet)).await
    }

    pub async fn stop(&self) -> Result<SuccessResponse> {
        self.request(&RpcCommand::stop()).await
    }

    pub async fn account_block_count(&self, account: Account) -> Result<AccountBlockCountResponse> {
        let cmd = RpcCommand::account_block_count(account);
        self.request(&cmd).await
    }

    pub async fn account_key(&self, account: Account) -> Result<KeyResponse> {
        self.request(&RpcCommand::account_key(account)).await
    }

    pub async fn account_representative(
        &self,
        account: Account,
    ) -> Result<AccountRepresentativeDto> {
        let cmd = RpcCommand::account_representative(account);
        self.request(&cmd).await
    }

    pub async fn account_weight(&self, account: Account) -> Result<WeightDto> {
        self.request(&RpcCommand::account_weight(account)).await
    }

    pub async fn available_supply(&self) -> Result<AvailableSupplyReponse> {
        self.request(&RpcCommand::AvailableSupply).await
    }

    pub async fn block_account(&self, hash: BlockHash) -> Result<AccountResponse> {
        self.request(&RpcCommand::block_account(hash)).await
    }

    pub async fn block_confirm(&self, hash: BlockHash) -> Result<StartedResponse> {
        self.request(&RpcCommand::block_confirm(hash)).await
    }

    pub async fn block_count(&self) -> Result<BlockCountResponse> {
        self.request(&RpcCommand::BlockCount).await
    }

    pub async fn uptime(&self) -> Result<UptimeResponse> {
        self.request(&RpcCommand::uptime()).await
    }

    pub async fn frontier_count(&self) -> Result<CountResponse> {
        self.request(&RpcCommand::FrontierCount).await
    }

    pub async fn validate_account_number(
        &self,
        account: impl Into<String>,
    ) -> Result<ValidResponse> {
        let cmd = RpcCommand::validate_account_number(account.into());
        self.request(&cmd).await
    }

    pub async fn nano_to_raw(&self, amount: u64) -> Result<AmountRpcMessage> {
        self.request(&RpcCommand::nano_to_raw(amount)).await
    }

    pub async fn raw_to_nano(&self, amount: Amount) -> Result<AmountRpcMessage> {
        self.request(&RpcCommand::raw_to_nano(amount)).await
    }

    pub async fn wallet_add_watch(
        &self,
        wallet: WalletId,
        accounts: Vec<Account>,
    ) -> Result<SuccessResponse> {
        self.request(&RpcCommand::wallet_add_watch(wallet, accounts))
            .await
    }

    pub async fn wallet_representative(
        &self,
        wallet: WalletId,
    ) -> Result<WalletRepresentativeResponse> {
        self.request(&RpcCommand::wallet_representative(wallet))
            .await
    }

    pub async fn work_set(
        &self,
        wallet: WalletId,
        account: Account,
        work: WorkNonce,
    ) -> Result<SuccessResponse> {
        self.request(&RpcCommand::work_set(wallet, account, work))
            .await
    }

    pub async fn work_get(&self, wallet: WalletId, account: Account) -> Result<WorkResponse> {
        self.request(&RpcCommand::work_get(wallet, account)).await
    }

    pub async fn wallet_work_get(&self, wallet: WalletId) -> Result<AccountsWithWorkResponse> {
        self.request(&RpcCommand::wallet_work_get(wallet)).await
    }

    pub async fn accounts_frontiers(&self, accounts: Vec<Account>) -> Result<FrontiersResponse> {
        self.request(&RpcCommand::accounts_frontiers(accounts))
            .await
    }

    pub async fn wallet_frontiers(&self, wallet: WalletId) -> Result<FrontiersResponse> {
        self.request(&RpcCommand::wallet_frontiers(wallet)).await
    }

    pub async fn frontiers(&self, account: Account, count: u64) -> Result<FrontiersResponse> {
        self.request(&RpcCommand::frontiers(account, count)).await
    }

    pub async fn wallet_info(&self, wallet: WalletId) -> Result<WalletInfoResponse> {
        self.request(&RpcCommand::wallet_info(wallet)).await
    }

    pub async fn wallet_export(&self, wallet: WalletId) -> Result<JsonResponse> {
        self.request(&RpcCommand::wallet_export(wallet)).await
    }

    pub async fn password_change(
        &self,
        wallet: WalletId,
        password: String,
    ) -> Result<ChangedResponse> {
        self.request(&RpcCommand::password_change(wallet, password))
            .await
    }

    pub async fn password_enter(
        &self,
        wallet: WalletId,
        password: String,
    ) -> Result<ValidResponse> {
        self.request(&RpcCommand::password_enter(wallet, password))
            .await
    }

    pub async fn password_valid(&self, wallet: WalletId) -> Result<ValidResponse> {
        let cmd = RpcCommand::password_valid(wallet);
        let result = self.request_raw(&cmd).await?;
        Ok(serde_json::from_value(result)?)
    }

    pub async fn deterministic_key(&self, seed: RawKey, index: u32) -> Result<KeyPairDto> {
        self.request(&RpcCommand::deterministic_key(seed, index))
            .await
    }

    pub async fn key_expand(&self, key: RawKey) -> Result<KeyPairDto> {
        self.request(&RpcCommand::key_expand(key)).await
    }

    pub async fn peers(&self, peer_details: Option<bool>) -> Result<PeersDto> {
        let result = self.request_raw(&RpcCommand::peers(peer_details)).await?;
        if peer_details.unwrap_or_default() {
            let peers: DetailedPeers = serde_json::from_value(result)?;
            Ok(PeersDto::Detailed(peers))
        } else {
            let peers: SimplePeers = serde_json::from_value(result)?;
            Ok(PeersDto::Simple(peers))
        }
    }

    pub async fn populate_backlog(&self) -> Result<SuccessResponse> {
        self.request(&RpcCommand::PopulateBacklog).await
    }

    pub async fn representatives(&self) -> Result<RepresentativesResponse> {
        self.request(&RpcCommand::representatives()).await
    }

    pub async fn representatives_with(
        &self,
        args: RepresentativesArgs,
    ) -> Result<RepresentativesResponse> {
        self.request(&RpcCommand::Representatives(args)).await
    }

    pub async fn accounts_representatives(
        &self,
        accounts: Vec<Account>,
    ) -> Result<AccountsRepresentativesResponse> {
        self.request(&RpcCommand::accounts_representatives(accounts))
            .await
    }

    pub async fn stats_clear(&self) -> Result<SuccessResponse> {
        self.request(&RpcCommand::stats_clear()).await
    }

    pub async fn unchecked_clear(&self) -> Result<SuccessResponse> {
        self.request(&RpcCommand::UncheckedClear).await
    }

    pub async fn unopened(&self, args: impl Into<UnopenedArgs>) -> Result<AccountsWithAmountsDto> {
        self.request(&RpcCommand::Unopened(args.into())).await
    }

    pub async fn node_id(&self) -> Result<NodeIdResponse> {
        self.request(&RpcCommand::node_id()).await
    }

    pub async fn search_receivable_all(&self) -> Result<SuccessResponse> {
        self.request(&RpcCommand::search_receivable_all()).await
    }

    pub async fn receive_minimum(&self) -> Result<AmountRpcMessage> {
        self.request(&RpcCommand::receive_minimum()).await
    }

    pub async fn wallet_change_seed(
        &self,
        args: impl Into<WalletChangeSeedArgs>,
    ) -> Result<WalletChangeSeedResponse> {
        self.request(&RpcCommand::wallet_change_seed(args.into()))
            .await
    }

    pub async fn delegators(&self, args: impl Into<DelegatorsArgs>) -> Result<DelegatorsResponse> {
        self.request(&RpcCommand::Delegators(args.into())).await
    }

    pub async fn delegators_count(&self, account: Account) -> Result<CountResponse> {
        self.request(&RpcCommand::delegators_count(account)).await
    }

    pub async fn block_hash(&self, block: JsonBlock) -> Result<HashRpcMessage> {
        self.request(&RpcCommand::block_hash(block)).await
    }

    pub async fn accounts_balances(
        &self,
        args: impl Into<AccountsBalancesArgs>,
    ) -> Result<AccountsBalancesResponse> {
        self.request(&RpcCommand::AccountsBalances(args.into()))
            .await
    }

    pub async fn wallet_balances(
        &self,
        args: impl Into<WalletBalancesArgs>,
    ) -> Result<AccountsBalancesResponse> {
        self.request(&RpcCommand::wallet_balances(args.into()))
            .await
    }

    pub async fn block_info(&self, hash: BlockHash) -> Result<BlockInfoResponse> {
        self.request(&RpcCommand::block_info(hash)).await
    }

    pub async fn blocks(&self, blocks: Vec<BlockHash>) -> Result<BlocksResponse> {
        self.request(&RpcCommand::blocks(blocks)).await
    }

    pub async fn blocks_info(&self, blocks: Vec<BlockHash>) -> Result<BlocksInfoResponse> {
        self.request(&RpcCommand::blocks_info(blocks)).await
    }

    pub async fn successors(&self, args: impl Into<ChainArgs>) -> Result<BlockHashesResponse> {
        self.request(&RpcCommand::Successors(args.into())).await
    }

    pub async fn chain(&self, args: ChainArgs) -> Result<BlockHashesResponse> {
        self.request(&RpcCommand::Chain(args)).await
    }

    pub async fn confirmation_active(
        &self,
        announcements: Option<u64>,
    ) -> Result<ConfirmationActiveResponse> {
        self.request(&RpcCommand::confirmation_active(announcements))
            .await
    }

    pub async fn confirmation_quorum(&self) -> Result<ConfirmationQuorumResponse> {
        self.request(&RpcCommand::confirmation_quorum()).await
    }

    pub async fn confirmation_quorum_with_details(&self) -> Result<ConfirmationQuorumResponse> {
        self.request(&RpcCommand::confirmation_quorum_with_details())
            .await
    }

    pub async fn work_validate(
        &self,
        args: impl Into<WorkValidateArgs>,
    ) -> Result<WorkValidateResponse> {
        self.request(&RpcCommand::work_validate(args)).await
    }

    pub async fn account_info(
        &self,
        args: impl Into<AccountInfoArgs>,
    ) -> Result<AccountInfoResponse> {
        self.request(&RpcCommand::account_info(args.into())).await
    }

    pub async fn receive(&self, args: ReceiveArgs) -> Result<BlockDto> {
        self.request(&RpcCommand::Receive(args)).await
    }

    pub async fn send(&self, args: SendArgs) -> Result<BlockDto> {
        self.request(&RpcCommand::send(args)).await
    }

    pub async fn send_receive(
        &self,
        wallet: WalletId,
        source: Account,
        destination: Account,
        amount: Amount,
    ) -> Result<()> {
        let send_args = SendArgs {
            wallet,
            source,
            destination,
            amount,
            ..Default::default()
        };
        let block = self.send(send_args).await?;
        let receive_args = ReceiveArgs::builder(wallet, destination, block.block).build();
        self.receive(receive_args).await?;
        Ok(())
    }

    pub async fn keepalive(
        &self,
        address: impl Into<String>,
        port: u16,
    ) -> Result<StartedResponse> {
        self.request(&RpcCommand::keepalive(address, port)).await
    }

    pub async fn key_create(&self) -> Result<KeyPairDto> {
        self.request(&RpcCommand::KeyCreate).await
    }

    pub async fn wallet_add(&self, args: WalletAddArgs) -> Result<AccountResponse> {
        self.request(&RpcCommand::wallet_add(args)).await
    }

    pub async fn stop_rpc(&self) -> Result<()> {
        self.request_raw(&RpcCommand::Stop).await?;
        Ok(())
    }

    pub async fn stats(&self, stats_type: StatsType) -> Result<serde_json::Value> {
        self.request_raw(&RpcCommand::Stats(StatsArgs { stats_type }))
            .await
    }

    pub async fn version(&self) -> Result<VersionResponse> {
        self.request(&RpcCommand::Version).await
    }

    async fn request<T, R>(&self, cmd: &T) -> Result<R>
    where
        T: Serialize,
        R: serde::de::DeserializeOwned,
    {
        let value = self.request_raw(cmd).await?;
        let result = serde_json::from_value::<R>(value)?;
        Ok(result)
    }

    async fn request_raw<T>(&self, request: &T) -> Result<serde_json::Value>
    where
        T: Serialize,
    {
        let result = self
            .client
            .post(self.url.clone())
            .json(request)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;

        check_error(&result).map_err(|e| anyhow!("node returned error: \"{}\"", e))?;
        Ok(result)
    }
}
