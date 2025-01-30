use serde::Serialize;
use serde_variant::to_variant_name;

/// Primary statistics type
#[repr(u8)]
#[derive(FromPrimitive, Serialize, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[serde(rename_all = "snake_case")]
pub enum StatType {
    Error,
    Message,
    Block,
    Ledger,
    Rollback,
    Network,
    Vote,
    VoteProcessor,
    VoteProcessorTier,
    VoteProcessorOverfill,
    Election,
    ElectionCleanup,
    ElectionVote,
    HttpCallbacks,
    Ipc,
    Tcp,
    TcpServer,
    TcpChannels,
    TcpChannelsRejected,
    TcpListener,
    TcpListenerRejected,
    TrafficTcp,
    TrafficTcpType,
    Channel,
    Socket,
    ConfirmationHeight,
    ConfirmationObserver,
    ConfirmingSet,
    Drop,
    Aggregator,
    Requests,
    RequestAggregator,
    RequestAggregatorVote,
    RequestAggregatorReplies,
    Filter,
    Telemetry,
    VoteGenerator,
    VoteCache,
    VoteCacheProcessor,
    Hinting,
    BlockProcessor,
    BlockProcessorSource,
    BlockProcessorResult,
    BlockProcessorOverfill,
    Bootstrap,
    BootstrapVerify,
    BootstrapVerifyBlocks,
    BootstrapVerifyFrontiers,
    BootstrapProcess,
    BootstrapRequest,
    BootstrapRequestBlocks,
    BootstrapReply,
    BootstrapNext,
    BootstrapFrontiers,
    BootstrapAccountSets,
    BootstrapFrontierScan,
    BootstrapTimeout,
    BootstrapServer,
    BootstrapServerRequest,
    BootstrapServerOverfill,
    BootstrapServerResponse,
    Active,
    ActiveElections,
    ActiveElectionsStarted,
    ActiveElectionsStopped,
    ActiveElectionsConfirmed,
    ActiveElectionsDropped,
    ActiveElectionsTimeout,
    ActiveElectionsCancelled,
    ActiveElectionsCemented,
    ActiveTimeout,
    Backlog,
    BacklogScan,
    BoundedBacklog,
    Unchecked,
    ElectionScheduler,
    ElectionBucket,
    OptimisticScheduler,
    Handshake,
    RepCrawler,
    LocalBlockBroadcaster,
    RepTiers,
    SynCookies,
    PeerHistory,
    MessageProcessor,
    MessageProcessorOverfill,
    MessageProcessorType,
    ProcessConfirmed,
    Pruning,
}

impl StatType {
    pub fn as_str(&self) -> &'static str {
        to_variant_name(self).unwrap_or_default()
    }
}

// Optional detail type
#[repr(u16)]
#[derive(FromPrimitive, Serialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[serde(rename_all = "snake_case")]
pub enum DetailType {
    // common
    All = 0,
    Ok,
    Loop,
    LoopCleanup,
    Total,
    Process,
    Processed,
    Ignored,
    Update,
    Updated,
    Inserted,
    Erased,
    Request,
    RequestFailed,
    RequestSuccess,
    Broadcast,
    Cleanup,
    Top,
    None,
    Success,
    Unknown,
    Cache,
    Rebroadcast,
    QueueOverflow,
    Triggered,
    Notify,
    Duplicate,
    Confirmed,
    Unconfirmed,
    Cemented,
    Cooldown,
    Empty,
    Done,
    Retry,
    Prioritized,
    Pending,
    Sync,
    Requeued,
    Evicted,

    // processing queue
    Queue,
    Overfill,
    Batch,

    // error specific
    InsufficientWork,
    HttpCallback,
    UnreachableHost,
    InvalidNetwork,

    // confirmation_observer specific
    ActiveQuorum,
    ActiveConfHeight,
    InactiveConfHeight,

    // ledger, block, bootstrap
    Send,
    Receive,
    Open,
    Change,
    StateBlock,
    EpochBlock,
    Fork,
    Old,
    GapPrevious,
    GapSource,
    Rollback,
    RollbackFailed,
    Progress,
    BadSignature,
    NegativeSpend,
    Unreceivable,
    GapEpochOpenPending,
    OpenedBurnAccount,
    BalanceMismatch,
    RepresentativeMismatch,
    BlockPosition,

    // block processor
    ProcessBlocking,
    ProcessBlockingTimeout,
    Force,

    // block source
    Live,
    LiveOriginator,
    Bootstrap,
    BootstrapLegacy,
    Unchecked,
    Local,
    Forced,
    Election,

    // message specific
    NotAType,
    Invalid,
    Keepalive,
    Publish,
    ConfirmReq,
    ConfirmAck,
    NodeIdHandshake,
    TelemetryReq,
    TelemetryAck,
    AscPullReq,
    AscPullAck,

    // dropped messages
    ConfirmAckZeroAccount,

    // bootstrap, callback
    Initiate,
    InitiateLegacyAge,
    InitiateLazy,
    InitiateWalletLazy,

    // bootstrap specific
    BulkPull,
    BulkPullAccount,
    BulkPullErrorStartingRequest,
    BulkPullFailedAccount,
    BulkPullRequestFailure,
    BulkPush,
    FrontierReq,
    FrontierConfirmationFailed,
    ErrorSocketClose,

    // vote result
    Vote,
    Valid,
    Replay,
    Indeterminate,

    // vote processor
    VoteOverflow,
    VoteIgnored,

    // election specific
    VoteNew,
    VoteProcessed,
    VoteCached,
    ElectionBlockConflict,
    ElectionRestart,
    ElectionNotConfirmed,
    ElectionHintedOverflow,
    ElectionHintedConfirmed,
    ElectionHintedDrop,
    BroadcastVote,
    BroadcastVoteNormal,
    BroadcastVoteFinal,
    GenerateVote,
    GenerateVoteNormal,
    GenerateVoteFinal,
    BroadcastBlockInitial,
    BroadcastBlockRepeat,
    ConfirmOnce,
    ConfirmOnceFailed,
    ConfirmationRequest,

    // election types
    Manual,
    Priority,
    Hinted,
    Optimistic,

    // received messages
    InvalidHeader,
    InvalidMessageType,
    InvalidKeepaliveMessage,
    InvalidPublishMessage,
    InvalidConfirmReqMessage,
    InvalidConfirmAckMessage,
    InvalidNodeIdHandshakeMessage,
    InvalidTelemetryReqMessage,
    InvalidTelemetryAckMessage,
    InvalidBulkPullMessage,
    InvalidBulkPullAccountMessage,
    InvalidFrontierReqMessage,
    InvalidAscPullReqMessage,
    InvalidAscPullAckMessage,
    MessageSizeTooBig,
    OutdatedVersion,

    // network
    LoopKeepalive,
    LoopReachout,
    LoopReachoutCached,
    MergePeer,
    ReachoutLive,
    ReachoutCached,

    // traffic
    Generic,
    BootstrapServer,
    BootstrapRequests,
    BlockBroadcast,
    BlockBroadcastRpc,
    BlockBroadcastInitial,
    ConfirmationRequests,
    VoteRebroadcast,
    RepCrawler,
    VoteReply,
    Telemetry,

    // tcp
    TcpWriteDrop,
    TcpWriteNoSocketDrop,
    TcpSilentConnectionDrop,
    TcpIoTimeoutDrop,
    TcpConnectError,
    TcpReadError,
    TcpWriteError,

    // tcp_listener
    AcceptSuccess,
    AcceptFailure,
    AcceptRejected,
    CloseError,
    MaxPerIp,
    MaxPerSubnetwork,
    MaxAttempts,
    MaxAttemptsPerIp,
    Excluded,
    EraseDead,
    ConnectInitiate,
    ConnectFailure,
    ConnectError,
    ConnectRejected,
    ConnectSuccess,
    AttemptTimeout,
    NotAPeer,

    // tcp_channels
    ChannelAccepted,
    ChannelRejected,
    ChannelDuplicate,
    Outdated,

    // tcp_server
    Handshake,
    HandshakeAbort,
    HandshakeError,
    HandshakeNetworkError,
    HandshakeInitiate,
    HandshakeResponse,
    HandshakeResponseInvalid,

    // ipc
    Invocations,

    // confirmation height
    BlocksConfirmed,

    // request aggregator
    AggregatorAccepted,
    AggregatorDropped,

    // requests
    RequestsCachedHashes,
    RequestsGeneratedHashes,
    RequestsCachedVotes,
    RequestsGeneratedVotes,
    RequestsCannotVote,
    RequestsUnknown,
    RequestsNonFinal,
    RequestsFinal,

    // request_aggregator
    RequestHashes,
    OverfillHashes,
    NormalVote,
    FinalVote,

    // duplicate
    DuplicatePublishMessage,
    DuplicateConfirmAckMessage,

    // telemetry
    InvalidSignature,
    NodeIdMismatch,
    GenesisMismatch,
    RequestWithinProtectionCacheZone,
    NoResponseReceived,
    UnsolicitedTelemetryAck,
    FailedSendTelemetryReq,
    EmptyPayload,
    CleanupOutdated,

    // vote generator
    GeneratorBroadcasts,
    GeneratorReplies,
    GeneratorRepliesDiscarded,
    GeneratorSpacing,

    // hinting
    MissingBlock,
    DependentUnconfirmed,
    AlreadyConfirmed,
    Activate,
    ActivateImmediate,
    DependentActivated,

    // bootstrap server
    Response,
    WriteError,
    Blocks,
    ChannelFull,
    Frontiers,
    AccountInfo,

    // backlog
    Activated,
    ActivateFailed,
    ActivateSkip,
    ActivateFull,
    Scanned,

    // active
    Insert,
    InsertFailed,
    TransitionPriority,
    TransitionPriorityFailed,
    ElectionCleanup,
    ActivateImmediately,

    // active_elections
    Started,
    Stopped,
    ConfirmDependent,

    // unchecked
    Put,
    Satisfied,
    Trigger,

    // election scheduler
    InsertManual,
    InsertPriority,
    InsertPrioritySuccess,
    EraseOldest,

    // handshake
    InvalidNodeId,
    MissingCookie,
    InvalidGenesis,

    // bootstrap
    MissingTag,
    Reply,
    Throttled,
    Track,
    Timeout,
    NothingNew,
    AccountInfoEmpty,
    FrontiersEmpty,
    LoopDatabase,
    LoopDependencies,
    LoopFrontiers,
    LoopFrontiersProcessing,
    DuplicateRequest,
    InvalidResponseType,
    InvalidResponse,
    TimestampReset,
    ProcessingFrontiers,
    FrontiersDropped,
    SyncAccounts,

    Prioritize,
    PrioritizeFailed,
    Block,
    BlockFailed,
    Unblock,
    UnblockFailed,
    DependencyUpdate,
    DependencyUpdateFailed,

    DoneRange,
    DoneEmpty,
    NextByRequests,
    NextByTimestamp,
    Advance,
    AdvanceFailed,

    NextNone,
    NextDatabase,
    NextBlocking,
    NextDependency,
    NextFrontier,

    BlockingInsert,
    BlockingOverflow,
    PriorityInsert,
    PrioritySet,
    PriorityErase,
    PriorityUnblocked,
    EraseByThreshold,
    EraseByBlocking,
    PriorityEraseThreshold,
    PriorityEraseBlock,
    PriorityOverflow,
    Deprioritize,
    DeprioritizeFailed,
    SyncDependencies,
    DependencySynced,

    RequestBlocks,
    RequestAccountInfo,

    Safe,
    Base,

    // active
    StartedHinted,
    StartedOptimistic,

    // rep_crawler
    ChannelDead,
    QueryTargetFailed,
    QueryChannelBusy,
    QuerySent,
    QueryDuplicate,
    RepTimeout,
    QueryTimeout,
    QueryCompletion,
    CrawlAggressive,
    CrawlNormal,

    // block broadcaster
    BroadcastNormal,
    BroadcastAggressive,
    EraseOld,
    EraseConfirmed,

    // rep tiers
    Tier1,
    Tier2,
    Tier3,

    // confirming_set
    NotifyCemented,
    NotifyAlreadyCemented,
    NotifyIntermediate,
    AlreadyCemented,
    Cementing,
    CementedHash,
    CementingFailed,

    // election_state
    Passive,
    Active,
    ExpiredConfirmed,
    ExpiredUnconfirmed,
    Cancelled,

    // election_status_type
    Ongoing,
    ActiveConfirmedQuorum,
    ActiveConfirmationHeight,
    InactiveConfirmationHeight,

    // election bucket
    ActivateSuccess,
    CancelLowest,

    // query_type
    BlocksByHash,
    BlocksByAccount,
    AccountInfoByHash,

    // bounded backlog
    GatheredTargets,
    PerformingRollbacks,
    NoTargets,
    RollbackMissingBlock,
    RollbackSkipped,
    LoopScan,

    // pruning
    LedgerPruning,
    PruningTarget,
    PrunedCount,
    CollectTargets,
}

impl DetailType {
    pub fn as_str(&self) -> &'static str {
        to_variant_name(self).unwrap_or_default()
    }
}

/// Direction of the stat. If the direction is irrelevant, use In
#[derive(FromPrimitive, PartialEq, PartialOrd, Eq, Ord, Clone, Copy, Debug)]
#[repr(u8)]
pub enum Direction {
    In,
    Out,
}

impl Direction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Direction::In => "in",
            Direction::Out => "out",
        }
    }
}

#[repr(u8)]
#[derive(FromPrimitive, Serialize, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Sample {
    ActiveElectionDuration,
    BootstrapTagDuration,
    RepResponseTime,
    VoteGeneratorFinalHashes,
    VoteGeneratorHashes,
}

impl Sample {
    pub fn as_str(&self) -> &'static str {
        to_variant_name(self).unwrap_or_default()
    }
}
