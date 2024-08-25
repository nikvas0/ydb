#include "kqp_write_actor.h"

#include "kqp_write_table.h"

#include <util/generic/singleton.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/data_events/shards_splitter.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>


namespace {
    constexpr i64 kInFlightMemoryLimitPerActor = 64_MB;
    constexpr i64 kMemoryLimitPerMessage = 64_MB;
    constexpr i64 kMaxBatchesPerMessage = 64;
    constexpr i64 kMaxForwardedSize = 8_MB;

    struct TWriteActorBackoffSettings {
        TDuration StartRetryDelay = TDuration::MilliSeconds(250);
        TDuration MaxRetryDelay = TDuration::Seconds(5);
        double UnsertaintyRatio = 0.5;
        double Multiplier = 2.0;

        ui64 MaxWriteAttempts = 32;
        ui64 MaxResolveAttempts = 5;
    };

    const TWriteActorBackoffSettings* BackoffSettings() {
        return Singleton<TWriteActorBackoffSettings>();
    }

    TDuration CalculateNextAttemptDelay(ui64 attempt) {
        auto delay = BackoffSettings()->StartRetryDelay;
        for (ui64 index = 0; index < attempt; ++index) {
            delay *= BackoffSettings()->Multiplier;
        }

        delay *= 1 + BackoffSettings()->UnsertaintyRatio * (1 - 2 * RandomNumber<double>());
        delay = Min(delay, BackoffSettings()->MaxRetryDelay);

        return delay;
    }

    struct TLockInfo {
        bool AddAndCheckLock(const NKikimrDataEvents::TLock& lock) {
            if (!Lock) {
                Lock = lock;
                return true;
            } else {
                return lock.GetLockId() == Lock->GetLockId()
                    && lock.GetDataShard() == Lock->GetDataShard()
                    && lock.GetSchemeShard() == Lock->GetSchemeShard()
                    && lock.GetPathId() == Lock->GetPathId()
                    && lock.GetGeneration() == Lock->GetGeneration()
                    && lock.GetCounter() == Lock->GetCounter();
            }
        }

        const std::optional<NKikimrDataEvents::TLock>& GetLock() const {
            return Lock;
        }

    private:
        std::optional<NKikimrDataEvents::TLock> Lock;
    };

    class TLocksManager {
    public:
        void AddLock(ui64 shardId, const NKikimrDataEvents::TLock& lock) {
            Locks[shardId].AddAndCheckLock(lock);
        }

        const std::optional<NKikimrDataEvents::TLock>& GetLock(ui64 shardId) {
            return Locks[shardId].GetLock();
        }

        const THashMap<ui64, TLockInfo>& GetLocks() const {
            return Locks;
        }

    private:
        THashMap<ui64, TLockInfo> Locks;
    };

    NKikimrDataEvents::TEvWrite::TOperation::EOperationType GetOperation(NKikimrKqp::TKqpTableSinkSettings::EType type) {
        switch (type) {
        case NKikimrKqp::TKqpTableSinkSettings::MODE_REPLACE:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE;
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT;
        case NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT;
        case NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE;
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE;
        default:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UNSPECIFIED;
        }
    }
}


namespace NKikimr {
namespace NKqp {

struct IKqpTableWriterCallbacks {
    virtual ~IKqpTableWriterCallbacks() = default;

    virtual void OnReady(const TTableId& tableId) = 0;

    virtual void OnPrepared(TPreparedInfo&& preparedInfo) = 0;

    //virtual void OnCommitted(ui64 shardId) = 0;

    virtual void OnMessageAcknowledged(ui64 shardId, ui64 dataSize, bool isShardEmpty) = 0;

    virtual void OnError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) = 0;
};

class TKqpTableWriteActor : public TActorBootstrapped<TKqpTableWriteActor> {
    using TBase = TActorBootstrapped<TKqpTableWriteActor>;

    struct TEvPrivate {
        enum EEv {
            EvShardRequestTimeout = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvResolveRequestPlanned,
            EvTerminate,
        };

        struct TEvShardRequestTimeout : public TEventLocal<TEvShardRequestTimeout, EvShardRequestTimeout> {
            ui64 ShardId;

            TEvShardRequestTimeout(ui64 shardId)
                : ShardId(shardId) {
            }
        };

        struct TEvResolveRequestPlanned : public TEventLocal<TEvResolveRequestPlanned, EvResolveRequestPlanned> {
        };

        struct TEvTerminate : public TEventLocal<TEvTerminate, EvTerminate> {
        };
    };

    enum class EMode {
        UNSPECIFIED,
        FLUSH,
        PREPARE,
        COMMIT,
        IMMEDIATE_COMMIT,
    };

public:
    TKqpTableWriteActor(
        IKqpTableWriterCallbacks* callbacks,
        const TTableId& tableId,
        const TStringBuf tablePath,
        const ui64 lockTxId,
        const ui64 lockNodeId,
        const bool inconsistentTx,
        const NMiniKQL::TTypeEnvironment& typeEnv,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc)
        : TypeEnv(typeEnv)
        , Alloc(alloc)
        , TableId(tableId)
        , TablePath(tablePath)
        , LockTxId(lockTxId)
        , LockNodeId(lockNodeId)
        , InconsistentTx(inconsistentTx)
        , Callbacks(callbacks)
    {
        try {
            ShardedWriteController = CreateShardedWriteController(
                TShardedWriteControllerSettings {
                    .MemoryLimitTotal = kInFlightMemoryLimitPerActor,
                    .MemoryLimitPerMessage = kMemoryLimitPerMessage,
                    .MaxBatchesPerMessage = (SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable
                        ? 1
                        : kMaxBatchesPerMessage),
                },
                TypeEnv,
                Alloc);
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        ResolveTable();
        Become(&TKqpTableWriteActor::StateProcessing);
    }

    static constexpr char ActorName[] = "KQP_TABLE_WRITE_ACTOR";

    i64 GetMemory() const {
        return IsReady()
            ? ShardedWriteController->GetMemory()
            : 0;
    }

    bool IsReady() const {
        return ShardedWriteController->IsReady();
    }

    const THashMap<ui64, TLockInfo>& GetLocks() const {
        return LocksManager.GetLocks();
    }

    TVector<ui64> GetShardsIds() const {
        return (!ShardedWriteController)
            ? TVector<ui64>()
            : ShardedWriteController->GetShardsIds();
    }

    std::optional<size_t> GetShardsCount() const {
        return (InconsistentTx || !ShardedWriteController)
            ? std::nullopt
            : std::optional<size_t>(ShardedWriteController->GetShardsCount());
    }

    // void Commit(bool immediate) {}

    using TWriteToken = IShardedWriteController::TWriteToken;

    TWriteToken Open(
        NKikimrDataEvents::TEvWrite::TOperation::EOperationType operationType,
        TVector<NKikimrKqp::TKqpColumnMetadataProto>&& columnsMetadata) {
        YQL_ENSURE(!Closed);
        auto token = ShardedWriteController->Open(
            TableId,
            operationType,
            std::move(columnsMetadata));
        return token;
    }

    void Write(TWriteToken token, const NMiniKQL::TUnboxedValueBatch& data) {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        try {
            ShardedWriteController->Write(token, data);
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void Close(TWriteToken token) {
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        try {
            ShardedWriteController->Close(token);
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void Close() {
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        YQL_ENSURE(ShardedWriteController->IsAllWritesClosed());
        Closed = true;
        ShardedWriteController->Close();
    }

    bool IsClosed() const {
        return Closed;
    }

    bool IsFinished() const {
        return IsClosed() && ShardedWriteController->IsAllWritesFinished();
    }

    STFUNC(StateProcessing) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvPrivate::TEvShardRequestTimeout, Handle);
                hFunc(TEvPrivate::TEvTerminate, Handle);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
            }
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    STFUNC(StateTerminating) {
        Y_UNUSED(ev);
    }

    bool IsResolving() const {
        return ResolveAttempts > 0;
    }

    void RetryResolveTable() {
        if (!IsResolving()) {
            ResolveTable();
        }
    }

    void PlanResolveTable() {
        TlsActivationContext->Schedule(
            CalculateNextAttemptDelay(ResolveAttempts),
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvResolveRequestPlanned{}, 0, 0));   
    }

    void ResolveTable() {
        if (ResolveAttempts++ >= BackoffSettings()->MaxResolveAttempts) {
            const auto error = TStringBuilder()
                << "Too many table resolve attempts for Sink=" << this->SelfId() << ".";
            CA_LOG_E(error);
            RuntimeError(
                error,
                NYql::NDqProto::StatusIds::SCHEME_ERROR);
            return;
        }

        CA_LOG_D("Resolve TableId=" << TableId);
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.TableId = TableId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.SyncVersion = false;
        request->ResultSet.emplace_back(entry);

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(TableId, {}));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ev->Get()->Request->ErrorCount > 0) {
            CA_LOG_E(TStringBuilder() << "Failed to get table: "
                << TableId << "'");
            PlanResolveTable();
            return;
        }
        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1);
        SchemeEntry = resultSet[0];

        CA_LOG_D("Resolved TableId=" << TableId << " ("
            << SchemeEntry->TableId.PathId.ToString() << " "
            << SchemeEntry->TableId.SchemaVersion << ")");

        if (SchemeEntry->TableId.SchemaVersion != TableId.SchemaVersion) {
            RuntimeError(TStringBuilder() << "Schema was updated.", NYql::NDqProto::StatusIds::SCHEME_ERROR);
            return;
        }

        if (SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
            Prepare();
        } else {
            ResolveShards();
        }
    }

    void ResolveShards() {
        YQL_ENSURE(!SchemeRequest || InconsistentTx);
        YQL_ENSURE(SchemeEntry);
        CA_LOG_D("Resolve shards for TableId=" << TableId);

        TVector<TKeyDesc::TColumnOp> columns;
        TVector<NScheme::TTypeInfo> keyColumnTypes;
        for (const auto& [_, column] : SchemeEntry->Columns) {
            TKeyDesc::TColumnOp op = { column.Id, TKeyDesc::EColumnOperation::Set, column.PType, 0, 0 };
            columns.push_back(op);

            if (column.KeyOrder >= 0) {
                keyColumnTypes.resize(Max<size_t>(keyColumnTypes.size(), column.KeyOrder + 1));
                keyColumnTypes[column.KeyOrder] = column.PType;
            }
        }

        const TVector<TCell> minKey(keyColumnTypes.size());
        const TTableRange range(minKey, true, {}, false, false);
        YQL_ENSURE(range.IsFullRange(keyColumnTypes.size()));
        auto keyRange = MakeHolder<TKeyDesc>(SchemeEntry->TableId, range, TKeyDesc::ERowOperation::Update, keyColumnTypes, columns);

        TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());
        request->ResultSet.emplace_back(std::move(keyRange));

        TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq(new TEvTxProxySchemeCache::TEvResolveKeySet(request));
        Send(MakeSchemeCacheID(), resolveReq.Release(), 0, 0);
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        YQL_ENSURE(!SchemeRequest || InconsistentTx);
        auto* request = ev->Get()->Request.Get();

        if (request->ErrorCount > 0) {
            CA_LOG_E(TStringBuilder() << "Failed to get table: "
                << TableId << "'");
            PlanResolveTable();
            return;
        }

        YQL_ENSURE(request->ResultSet.size() == 1);
        SchemeRequest = std::move(request->ResultSet[0]);

        CA_LOG_D("Resolved shards for TableId=" << TableId << ". PartitionsCount=" << SchemeRequest->KeyDescription->GetPartitions().size() << ".");

        Prepare();
    }

    void Handle(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        auto getIssues = [&ev]() {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
            return issues;
        };

        switch (ev->Get()->GetStatus()) {
        case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED: {
            CA_LOG_E("Got UNSPECIFIED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Got UNSPECIFIED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::UNSPECIFIED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED: {
            const auto& result = ev->Get()->Record;
            TPreparedInfo preparedInfo;
            preparedInfo.ShardId = result.GetOrigin();
            preparedInfo.MinStep = result.GetMinStep();
            preparedInfo.MaxStep = result.GetMaxStep();
            preparedInfo.Coordinators = TVector<ui64>(result.GetDomainCoordinators().begin(),
                                                                  result.GetDomainCoordinators().end());
            Callbacks->OnPrepared(std::move(preparedInfo));
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED: {
            ProcessWriteCompletedShard(ev);
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED: {
            CA_LOG_E("Got ABORTED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Got ABORTED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::ABORTED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR: {
            CA_LOG_E("Got INTERNAL ERROR for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            
            // TODO: Add new status for splits in datashard. This is tmp solution.
            if (getIssues().ToOneLineString().Contains("in a pre/offline state assuming this is due to a finished split (wrong shard state)")) {
                ResetShardRetries(ev->Get()->Record.GetOrigin(), ev->Cookie);
                RetryResolveTable();
            } else {
                RuntimeError(
                    TStringBuilder() << "Got INTERNAL ERROR for table `"
                        << SchemeEntry->TableId.PathId.ToString() << "`.",
                    NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                    getIssues());
            }
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED: {
            CA_LOG_W("Got OVERLOADED for table `"
                << SchemeEntry->TableId.PathId.ToString() << "`."
                << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                << " Sink=" << this->SelfId() << "."
                << " Ignored this error."
                << getIssues().ToOneLineString());
            // TODO: support waiting
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED: {
            CA_LOG_E("Got CANCELLED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Got CANCELLED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::CANCELLED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST: {
            CA_LOG_E("Got BAD REQUEST for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Got BAD REQUEST for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::BAD_REQUEST,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED: {
            CA_LOG_E("Got SCHEME CHANGED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            if (InconsistentTx) {
                ResetShardRetries(ev->Get()->Record.GetOrigin(), ev->Cookie);
                RetryResolveTable();
            } else {
                RuntimeError(
                    TStringBuilder() << "Got SCHEME CHANGED for table `"
                        << SchemeEntry->TableId.PathId.ToString() << "`.",
                    NYql::NDqProto::StatusIds::SCHEME_ERROR,
                    getIssues());
            }
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN: {
            CA_LOG_E("Got LOCKS BROKEN for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Got LOCKS BROKEN for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::ABORTED,
                getIssues());
            return;
        }
        }
    }

    void ProcessWriteCompletedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        CA_LOG_D("Got completed result TxId=" << ev->Get()->Record.GetTxId()
            << ", TabletId=" << ev->Get()->Record.GetOrigin()
            << ", Cookie=" << ev->Cookie
            << ", LocksCount=" << ev->Get()->Record.GetTxLocks().size());

        for (const auto& lock : ev->Get()->Record.GetTxLocks()) {
            LocksManager.AddLock(ev->Get()->Record.GetOrigin(), lock);
        }

        if (Mode == EMode::COMMIT) {
            Callbacks->OnMessageAcknowledged(ev->Get()->Record.GetOrigin(), 0, true);
        } else {
            const auto result = ShardedWriteController->OnMessageAcknowledged(
                ev->Get()->Record.GetOrigin(), ev->Cookie);
            if (result) {
                Callbacks->OnMessageAcknowledged(ev->Get()->Record.GetOrigin(), result->DataSize, result->IsShardEmpty);
            }
        }
    }

    void SetPrepare(ui64 txId) {
        Mode = EMode::PREPARE;
        TxId = txId;
        for (const auto shardId : ShardedWriteController->GetShardsIds()) {
            const auto metadata = ShardedWriteController->GetMessageMetadata(shardId);
            if (!metadata || (metadata->IsLast && metadata->SendAttempts != 0)) {
                SendEmptyFinalToShard(shardId);
            }
        }
    }

    void SetCommit() {
        Mode = EMode::COMMIT;
    }

    void SetImmediateCommit(ui64 txId) {
        Mode = EMode::IMMEDIATE_COMMIT;
        TxId = txId;
        // TODO: send data for empty
    }

    void Flush() {
        //Mode = EMode::FLUSH;
        for (const size_t shardId : ShardedWriteController->GetPendingShards()) {
            SendDataToShard(shardId);
        }
    }

    void SendEmptyFinalToShard(const ui64 shardId) {
        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(
            NKikimrDataEvents::TEvWrite::MODE_PREPARE);
        evWrite->SetTxId(TxId);
        evWrite->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
        Send(
            PipeCacheId,
            new TEvPipeCache::TEvForward(evWrite.release(), shardId, true),
            0,
            0);
    }

    void SendDataToShard(const ui64 shardId) {
        const auto metadata = ShardedWriteController->GetMessageMetadata(shardId);
        YQL_ENSURE(metadata);
        if (metadata->SendAttempts >= BackoffSettings()->MaxWriteAttempts) {
            CA_LOG_E("ShardId=" << shardId
                    << " for table '" << TablePath
                    << "': retry limit exceeded."
                    << " Sink=" << this->SelfId() << ".");
            RuntimeError(
                TStringBuilder()
                    << "ShardId=" << shardId
                    << " for table '" << TablePath
                    << "': retry limit exceeded.",
                NYql::NDqProto::StatusIds::UNAVAILABLE);
            return;
        }

        const bool isPrepare = metadata->IsFinal && Mode == EMode::PREPARE;
        const bool isImmediateCommit = metadata->IsFinal && Mode == EMode::IMMEDIATE_COMMIT;

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>();

        evWrite->Record.SetTxMode(isPrepare
            ? NKikimrDataEvents::TEvWrite::MODE_PREPARE
            : NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        
        if (Closed && isImmediateCommit) {
            evWrite->Record.SetTxId(TxId);
            const auto lock = LocksManager.GetLock(shardId);
                // multi immediate evwrite
            auto* locks = evWrite->Record.MutableLocks();
            locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            //locks->AddSendingShards(shardId); // TODO: other shards
            //locks->AddReceivingShards(shardId);
            if (lock) {
                *locks->AddLocks() = *lock;
            }
        } else if (Closed && isPrepare) {
            evWrite->Record.SetTxId(TxId);
            // NOT TRUE:: // Last immediate write (only for datashard)
            const auto lock = LocksManager.GetLock(shardId);
                // multi immediate evwrite
            auto* locks = evWrite->Record.MutableLocks();
            locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            //locks->AddSendingShards(shardId); // TODO: other shards
            locks->AddReceivingShards(shardId);
            if (lock) {
                *locks->AddLocks() = *lock;
            }
        } else if (!InconsistentTx) {
            evWrite->SetLockId(LockTxId, LockNodeId);
        }

        const auto serializationResult = ShardedWriteController->SerializeMessageToPayload(shardId, *evWrite);
        YQL_ENSURE(serializationResult.TotalDataSize > 0);

        CA_LOG_D("Send EvWrite to ShardID=" << shardId << ", isPrepare=" << isPrepare << ", isImmediateCommit=" << isImmediateCommit << ", TxId=" << evWrite->Record.GetTxId()
            << ", LockTxId=" << evWrite->Record.GetLockTxId() << ", LockNodeId=" << evWrite->Record.GetLockNodeId()
            << ", Size=" << serializationResult.TotalDataSize << ", Cookie=" << metadata->Cookie
            << ", Operations=" << metadata->OperationsCount << ", IsFinal=" << metadata->IsFinal
            << ", Attempts=" << metadata->SendAttempts);
        Send(
            PipeCacheId,
            new TEvPipeCache::TEvForward(evWrite.release(), shardId, true),
            0,
            metadata->Cookie);

        ShardedWriteController->OnMessageSent(shardId, metadata->Cookie);

        if (InconsistentTx) {
            TlsActivationContext->Schedule(
                CalculateNextAttemptDelay(metadata->SendAttempts),
                new IEventHandle(
                    SelfId(),
                    SelfId(),
                    new TEvPrivate::TEvShardRequestTimeout(shardId),
                    0,
                    metadata->Cookie));
        }
    }

    void RetryShard(const ui64 shardId, const std::optional<ui64> ifCookieEqual) {
        const auto metadata = ShardedWriteController->GetMessageMetadata(shardId);
        if (!metadata || (ifCookieEqual && metadata->Cookie != ifCookieEqual)) {
            CA_LOG_D("Retry failed: not found ShardID=" << shardId << " with Cookie=" << ifCookieEqual.value_or(0));
            return;
        }

        CA_LOG_D("Retry ShardID=" << shardId << " with Cookie=" << ifCookieEqual.value_or(0));
        SendDataToShard(shardId);
    }

    void ResetShardRetries(const ui64 shardId, const ui64 cookie) {
        ShardedWriteController->ResetRetries(shardId, cookie);
    }

    void Handle(TEvPrivate::TEvShardRequestTimeout::TPtr& ev) {
        CA_LOG_W("Timeout shardID=" << ev->Get()->ShardId);
        YQL_ENSURE(InconsistentTx);
        RetryShard(ev->Get()->ShardId, ev->Cookie);
    }

    void Handle(TEvPrivate::TEvTerminate::TPtr&) {
        Become(&TKqpTableWriteActor::StateTerminating);
        PassAway();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_W("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
        if (InconsistentTx) {
            RetryShard(ev->Get()->TabletId, std::nullopt);
        } else {
            RuntimeError(
                TStringBuilder() << "Error while delivering message to tablet " << ev->Get()->TabletId,
                NYql::NDqProto::StatusIds::UNAVAILABLE);
        }
    }

    void Prepare() {
        YQL_ENSURE(SchemeEntry);
        ResolveAttempts = 0;

        try {
            if (SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
                ShardedWriteController->OnPartitioningChanged(std::move(*SchemeEntry));
            } else {
                ShardedWriteController->OnPartitioningChanged(std::move(*SchemeEntry), std::move(*SchemeRequest));
            }
            SchemeEntry.reset();
            SchemeRequest.reset();
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }

        Callbacks->OnReady(TableId);
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        Callbacks->OnError(message, statusCode, subIssues);
    }

    void PassAway() override {;
        Send(PipeCacheId, new TEvPipeCache::TEvUnlink(0));
        TActorBootstrapped<TKqpTableWriteActor>::PassAway();
    }

    void Terminate() {
        Send(this->SelfId(), new TEvPrivate::TEvTerminate{});
    }

    NActors::TActorId PipeCacheId = NKikimr::MakePipePerNodeCacheID(false);

    TString LogPrefix;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    ui64 TxId = 0;
    const TTableId TableId;
    const TString TablePath;

    const ui64 LockTxId;
    const ui64 LockNodeId;
    const bool InconsistentTx;

    IKqpTableWriterCallbacks* Callbacks;

    std::optional<NSchemeCache::TSchemeCacheNavigate::TEntry> SchemeEntry;
    std::optional<NSchemeCache::TSchemeCacheRequest::TEntry> SchemeRequest;
    ui64 ResolveAttempts = 0;

    TLocksManager LocksManager;
    bool Closed = false;
    EMode Mode = EMode::UNSPECIFIED;

    IShardedWriteControllerPtr ShardedWriteController = nullptr;
};

class TKqpDirectWriteActor : public TActorBootstrapped<TKqpDirectWriteActor>, public NYql::NDq::IDqComputeActorAsyncOutput, public IKqpTableWriterCallbacks {
    using TBase = TActorBootstrapped<TKqpDirectWriteActor>;

public:
    TKqpDirectWriteActor(
        NKikimrKqp::TKqpTableSinkSettings&& settings,
        NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args,
        TIntrusivePtr<TKqpCounters> counters)
        : LogPrefix(TStringBuilder() << "TxId: " << args.TxId << ", task: " << args.TaskId << ". ")
        , Settings(std::move(settings))
        , OutputIndex(args.OutputIndex)
        , Callbacks(args.Callback)
        , Counters(counters)
        , TypeEnv(args.TypeEnv)
        , Alloc(args.Alloc)
        , TxId(std::get<ui64>(args.TxId))
        , TableId(
            Settings.GetTable().GetOwnerId(),
            Settings.GetTable().GetTableId(),
            Settings.GetTable().GetVersion())
    {
        EgressStats.Level = args.StatsLevel;
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;

        WriteTableActor = new TKqpTableWriteActor(
            this,
            TableId,
            Settings.GetTable().GetPath(),
            Settings.GetLockTxId(),
            Settings.GetLockNodeId(),
            Settings.GetInconsistentTx(),
            TypeEnv,
            Alloc);

        WriteTableActorId = RegisterWithSameMailbox(WriteTableActor);

        TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata;
        columnsMetadata.reserve(Settings.GetColumns().size());
        for (const auto & column : Settings.GetColumns()) {
            columnsMetadata.push_back(column);
        }
        WriteToken = WriteTableActor->Open(GetOperation(Settings.GetType()), std::move(columnsMetadata));
        WaitingForTableActor = true;
    }

    static constexpr char ActorName[] = "KQP_DIRECT_WRITE_ACTOR";

private:
    virtual ~TKqpDirectWriteActor() {
    }

    void CommitState(const NYql::NDqProto::TCheckpoint&) final {};
    void LoadState(const NYql::NDq::TSinkState&) final {};

    ui64 GetOutputIndex() const final {
        return OutputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetEgressStats() const final {
        return EgressStats;
    }

    i64 GetFreeSpace() const final {
        return (WriteTableActor && WriteTableActor->IsReady())
            ? MemoryLimit - GetMemory()
            : std::numeric_limits<i64>::min(); // Can't use zero here because compute can use overcommit!
    }

    i64 GetMemory() const {
        return (WriteTableActor && WriteTableActor->IsReady())
            ? WriteTableActor->GetMemory()
            : 0;
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        NKikimrKqp::TEvKqpOutputActorResultInfo resultInfo;
        for (const auto& [_, lockInfo] : WriteTableActor->GetLocks()) {
            if (const auto lock = lockInfo.GetLock(); lock) {
                resultInfo.AddLocks()->CopyFrom(*lock);
            }
        }
        google::protobuf::Any result;
        result.PackFrom(resultInfo);
        return result;
    }

    void SendData(NMiniKQL::TUnboxedValueBatch&& data, i64 size, const TMaybe<NYql::NDqProto::TCheckpoint>&, bool finished) final {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        YQL_ENSURE(!Closed);
        Closed = finished;
        EgressStats.Resume();
        Y_UNUSED(size);

        WriteTableActor->Write(*WriteToken, data);
        if (Closed) {
            WriteTableActor->Close(*WriteToken);
            WriteTableActor->Close();
        }
        Process();
    }

    void Process() {
        if (GetFreeSpace() <= 0) {
            WaitingForTableActor = true;
        } else if (WaitingForTableActor && GetFreeSpace() > MemoryLimit / 2) {
            ResumeExecution();
        }

        if (Closed || GetFreeSpace() <= 0) {
            WriteTableActor->Flush();
        }

        if (Closed && WriteTableActor->IsFinished()) {
            CA_LOG_D("Write actor finished");
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
        }
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

        Callbacks->OnAsyncOutputError(OutputIndex, std::move(issues), statusCode);
    }

    void PassAway() override {
        WriteTableActor->Terminate();
        TActorBootstrapped<TKqpDirectWriteActor>::PassAway();
    }

    void ResumeExecution() {
        CA_LOG_D("Resuming execution.");
        WaitingForTableActor = false;
        Callbacks->ResumeExecution();
    }

    void OnReady(const TTableId&) override {
        Process();
    }

    void OnPrepared(TPreparedInfo&&) override {
    }

    void OnMessageAcknowledged(ui64 shardId, ui64 dataSize, bool isShardEmpty) override {
        Y_UNUSED(shardId, isShardEmpty);
        EgressStats.Bytes += dataSize;
        EgressStats.Chunks++;
        EgressStats.Splits++;
        EgressStats.Resume();
        Process();
    }

    void OnError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) override {
        RuntimeError(message, statusCode, subIssues);
    }

    TString LogPrefix;
    const NKikimrKqp::TKqpTableSinkSettings Settings;
    const ui64 OutputIndex;
    NYql::NDq::TDqAsyncStats EgressStats;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks * Callbacks = nullptr;
    TIntrusivePtr<TKqpCounters> Counters;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    const ui64 TxId;
    const TTableId TableId;
    TKqpTableWriteActor* WriteTableActor = nullptr;
    TActorId WriteTableActorId;

    std::optional<TKqpTableWriteActor::TWriteToken> WriteToken;

    bool Closed = false;

    bool WaitingForTableActor = false;
    const i64 MemoryLimit = kInFlightMemoryLimitPerActor;
};


namespace {

struct TWriteToken {
    TTableId TableId;
    ui64 Cookie;

    bool IsEmpty() const {
        return !TableId;
    }
};

struct TTransactionSettings {
    ui64 TxId = 0;
    ui64 LockTxId = 0;
    ui64 LockNodeId = 0;
    bool InconsistentTx = false;
};

struct TWriteSettings {
    TTableId TableId;
    TString TablePath; // for error messages
    NKikimrDataEvents::TEvWrite::TOperation::EOperationType OperationType;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> Columns;
    TTransactionSettings TransactionSettings;
};

struct TBufferWriteMessage {
    TActorId From;
    TWriteToken Token;
    bool Close = false;
    std::shared_ptr<TVector<NMiniKQL::TUnboxedValueBatch>> Data;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
};

struct TEvBufferWrite : public TEventLocal<TEvBufferWrite, TKqpEvents::EvBufferWrite> {
    bool Close = false;
    std::optional<TWriteToken> Token;
    std::optional<TWriteSettings> Settings;
    std::shared_ptr<TVector<NMiniKQL::TUnboxedValueBatch>> Data;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
};

struct TEvBufferWriteResult : public TEventLocal<TEvBufferWriteResult, TKqpEvents::EvBufferWriteResult> {
    TWriteToken Token;
};

}


class TKqpBufferWriteActor :public TActorBootstrapped<TKqpBufferWriteActor>, public IKqpWriteBuffer, public IKqpTableWriterCallbacks {
    using TBase = TActorBootstrapped<TKqpBufferWriteActor>;

public:
    struct TEvPrivate {
        enum EEv {
            EvTerminate = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        };

        struct TEvTerminate : public TEventLocal<TEvTerminate, EvTerminate> {
        };
    };

    enum class EState {
        WAITING, // Out of memory, wait for free memory. Can't accept any writes in this state.
        WRITING, // Allow to write data to buffer (there is free memory).
        FLUSHING, // Force flush (for uncommitted changes visibility). Can't accept any writes in this state.
        PREPARING, // Do preparation for commit. All writers are closed. New writes wouldn't be accepted.
        COMMITTING, // Do immediate commit (single shard). All writers are closed. New writes wouldn't be accepted.
        ROLLINGBACK, // Do rollback. New writes wouldn't be accepted.
        FINISHED,
    };

public:
    TKqpBufferWriteActor(
        TKqpBufferWriterSettings&& settings)
        : Settings(std::move(settings))
        , Alloc(std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__))
        , TypeEnv(*Alloc)
    {
        Alloc->Release();
        State = EState::WRITING;
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        Become(&TKqpBufferWriteActor::StateFuncBuf);
    }

    static constexpr char ActorName[] = "KQP_BUFFER_WRITE_ACTOR";

    STFUNC(StateFuncBuf) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvTerminate, Handle);
                hFunc(TEvBufferWrite, Handle);
            default:
                AFL_ENSURE(false)("unknown message", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            ReplyErrorAndDie(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR, {});
        }
    }

    void Handle(TEvBufferWrite::TPtr& ev) {
        TWriteToken token;
        if (!ev->Get()->Token) {
            AFL_ENSURE(ev->Get()->Settings);
            token = Open(std::move(*ev->Get()->Settings));
        } else {
            token = *ev->Get()->Token;
        }
        
        auto& queue = DataQueues[token.TableId];
        queue.emplace_back();
        auto& message = queue.back();

        message.Token = token;
        message.From = ev->Sender;
        message.Close = ev->Get()->Close;
        message.Data = ev->Get()->Data;
        message.Alloc = ev->Get()->Alloc;

        if (HasWrites) {
            AFL_ENSURE(LockTxId == ev->Get()->Settings->TransactionSettings.LockTxId);
            AFL_ENSURE(LockNodeId == ev->Get()->Settings->TransactionSettings.LockNodeId);
            AFL_ENSURE(InconsistentTx == ev->Get()->Settings->TransactionSettings.InconsistentTx);
        } else {
            LockTxId = ev->Get()->Settings->TransactionSettings.LockTxId;
            LockNodeId = ev->Get()->Settings->TransactionSettings.LockNodeId;
            InconsistentTx = ev->Get()->Settings->TransactionSettings.InconsistentTx;
            HasWrites = true;
        }
        
        ProcessQueue(token.TableId);
    }

    void ProcessQueue(const TTableId& tableId) {
        auto& queue = DataQueues.at(tableId);
        auto& writeInfo = WriteInfos.at(tableId);

        if (!writeInfo.WriteTableActor->IsReady()) {
            return;
        }

        while (!queue.empty()) {
            auto& message = queue.front();

            if (!message.Data->empty()) {
                for (const auto& data : *message.Data) {
                    Write(message.Token, data);
                }
            }
            if (message.Close) {
                Close(message.Token);
            }

            auto result = std::make_unique<TEvBufferWriteResult>();
            result->Token = message.Token;

            Send(message.From, result.release());

            {
                TGuard guard(*message.Alloc);
                message.Data = nullptr;
            }
            queue.pop_front();
        }

        Process();
    }

    TWriteToken Open(TWriteSettings&& settings) {
        YQL_ENSURE(State == EState::WRITING || State == EState::WAITING);

        auto& info = WriteInfos[settings.TableId];
        if (!info.WriteTableActor) {
            info.WriteTableActor = new TKqpTableWriteActor(
                this,
                settings.TableId,
                settings.TablePath,
                LockTxId,
                LockNodeId,
                InconsistentTx,
                TypeEnv,
                Alloc);
            info.WriteTableActorId = RegisterWithSameMailbox(info.WriteTableActor);
            State = EState::WAITING;
        }

        auto writeToken = info.WriteTableActor->Open(settings.OperationType, std::move(settings.Columns));
        return {settings.TableId, std::move(writeToken)};
    }

    void Write(TWriteToken token, const NMiniKQL::TUnboxedValueBatch& data) {
        YQL_ENSURE(State == EState::WRITING || State == EState::WAITING);

        auto& info = WriteInfos.at(token.TableId);
        info.WriteTableActor->Write(token.Cookie, data);
    }

    void Close(TWriteToken token) {
        YQL_ENSURE(State == EState::WRITING || State == EState::WAITING);

        auto& info = WriteInfos.at(token.TableId);
        info.WriteTableActor->Close(token.Cookie);
    }

    THashMap<ui64, NKikimrDataEvents::TLock> GetLocks(TWriteToken token) const {
        auto& info = WriteInfos.at(token.TableId);
        THashMap<ui64, NKikimrDataEvents::TLock> result;
        for (const auto& [shardId, lockInfo] : info.WriteTableActor->GetLocks()) {
            if (const auto lock = lockInfo.GetLock(); lock) {
                result.emplace(shardId, *lock);
            }
        }
        return result;
    }

    THashMap<ui64, NKikimrDataEvents::TLock> GetLocks() const override {
        THashMap<ui64, NKikimrDataEvents::TLock> result;
        for (const auto& [_, info] : WriteInfos) {
            for (const auto& [shardId, lockInfo] : info.WriteTableActor->GetLocks()) {
                if (const auto lock = lockInfo.GetLock(); lock) {
                    result.emplace(shardId, *lock);
                }
            }
        }
        return result;
    }

    void Flush(std::function<void()> callback) override {
        State = EState::FLUSHING;
        OnFlushedCallback = callback;
        Close();
        Process();
    }

    void Prepare(std::function<void(TPreparedInfo&& preparedInfo)> callback, TPrepareSettings&& prepareSettings) override {
        YQL_ENSURE(State == EState::WRITING);
        Y_UNUSED(callback, prepareSettings);
        State = EState::PREPARING;
        OnPreparedCallback = std::move(callback);
        for (auto& [_, info] : WriteInfos) {
            info.WriteTableActor->SetPrepare(prepareSettings.TxId);
        }
        Close();
        Process();
    }

    void OnCommit(std::function<void(ui64)> callback) override {
        YQL_ENSURE(State == EState::PREPARING);
        State = EState::COMMITTING;
        OnCommitCallback = std::move(callback);
        for (auto& [_, info] : WriteInfos) {
            info.WriteTableActor->SetCommit();
        }
    }

    void ImmediateCommit(std::function<void(ui64)> callback, ui64 txId) override {
        YQL_ENSURE(State == EState::WRITING);
        State = EState::COMMITTING;
        OnCommitCallback = std::move(callback);
        for (auto& [_, info] : WriteInfos) {
            info.WriteTableActor->SetImmediateCommit(txId);
        }
        Close();
        Process();
    }

    void Close() {
        for (auto& [_, info] : WriteInfos) {
            if (!info.WriteTableActor->IsClosed()) {
                info.WriteTableActor->Close();
            }
        }
    }

    bool IsFinished() const override {
        return State == EState::FINISHED;
    }

    i64 GetFreeSpace(TWriteToken token) const {
        auto& info = WriteInfos.at(token.TableId);
        return info.WriteTableActor->IsReady()
            ? MemoryLimit - info.WriteTableActor->GetMemory()
            : std::numeric_limits<i64>::min(); // Can't use zero here because compute can use overcommit!
    }

    i64 GetTotalFreeSpace() const {
        return MemoryLimit - GetTotalMemory();
    }

    i64 GetTotalMemory() const {
        i64 totalMemory = 0;
        for (auto& [_, info] : WriteInfos) {
            totalMemory += info.WriteTableActor->IsReady()
                ? info.WriteTableActor->GetMemory()
                : 0;
        }
        return totalMemory;
    }

    THashSet<ui64> GetShardsIds() const override {
        THashSet<ui64> shardIds;
        for (auto& [_, info] : WriteInfos) {
            for (const auto& id : info.WriteTableActor->GetShardsIds()) {
                shardIds.insert(id);
            }
        }
        return shardIds;
    }

    void PassAway() override {
        for (auto& [_, queue] : DataQueues) {
            while (!queue.empty()) {
                auto& message = queue.front();
                {
                    TGuard guard(*message.Alloc);
                    message.Data = nullptr;
                }
                queue.pop_front();
            }
        }
        
        for (auto& [_, info] : WriteInfos) {
            if (info.WriteTableActor) {
                info.WriteTableActor->Terminate();
            }
        }
        TActorBootstrapped<TKqpBufferWriteActor>::PassAway();
    }

    void Handle(TEvPrivate::TEvTerminate::TPtr&) {
        PassAway();
    }

    void Terminate() override {
        Send(SelfId(), new TEvPrivate::TEvTerminate{});
    }

    void Process() {
        if (GetTotalFreeSpace() <= 0) {
            State = EState::WAITING;
        } else if (State == EState::WAITING && GetTotalFreeSpace() > MemoryLimit / 2) {
            ResumeExecution();
        }

        const bool needToFlush = (State == EState::WAITING
            || State == EState::FLUSHING
            || State == EState::PREPARING
            || State == EState::COMMITTING
            || State == EState::ROLLINGBACK);

        if (needToFlush) {
            for (auto& [_, info] : WriteInfos) {
                if (info.WriteTableActor->IsReady()) {
                    info.WriteTableActor->Flush();
                }
            }
        }

        bool isFinished = true;
        for (auto& [_, info] : WriteInfos) {
            isFinished &= info.WriteTableActor->IsFinished();
        }
        if (isFinished) {
            CA_LOG_D("Write actor finished");
            switch (State) {
                case EState::PREPARING:
                    //Settings.Callbacks->OnPrepared();
                    break;
                case EState::COMMITTING:
                    //Settings.Callbacks->OnCommitted();
                    break;
                case EState::ROLLINGBACK:
                    //Settings.Callbacks->OnRolledBack();
                    break;
                case EState::FLUSHING:
                    //Settings.Callbacks->OnFlushed();
                    //if (OnFlushedCallback != nullptr) {
                    YQL_ENSURE(OnFlushedCallback != nullptr);
                    OnFlushedCallback();
                    //}
                    break;
                default:
                    YQL_ENSURE(false);
            }

            State = EState::FINISHED;
        }
    }

    void ResumeExecution() {
        CA_LOG_D("Resuming execution.");
        State = EState::WRITING;
    }

    void OnReady(const TTableId& tableId) override {
        ProcessQueue(tableId);
    }

    void OnPrepared(TPreparedInfo&& preparedInfo) override {
        OnPreparedCallback(std::move(preparedInfo));
        Process();
    }

    void OnMessageAcknowledged(ui64 shardId, ui64 dataSize, bool isShardEmpty) override {
        Y_UNUSED(dataSize);
        if (State == EState::COMMITTING && isShardEmpty) {
            OnCommitCallback(shardId);
        } else {
            Process();
        }
    }

    void OnError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) override {
        ReplyErrorAndDie(message, statusCode, subIssues);
    }

    void ReplyErrorAndDie(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) {
        Y_DEBUG_ABORT_UNLESS(false);
        CA_LOG_E("Error: " << message << ". statusCode=" << statusCode << ". subIssues=" << subIssues.ToString());
        Y_UNUSED(message, statusCode, subIssues);
    }

private:
    TString LogPrefix;

    const TKqpBufferWriterSettings Settings;

    bool HasWrites = false;
    ui64 LockTxId = 0;
    ui64 LockNodeId = 0;
    bool InconsistentTx = false;

    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    NMiniKQL::TTypeEnvironment TypeEnv;

    struct TWriteInfo {
        TKqpTableWriteActor* WriteTableActor = nullptr;
        TActorId WriteTableActorId;

        THashMap<ui64, std::function<void()>> ResumeExecutionCallbacks;
    };

    THashMap<TTableId, TWriteInfo> WriteInfos;

    EState State;
    std::function<void()> OnFlushedCallback;
    std::function<void(TPreparedInfo&& preparedInfo)> OnPreparedCallback;
    std::function<void(ui64)> OnCommitCallback;

    THashMap<TTableId, std::deque<TBufferWriteMessage>> DataQueues;

    const i64 MemoryLimit = kInFlightMemoryLimitPerActor;
};

class TKqpForwardWriteActor : public TActorBootstrapped<TKqpForwardWriteActor>, public NYql::NDq::IDqComputeActorAsyncOutput {
    using TBase = TActorBootstrapped<TKqpForwardWriteActor>;

public:
    TKqpForwardWriteActor(
        NKikimrKqp::TKqpTableSinkSettings&& settings,
        NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args,
        TIntrusivePtr<TKqpCounters> counters)
        : LogPrefix(TStringBuilder() << "TxId: " << args.TxId << ", task: " << args.TaskId << ". ")
        , Settings(std::move(settings))
        , OutputIndex(args.OutputIndex)
        , Callbacks(args.Callback)
        , Counters(counters)
        , TypeEnv(args.TypeEnv)
        , Alloc(args.Alloc)
        , BufferActorId(ActorIdFromProto(Settings.GetBufferActorId()))
        , TxId(std::get<ui64>(args.TxId))
        , TableId(
            Settings.GetTable().GetOwnerId(),
            Settings.GetTable().GetTableId(),
            Settings.GetTable().GetVersion())
    {
        EgressStats.Level = args.StatsLevel;
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        Become(&TKqpForwardWriteActor::StateFuncFwd);
    }

    static constexpr char ActorName[] = "KQP_FORWARD_WRITE_ACTOR";

private:
    STFUNC(StateFuncFwd) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBufferWriteResult, Handle);
            default:
                AFL_ENSURE(false)("unknown message", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void Handle(TEvBufferWriteResult::TPtr& result) {
        WriteToken = result->Get()->Token;
        DataSize = 0;
        {
            auto alloc = TypeEnv.BindAllocator();
            Data = nullptr;
        }

        if (Closed) {
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
        }
        Callbacks->ResumeExecution();
    }

    void WriteToBuffer() {
        auto ev = std::make_unique<TEvBufferWrite>();

        ev->Data = Data;
        ev->Close = Closed;
        ev->Alloc = Alloc;

        if (!WriteToken.IsEmpty()) {
            ev->Token = WriteToken;
        } else {
            TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata;
            columnsMetadata.reserve(Settings.GetColumns().size());
            for (const auto & column : Settings.GetColumns()) {
                columnsMetadata.push_back(column);
            }

            ev->Settings = TWriteSettings{
                .TableId = TableId,
                .TablePath = Settings.GetTable().GetPath(),
                .OperationType = GetOperation(Settings.GetType()),
                .Columns = std::move(columnsMetadata),
                .TransactionSettings = TTransactionSettings{
                    .TxId = TxId,
                    .LockTxId = Settings.GetLockTxId(),
                    .LockNodeId = Settings.GetLockNodeId(),
                    .InconsistentTx = Settings.GetInconsistentTx(),
                },
            };
        }

        AFL_ENSURE(Send(BufferActorId, ev.release()));
        EgressStats.Bytes += DataSize;
        EgressStats.Chunks++;
        EgressStats.Splits++;
        EgressStats.Resume();
    }

    void CommitState(const NYql::NDqProto::TCheckpoint&) final {};
    void LoadState(const NYql::NDq::TSinkState&) final {};

    ui64 GetOutputIndex() const final {
        return OutputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetEgressStats() const final {
        return EgressStats;
    }

    i64 GetFreeSpace() const final {
        return kMaxForwardedSize - DataSize > 0
            ? kMaxForwardedSize - DataSize
            : std::numeric_limits<i64>::min();
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        return {};
    }

    void SendData(NMiniKQL::TUnboxedValueBatch&& data, i64 size, const TMaybe<NYql::NDqProto::TCheckpoint>&, bool finished) final {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        Closed |= finished;
        if (!Data) {
            Data = std::make_shared<TVector<NMiniKQL::TUnboxedValueBatch>>();
        }

        Data->emplace_back(std::move(data));
        DataSize += size;

        if (Closed || GetFreeSpace() <= 0) {
            WriteToBuffer();
        }
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

        Callbacks->OnAsyncOutputError(OutputIndex, std::move(issues), statusCode);
    }

    void PassAway() override {
        {
            auto alloc = TypeEnv.BindAllocator();
            Data = nullptr;
        }
        TActorBootstrapped<TKqpForwardWriteActor>::PassAway();
    }

    TString LogPrefix;
    const NKikimrKqp::TKqpTableSinkSettings Settings;
    const ui64 OutputIndex;
    NYql::NDq::TDqAsyncStats EgressStats;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks * Callbacks = nullptr;
    TIntrusivePtr<TKqpCounters> Counters;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    TActorId BufferActorId;

    std::shared_ptr<TVector<NMiniKQL::TUnboxedValueBatch>> Data;
    i64 DataSize = 0;
    bool Closed = false;

    const ui64 TxId;
    const TTableId TableId;

    TWriteToken WriteToken;
};

std::pair<IKqpWriteBuffer*, NActors::IActor*> CreateKqpBufferWriterActor(TKqpBufferWriterSettings&& settings) {
    auto* actor = new TKqpBufferWriteActor(std::move(settings));
    return std::make_pair<IKqpWriteBuffer*, NActors::IActor*>(actor, actor);
}


void RegisterKqpWriteActor(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterSink<NKikimrKqp::TKqpTableSinkSettings>(
        TString(NYql::KqpTableSinkName),
        [counters] (NKikimrKqp::TKqpTableSinkSettings&& settings, NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args) {
            if (!ActorIdFromProto(settings.GetBufferActorId())) {
                auto* actor = new TKqpDirectWriteActor(std::move(settings), std::move(args), counters);
                return std::make_pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*>(actor, actor);
            } else {
                auto* actor = new TKqpForwardWriteActor(std::move(settings), std::move(args), counters);
                return std::make_pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*>(actor, actor);
            }
        });
}

}
}
