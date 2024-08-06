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


namespace {
    constexpr i64 kInFlightMemoryLimitPerActor = 64_MB;
    constexpr i64 kMemoryLimitPerMessage = 64_MB;
    constexpr i64 kMaxBatchesPerMessage = 64;

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

        const std::optional<NKikimrDataEvents::TLock>& GetLock(ui64 shardId) const {
            return Locks.at(shardId).GetLock();
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

    virtual void OnPrepared() = 0;

    virtual void OnMessageAcknowledged(ui64 dataSize) = 0;

    virtual void OnError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) = 0;
};

class TKqpTableWriteActor : public TActorBootstrapped<TKqpTableWriteActor> {
    using TBase = TActorBootstrapped<TKqpTableWriteActor>;

    struct TEvPrivate {
        enum EEv {
            EvShardRequestTimeout = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvResolveRequestPlanned,
        };

        struct TEvShardRequestTimeout : public TEventLocal<TEvShardRequestTimeout, EvShardRequestTimeout> {
            ui64 ShardId;

            TEvShardRequestTimeout(ui64 shardId)
                : ShardId(shardId) {
            }
        };

        struct TEvResolveRequestPlanned : public TEventLocal<TEvResolveRequestPlanned, EvResolveRequestPlanned> {
        };
    };

    //enum class EState {
    //    WRITING,
    //    FLUSHING,
    //    FINISHING /* = COMMITTING || ROLLINGBACK || PREPARING */,
    //};

public:
    TKqpTableWriteActor(
        IKqpTableWriterCallbacks* callbacks,
        const TTableId& tableId,
        const TStringBuf tablePath,
        const ui64 txId,
        const ui64 lockTxId,
        const ui64 lockNodeId,
        const bool inconsistentTx,
        const NMiniKQL::TTypeEnvironment& typeEnv,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc)
        : LogPrefix(TStringBuilder() << "TxId: " << txId << ". ")
        , TypeEnv(typeEnv)
        , Alloc(alloc)
        , TxId(txId)
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
        Become(&TKqpTableWriteActor::StateFunc);
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

    void Write(TWriteToken token, NMiniKQL::TUnboxedValueBatch&& data) {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        try {
            ShardedWriteController->Write(token, std::move(data));
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

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvPrivate::TEvShardRequestTimeout, Handle);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
            }
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
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
        Cerr << " RESOLVE FIRST " << ev->Get()->Request->ErrorCount << Endl;
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

        Cerr << " RESOLVE SECOND " << request->ErrorCount << Endl;

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
            YQL_ENSURE(false);
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

        const auto removedDataSize = ShardedWriteController->OnMessageAcknowledged(
            ev->Get()->Record.GetOrigin(), ev->Cookie);
        if (removedDataSize) {
            Callbacks->OnMessageAcknowledged(*removedDataSize);
        }
    }

    void Flush() {
        for (const size_t shardId : ShardedWriteController->GetPendingShards()) {
            SendDataToShard(shardId);
        }
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

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(
            NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        
        if (false && Closed && metadata->IsFinal) {
            // Last immediate write (only for datashard)
            const auto lock = LocksManager.GetLock(shardId);
            if (lock) {
                // multi immediate evwrite
                auto* locks = evWrite->Record.MutableLocks();
                locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
                locks->AddSendingShards(shardId);
                locks->AddReceivingShards(shardId);
                *locks->AddLocks() = *lock;
            }
        } else if (!InconsistentTx) {
            evWrite->SetLockId(LockTxId, LockNodeId);
        }

        const auto serializationResult = ShardedWriteController->SerializeMessageToPayload(shardId, *evWrite);
        YQL_ENSURE(serializationResult.TotalDataSize > 0);

        CA_LOG_D("Send EvWrite to ShardID=" << shardId << ", TxId=" << TxId
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

        Callbacks->OnPrepared();
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        Callbacks->OnError(message, statusCode, subIssues);
    }

    void PassAway() override {;
        Send(PipeCacheId, new TEvPipeCache::TEvUnlink(0));
        TActorBootstrapped<TKqpTableWriteActor>::PassAway();
    }

    void Terminate() {
        // TODO: Change to message
        PassAway();
    }

    NActors::TActorId PipeCacheId = NKikimr::MakePipePerNodeCacheID(false);

    TString LogPrefix;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    const ui64 TxId;
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
        Cerr << "TEST" << Settings.GetTable().GetPath() << Endl;

        WriteTableActor = new TKqpTableWriteActor(
            this,
            TableId,
            Settings.GetTable().GetPath(),
            TxId,
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

        WriteTableActor->Write(*WriteToken, std::move(data));
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

    void OnPrepared() override {
        Process();
    }

    void OnMessageAcknowledged(ui64 dataSize) override {
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

class TKqpBufferWriteActor :public TActorBootstrapped<TKqpBufferWriteActor>, public IKqpBufferWriter, public IKqpTableWriterCallbacks {
    using TBase = TActorBootstrapped<TKqpBufferWriteActor>;

    enum class EState {
        WAITING,
        WRITING,
        FLUSHING,
        PREPARING,
        COMMITTING,
        ROLLINGBACK,
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
    }

    static constexpr char ActorName[] = "KQP_BUFFER_WRITE_ACTOR";

private:
    TWriteToken Open(TWriteSettings&& settings) override {
        YQL_ENSURE(State == EState::WRITING || State == EState::WAITING);

        auto& info = WriteInfos[settings.TableId];
        if (!info.WriteTableActor) {
            info.WriteTableActor = new TKqpTableWriteActor(
                this,
                settings.TableId,
                settings.TablePath,
                Settings.TxId,
                Settings.LockTxId,
                Settings.LockNodeId,
                Settings.InconsistentTx,
                TypeEnv,
                Alloc);

            info.WriteTableActorId = RegisterWithSameMailbox(info.WriteTableActor);
            State = EState::WAITING;
        }

        auto writeToken = info.WriteTableActor->Open(settings.OperationType, std::move(settings.Columns));
        return {settings.TableId, std::move(writeToken)};
    }

    void Write(TWriteToken token, NMiniKQL::TUnboxedValueBatch&& data) override {
        YQL_ENSURE(State == EState::WRITING || State == EState::WAITING);

        auto& info = WriteInfos.at(token.TableId);
        info.WriteTableActor->Write(token.Cookie,std::move(data));
        Process();
    }

    void Close(TWriteToken token) override {
        YQL_ENSURE(State == EState::WRITING || State == EState::WAITING);

        auto& info = WriteInfos.at(token.TableId);
        info.WriteTableActor->Close(token.Cookie);
        Process();
    }

    void Flush(std::unique_ptr<IOnFlushedCallback>&& callback) override {
        State = EState::FLUSHING;
        //for (auto& [_, info] : WriteInfos) {
        //    YQL_ENSURE(info.WriteTableActor->IsClosed());
        //}
        OnFlushedCallback = std::move(callback);
        Process();
    }

    void Prepare(std::unique_ptr<IOnPreparedCallback>&& callback, TPrepareSettings&& prepareSettings) override {
        YQL_ENSURE(State == EState::WRITING);
        Y_UNUSED(callback, prepareSettings);
        Close();
    }

    void ImmediateCommit(std::unique_ptr<IOnCommittedCallback>&& callback) override {
        YQL_ENSURE(State == EState::WRITING);
        Y_UNUSED(callback);
        Close();
    }

    void Rollback(std::unique_ptr<IOnRolledBackCallback>&& callback) override {
        YQL_ENSURE(State == EState::WRITING);
        Y_UNUSED(callback);
    }

    void Close() {
        YQL_ENSURE(State == EState::WRITING);
        for (auto& [_, info] : WriteInfos) {
            info.WriteTableActor->Close();
        }
    }

    i64 GetFreeSpace(TWriteToken token) const override {
        auto& info = WriteInfos.at(token.TableId);
        return info.WriteTableActor->IsReady()
            ? MemoryLimit - info.WriteTableActor->GetMemory()
            : std::numeric_limits<i64>::min(); // Can't use zero here because compute can use overcommit!
    }

    i64 GetTotalFreeSpace() const override {
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

    void PassAway() override {
        for (auto& [_, info] : WriteInfos) {
            if (info.WriteTableActor) {
                info.WriteTableActor->Terminate();
            }
        }
        TActorBootstrapped<TKqpBufferWriteActor>::PassAway();
    }

    void Process() {
        if (GetTotalFreeSpace() <= 0) {
            State = EState::WAITING;
        } else if (State == EState::WAITING && GetTotalFreeSpace() > MemoryLimit / 2) {
            ResumeExecution();
        }

        const bool closed = (State == EState::FLUSHING
            || State == EState::PREPARING
            || State == EState::COMMITTING);

        if (closed || GetTotalFreeSpace() <= 0) {
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
                    //(*OnFlushedCallback)();
                    break;
                default:
                    YQL_ENSURE(false);
            }
        }
    }

    void ResumeExecution() {
        CA_LOG_D("Resuming execution.");
        State = EState::WRITING;
        for (auto& [_, info] : WriteInfos) {
            for (auto& [_, callback] : info.ResumeExecutionCallbacks) {
                callback();
            }
        }
    }

    void OnPrepared() override {
        Process();
    }

    void OnMessageAcknowledged(ui64) override {
        Process();
    }

    void OnError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) override {
        Settings.Callbacks->OnRuntimeError(message, statusCode, subIssues);
    }

private:
    TString LogPrefix;

    const TKqpBufferWriterSettings Settings;

    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    NMiniKQL::TTypeEnvironment TypeEnv;

    struct TWriteInfo {
        TKqpTableWriteActor* WriteTableActor = nullptr;
        TActorId WriteTableActorId;

        THashMap<ui64, std::function<void()>> ResumeExecutionCallbacks;
    };

    THashMap<TTableId, TWriteInfo> WriteInfos;

    EState State;
    std::unique_ptr<IOnFlushedCallback> OnFlushedCallback;

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
        , BufferWriter(reinterpret_cast<IKqpBufferWriter*>(Settings.GetBufferPtr()))
        , TxId(std::get<ui64>(args.TxId))
        , TableId(
            Settings.GetTable().GetOwnerId(),
            Settings.GetTable().GetTableId(),
            Settings.GetTable().GetVersion())
    {
        YQL_ENSURE(BufferWriter != nullptr);
        EgressStats.Level = args.StatsLevel;
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;

        TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata;
        columnsMetadata.reserve(Settings.GetColumns().size());
        for (const auto & column : Settings.GetColumns()) {
            columnsMetadata.push_back(column);
        }

        WriteToken = BufferWriter->Open(IKqpBufferWriter::TWriteSettings{
            .TableId = TableId,
            .TablePath = Settings.GetTable().GetPath(),
            .OperationType = GetOperation(Settings.GetType()),
            .Columns = std::move(columnsMetadata),
            .ResumeExecutionCallback = [this]() {
                Callbacks->ResumeExecution();
            },
        });
    }

    static constexpr char ActorName[] = "KQP_DIRECT_WRITE_ACTOR";

private:
    virtual ~TKqpForwardWriteActor() {
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
        auto result = WriteToken.IsEmpty()
            ? std::numeric_limits<i64>::min()
            : BufferWriter->GetFreeSpace(WriteToken);
        Cerr << "FWD::GET_FREE_SPACE" << WriteToken.IsEmpty() << " " << result << Endl;
        return result;
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        google::protobuf::Any result;
        return result;
    }

    void SendData(NMiniKQL::TUnboxedValueBatch&& data, i64 size, const TMaybe<NYql::NDqProto::TCheckpoint>&, bool finished) final {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        EgressStats.Resume();

        Cerr << "FWD::WRITE" << size << " " << finished << Endl;

        BufferWriter->Write(WriteToken, std::move(data));
        EgressStats.Bytes += size;
        EgressStats.Chunks++;
        EgressStats.Splits++;
        EgressStats.Resume();

        Cerr << "FWD::WRITE2" << size << " " << finished << Endl;

        if (finished) {
            Cerr << "FWD::FINISHED" << Endl;
            BufferWriter->Close(WriteToken);
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
        }
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        Cerr << "FWD::RuntimeError" << Endl;
        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

        Callbacks->OnAsyncOutputError(OutputIndex, std::move(issues), statusCode);
    }

    void PassAway() override {
        TActorBootstrapped<TKqpForwardWriteActor>::PassAway();
    }

    TString LogPrefix;
    const NKikimrKqp::TKqpTableSinkSettings Settings;
    const ui64 OutputIndex;
    NYql::NDq::TDqAsyncStats EgressStats;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks * Callbacks = nullptr;
    TIntrusivePtr<TKqpCounters> Counters;

    IKqpBufferWriter* BufferWriter = nullptr;

    const ui64 TxId;
    const TTableId TableId;

    IKqpBufferWriter::TWriteToken WriteToken;
};

std::pair<IKqpBufferWriter*, NActors::IActor*> CreateKqpBufferWriterActor(TKqpBufferWriterSettings&& settings) {
    auto* actor = new TKqpBufferWriteActor(std::move(settings));
    return std::make_pair<IKqpBufferWriter*, NActors::IActor*>(actor, actor);
}


void RegisterKqpWriteActor(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterSink<NKikimrKqp::TKqpTableSinkSettings>(
        TString(NYql::KqpTableSinkName),
        [counters] (NKikimrKqp::TKqpTableSinkSettings&& settings, NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args) {
            if (settings.GetBufferPtr() == 0) {
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
