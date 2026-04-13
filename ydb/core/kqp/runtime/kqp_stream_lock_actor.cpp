#include "kqp_stream_lock_actor.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/common/kqp_event_ids.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/data_events/events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr {
namespace NKqp {

namespace {

NActors::TActorId MainPipeCacheId = NKikimr::MakePipePerNodeCacheID(false);

struct TKeyInfo {
    ui32 RowIndex;
    TString KeyCells;
};

struct TInFlightLockRequest {
    ui64 RequestId;
    ui64 ShardId;
    TVector<TKeyInfo> Keys;
};

} // namespace

class TStreamLockActor : public TActorBootstrapped<TStreamLockActor>, public NYql::NDq::IDqComputeActorAsyncOutput {
public:
    using TBase = TActorBootstrapped<TStreamLockActor>;

    TStreamLockActor(TKqpStreamLockSettings&& settings,
                     NYql::NDq::IDqAsyncIoFactory::TSinkArguments&& args,
                     TIntrusivePtr<TKqpCounters> counters)
        : Settings(std::move(settings))
        , OutputIndex(args.OutputIndex)
        , Callbacks(args.Callback)
        , TypeEnv(args.TypeEnv)
        , Alloc(args.Alloc)
        , Counters(counters)
        , TxId(args.TxId)
        , StatsLevel(args.StatsLevel)
        , TraceId(args.TraceId)
        , LogPrefix(TStringBuilder() << "StreamLockActor, outputIndex: " << args.OutputIndex << ", task: " << args.TaskId)
    {
        EgressStats.Level = StatsLevel;
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        ResolveTableShards();
    }

    void PassAway() override {
        for (auto& [requestId, request] : InFlightRequests) {
            Send(PipeCacheId, new TEvPipeCache::TEvUnlink(request.ShardId));
        }
        TActorBootstrapped::PassAway();
    }

private:
    void CommitState(const NYql::NDqProto::TCheckpoint&) final {}
    void LoadState(const NYql::NDq::TSinkState&) final {}

    ui64 GetOutputIndex() const final {
        return OutputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetEgressStats() const final {
        return EgressStats;
    }

    i64 GetFreeSpace() const final {
        return std::numeric_limits<i64>::max();
    }

    void SendData(NMiniKQL::TUnboxedValueBatch&& batch, i64 dataSize,
        const TMaybe<NYql::NDqProto::TCheckpoint>& checkpoint, bool finished) final;

    void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev);
    void HandleLockResult(NEvents::TDataEvents::TEvLockRowsResult::TPtr& ev);
    void HandleDeliveryProblem(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);

    void ResolveTableShards();
    void SendLockRequests();
    void OutputResults(ui64 requestId, const NEvents::TDataEvents::TEvLockRowsResult& result);
    void CheckCompletion();

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {});

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return TypeEnv.BindAllocator();
    }

    TKqpStreamLockSettings Settings;
    const ui64 OutputIndex;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks* Callbacks = nullptr;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    TIntrusivePtr<TKqpCounters> Counters;
    const NYql::NDq::TTxId TxId;
    const NYql::NDq::TCollectStatsLevel StatsLevel;
    const NWilson::TTraceId TraceId;
    TString LogPrefix;

    NYql::NDq::TDqAsyncStats EgressStats;

    const TActorId PipeCacheId = MainPipeCacheId;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
    bool ResolveShardsInProgress = false;

    struct TRowBuffer {
        NMiniKQL::TUnboxedValueBatch Batch;
        TVector<ui32> KeyColumnIds;
        TVector<NScheme::TTypeInfo> KeyColumnTypes;
    };

    TVector<TRowBuffer> PendingRows;
    THashMap<ui64, TInFlightLockRequest> InFlightRequests;
    ui64 NextRequestId = 1;
    bool InputFinished = false;
    bool ResolveCompleted = false;
};

void TStreamLockActor::SendData(NMiniKQL::TUnboxedValueBatch&& batch, i64 dataSize,
    const TMaybe<NYql::NDqProto::TCheckpoint>& checkpoint, bool finished)
{
    Y_UNUSED(dataSize);
    Y_UNUSED(checkpoint);

    if (batch.empty() && !finished) {
        return;
    }

    auto guard = BindAllocator();

    if (!batch.empty()) {
        TRowBuffer buffer;
        buffer.Batch = std::move(batch);

        buffer.KeyColumnIds.reserve(Settings.KeyColumns.size());
        buffer.KeyColumnTypes.reserve(Settings.KeyColumns.size());
        for (const auto& col : Settings.KeyColumns) {
            buffer.KeyColumnIds.push_back(col.GetId());
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(
                col.GetTypeId(),
                col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr);
            buffer.KeyColumnTypes.push_back(typeInfoMod.TypeInfo);
        }

        PendingRows.push_back(std::move(buffer));
    }

    InputFinished = finished;

    if (ResolveCompleted && !PendingRows.empty()) {
        SendLockRequests();
    }

    CheckCompletion();
}

void TStreamLockActor::ResolveTableShards() {
    if (ResolveShardsInProgress) {
        return;
    }

    ResolveShardsInProgress = true;

    auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
    request->DatabaseName = Settings.Database;

    TVector<NScheme::TTypeInfo> keyColumnTypes;
    keyColumnTypes.reserve(Settings.KeyColumns.size());
    for (const auto& col : Settings.KeyColumns) {
        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(
            col.GetTypeId(),
            col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr);
        keyColumnTypes.push_back(typeInfoMod.TypeInfo);
    }

    TVector<TCell> minusInf(keyColumnTypes.size());
    TVector<TCell> plusInf;
    TTableRange range(minusInf, true, plusInf, true, false);

    TTableId tableId(Settings.Table.GetOwnerId(), Settings.Table.GetTableId(), Settings.Table.GetVersion());

    request->ResultSet.emplace_back(MakeHolder<TKeyDesc>(tableId, range, TKeyDesc::ERowOperation::Read,
        keyColumnTypes, TVector<TKeyDesc::TColumnOp>{}));

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));
}

void TStreamLockActor::HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
    if (!ResolveShardsInProgress) {
        return;
    }

    ResolveShardsInProgress = false;

    if (ev->Get()->Request->ErrorCount > 0) {
        TString errorMsg = TStringBuilder() << "Failed to resolve shards for table: "
            << Settings.Table.GetPath();
        return RuntimeError(errorMsg, NYql::NDqProto::StatusIds::SCHEME_ERROR);
    }

    auto& resultSet = ev->Get()->Request->ResultSet;
    YQL_ENSURE(resultSet.size() == 1, "Expected one result for range [NULL, +inf)");
    Partitioning = resultSet[0].KeyDescription->Partitioning;

    ResolveCompleted = true;

    if (!PendingRows.empty()) {
        SendLockRequests();
    }

    CheckCompletion();
}

void TStreamLockActor::SendLockRequests() {
    if (!Partitioning || PendingRows.empty()) {
        return;
    }

    auto guard = BindAllocator();

    for (auto& rowBuffer : PendingRows) {
        const auto& batch = rowBuffer.Batch;
        if (batch.empty()) {
            continue;
        }

        THashMap<ui64, TVector<TKeyInfo>> keysByShard;

        batch.ForEachRow([&](const NUdf::TUnboxedValue& row) {
            NMiniKQL::TStringProviderBackend backend;
            std::vector<TCell> keyCells(rowBuffer.KeyColumnTypes.size());
            for (size_t colIdx = 0; colIdx < rowBuffer.KeyColumnIds.size(); ++colIdx) {
                auto value = row.GetElement(colIdx);
                keyCells[colIdx] = MakeCell(rowBuffer.KeyColumnTypes[colIdx], value, backend, false);
            }

            TString serializedKeys;
            for (const auto& cell : keyCells) {
                if (cell.IsNull()) {
                    serializedKeys.append(1, 0);
                } else {
                    serializedKeys.append(cell.Data(), cell.Size());
                }
            }

            ui64 shardId = 0;
            bool foundShard = false;
            for (const auto& partition : *Partitioning) {
                shardId = partition.ShardId;
                foundShard = true;
                break;
            }

            if (foundShard) {
                keysByShard[shardId].push_back({0, std::move(serializedKeys)});
            }
        });

        for (auto& [shardId, keys] : keysByShard) {
            if (keys.empty()) {
                continue;
            }

            ui64 requestId = NextRequestId++;

            auto lockRequest = MakeHolder<NEvents::TDataEvents::TEvLockRows>(requestId);
            lockRequest->Record.SetLockId(Settings.LockTxId);
            lockRequest->Record.SetLockNodeId(Settings.LockNodeId);
            lockRequest->Record.SetLockMode(Settings.LockMode);

            TTableId tableId(Settings.Table.GetOwnerId(), Settings.Table.GetTableId(), Settings.Table.GetVersion());
            lockRequest->SetTableId(tableId);

            if (Settings.Snapshot.GetStep() || Settings.Snapshot.GetTxId()) {
                lockRequest->Record.MutableSnapshot()->SetStep(Settings.Snapshot.GetStep());
                lockRequest->Record.MutableSnapshot()->SetTxId(Settings.Snapshot.GetTxId());
            }

            for (const auto& col : Settings.KeyColumns) {
                lockRequest->Record.AddColumnIds(col.GetId());
            }

            lockRequest->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);

            TString matrix;
            for (const auto& key : keys) {
                matrix.append(key.KeyCells);
            }
            lockRequest->SetCellMatrix(std::move(matrix));

            bool needToCreatePipe = true;

            Send(PipeCacheId,
                new TEvPipeCache::TEvForward(
                    lockRequest.Release(),
                    shardId,
                    TEvPipeCache::TEvForwardOptions{
                        .AutoConnect = needToCreatePipe,
                        .Subscribe = needToCreatePipe,
                    }),
                IEventHandle::FlagTrackDelivery);

            InFlightRequests[requestId] = {requestId, shardId, std::move(keys)};
        }
    }

    PendingRows.clear();
}

void TStreamLockActor::HandleLockResult(NEvents::TDataEvents::TEvLockRowsResult::TPtr& ev) {
    const auto& record = ev->Get()->Record;

    auto requestIt = InFlightRequests.find(record.GetRequestId());
    if (requestIt == InFlightRequests.end()) {
        return;
    }

    auto& request = requestIt->second;

    switch (record.GetStatus()) {
        case NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS:
        case NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN:
            break;
        case NKikimrDataEvents::TEvLockRowsResult::STATUS_OVERLOADED:
        case NKikimrDataEvents::TEvLockRowsResult::STATUS_INTERNAL_ERROR: {
            TString errorMsg = TStringBuilder() << "Lock request failed with status: " << record.GetStatus();
            RuntimeError(errorMsg, NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            InFlightRequests.erase(requestIt);
            return;
        }
        default: {
            TString errorMsg = TStringBuilder() << "Lock request failed with status: " << record.GetStatus();
            RuntimeError(errorMsg, NYql::NDqProto::StatusIds::ABORTED);
            InFlightRequests.erase(requestIt);
            return;
        }
    }

    OutputResults(request.RequestId, *ev->Get());

    InFlightRequests.erase(requestIt);

    CheckCompletion();
}

void TStreamLockActor::OutputResults(ui64 requestId, const NEvents::TDataEvents::TEvLockRowsResult& result) {
    Y_UNUSED(requestId);
    Y_UNUSED(result);

    if (Callbacks) {
        Callbacks->ResumeExecution();
    }
}

void TStreamLockActor::HandleDeliveryProblem(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    const auto& tabletId = ev->Get()->TabletId;

    for (auto& [requestId, request] : InFlightRequests) {
        if (request.ShardId == tabletId) {
            TString errorMsg = TStringBuilder() << "Delivery problem for shard: " << tabletId;
            RuntimeError(errorMsg, NYql::NDqProto::StatusIds::UNAVAILABLE);
            return;
        }
    }
}

void TStreamLockActor::CheckCompletion() {
    if (InputFinished && InFlightRequests.empty() && PendingRows.empty()) {
        if (Callbacks) {
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
        }
    }
}

void TStreamLockActor::RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) {
    NYql::TIssue issue(message);
    for (const auto& i : subIssues) {
        issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
    }

    NYql::TIssues issues;
    issues.AddIssue(std::move(issue));

    if (Callbacks) {
        Callbacks->OnAsyncOutputError(GetOutputIndex(), issues, statusCode);
    }
}

std::pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*> CreateKqpStreamLockActor(TKqpStreamLockSettings&& settings,
                                  NYql::NDq::IDqAsyncIoFactory::TSinkArguments&& args,
                                  TIntrusivePtr<TKqpCounters> counters) {
    auto* actor = new TStreamLockActor(std::move(settings), std::move(args), counters);
    return {actor, actor};
}

} // namespace NKqp
} // namespace NKikimr
