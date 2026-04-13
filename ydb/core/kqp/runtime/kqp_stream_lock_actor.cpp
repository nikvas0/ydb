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

class TStreamLockActor : public TActorBootstrapped<TStreamLockActor>, public NYql::NDq::IDqComputeActorAsyncInput {
public:
    using TBase = TActorBootstrapped<TStreamLockActor>;

    TStreamLockActor(TKqpStreamLockSettings&& settings,
                     NYql::NDq::IDqAsyncIoFactory::TInputTransformArguments&& args,
                     TIntrusivePtr<TKqpCounters> counters)
        : Settings(std::move(settings))
        , InputIndex(args.InputIndex)
        , Input(args.TransformInput)
        , ComputeActorId(args.ComputeActorId)
        , TypeEnv(args.TypeEnv)
        , Alloc(args.Alloc)
        , Counters(counters)
        , TxId(args.TxId)
        , StatsLevel(args.StatsLevel)
        , TraceId(args.TraceId)
        , LogPrefix(TStringBuilder() << "StreamLockActor, inputIndex: " << args.InputIndex << ", CA Id: " << args.ComputeActorId)
    {
        IngressStats.Level = StatsLevel;
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        ResolveTableShards();
        Become(&TStreamLockActor::StateFunc);
    }

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolve);
                hFunc(NEvents::TDataEvents::TEvLockRowsResult, HandleLockResult);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandleDeliveryProblem);
                default:
                    RuntimeError(TStringBuilder() << "Unexpected event: " << ev->GetTypeRewrite(),
                        NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            }
        } catch (const NKikimr::TMemoryLimitExceededException& e) {
            RuntimeError("Memory limit exceeded at stream lock", NYql::NDqProto::StatusIds::PRECONDITION_FAILED);
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void PassAway() override {
        for (auto& [requestId, request] : InFlightRequests) {
            Send(PipeCacheId, new TEvPipeCache::TEvUnlink(request.ShardId));
        }
        TActorBootstrapped::PassAway();
    }

private:
    ui64 GetInputIndex() const final {
        return InputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetIngressStats() const final {
        return IngressStats;
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final;

    void SaveState(const NYql::NDqProto::TCheckpoint& checkpoint, NYql::NDq::TSourceState& state) final {
        Y_UNUSED(checkpoint);
        Y_UNUSED(state);
    }

    void LoadState(const NYql::NDq::TSourceState& state) final {
        Y_UNUSED(state);
    }

    void CommitState(const NYql::NDqProto::TCheckpoint& checkpoint) final {
        Y_UNUSED(checkpoint);
    }

    void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev);
    void HandleLockResult(NEvents::TDataEvents::TEvLockRowsResult::TPtr& ev);
    void HandleDeliveryProblem(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);

    void ResolveTableShards();
    void FetchInputRows();
    void ProcessInputRows();
    void SendLockRequests();
    void OutputResults(ui64 requestId, const NEvents::TDataEvents::TEvLockRowsResult& result);
    void CheckCompletion();
    void NotifyCA();

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {});

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return TypeEnv.BindAllocator();
    }

    TKqpStreamLockSettings Settings;
    const ui64 InputIndex;
    NUdf::TUnboxedValue Input;
    const TActorId ComputeActorId;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    TIntrusivePtr<TKqpCounters> Counters;
    const NYql::NDq::TTxId TxId;
    const NYql::NDq::TCollectStatsLevel StatsLevel;
    const NWilson::TTraceId TraceId;
    TString LogPrefix;

    NYql::NDq::TDqAsyncStats IngressStats;

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
    NUdf::EFetchStatus LastFetchStatus = NUdf::EFetchStatus::Ok;
    bool HasPendingResults = false;
};

i64 TStreamLockActor::GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>&, bool& finished, i64) {
    YQL_ENSURE(!batch.IsWide(), "Wide stream is not supported");

    if (ResolveShardsInProgress) {
        finished = false;
        return 0;
    }

    FetchInputRows();

    if (Partitioning) {
        ProcessInputRows();
    }

    HasPendingResults = false;
    for (const auto& [requestId, request] : InFlightRequests) {
        Y_UNUSED(requestId);
        if (!request.Keys.empty()) {
            HasPendingResults = true;
            break;
        }
    }

    if (HasPendingResults || !PendingRows.empty()) {
        NotifyCA();
    }

    finished = InputFinished && InFlightRequests.empty() && PendingRows.empty();

    CA_LOG_D("Returned data, finished: " << finished);
    return 0;
}

void TStreamLockActor::FetchInputRows() {
    auto guard = BindAllocator();

    NUdf::TUnboxedValue row;

    YQL_ENSURE(!Input.IsInvalid());
    if (Input.IsFinish() || !Input.HasValue()) {
        LastFetchStatus = NUdf::EFetchStatus::Finish;
        InputFinished = true;
        return;
    }

    while ((LastFetchStatus = Input.Fetch(row)) == NUdf::EFetchStatus::Ok) {
        TRowBuffer buffer;
        buffer.Batch.emplace_back(std::move(row));

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
}

void TStreamLockActor::NotifyCA() {
    Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
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

    NotifyCA();
}

void TStreamLockActor::ProcessInputRows() {
    if (!Partitioning || PendingRows.empty()) {
        return;
    }

    SendLockRequests();
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

    NotifyCA();
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
        NotifyCA();
    }
}

void TStreamLockActor::RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) {
    NYql::TIssue issue(message);
    for (const auto& i : subIssues) {
        issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
    }

    NYql::TIssues issues;
    issues.AddIssue(std::move(issue));

    Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, statusCode));
}

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateKqpStreamLockActor(TKqpStreamLockSettings&& settings,
                                  NYql::NDq::IDqAsyncIoFactory::TInputTransformArguments&& args,
                                  TIntrusivePtr<TKqpCounters> counters) {
    auto* actor = new TStreamLockActor(std::move(settings), std::move(args), counters);
    return {actor, actor};
}

} // namespace NKqp
} // namespace NKikimr
