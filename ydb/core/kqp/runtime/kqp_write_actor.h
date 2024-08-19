#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimr {
namespace NKqp {

struct IKqpWriteBufferCallbacks {
    virtual ~IKqpWriteBufferCallbacks() {}

    virtual void OnRuntimeError(
        const TString& message,
        NYql::NDqProto::StatusIds::StatusCode statusCode,
        const NYql::TIssues& subIssues) = 0;
};

// TODO: move somewhere else
class IKqpWriteBuffer {
public:
    virtual ~IKqpWriteBuffer() = default;

    struct TWriteToken {
        TTableId TableId;
        ui64 Cookie;

        bool IsEmpty() const {
            return !TableId;
        }
    };

    struct TWriteSettings {
        TTableId TableId;
        TString TablePath; // for error messages
        NKikimrDataEvents::TEvWrite::TOperation::EOperationType OperationType;
        TVector<NKikimrKqp::TKqpColumnMetadataProto> Columns;
        std::function<void()> ResumeExecutionCallback; // TODO: get rid of std::fuction
    };

    virtual void SetOnRuntimeError(IKqpWriteBufferCallbacks* callbacks) = 0;

    virtual TWriteToken Open(TWriteSettings&& settings) = 0;
    virtual void Write(TWriteToken token, NMiniKQL::TUnboxedValueBatch&& data) = 0;
    virtual void Close(TWriteToken token) = 0;
    virtual THashMap<ui64, NKikimrDataEvents::TLock> GetLocks(TWriteToken token) const = 0;

    virtual i64 GetFreeSpace(TWriteToken token) const = 0;
    virtual i64 GetTotalFreeSpace() const = 0;

    // Only when all writes are closed!
    virtual void Flush(std::function<void()> callback) = 0;
    //virtual void Flush(TTableId tableId) = 0;

    struct TPrepareSettings {
        THashSet<ui64> SendingShards;
        THashSet<ui64> ReceivingShards;
        std::optional<ui64> ArbiterShard;
    };

    struct TPreparedInfo {
        ui64 ShardId;
        ui64 MinStep;
        ui64 MaxStep;
        TVector<ui64> Coordinators;
    };

    virtual void Prepare(std::function<void(TPreparedInfo&&)> callback, TPrepareSettings&& prepareSettings) = 0;
    virtual void OnCommit(std::function<void(ui64)> callback) = 0;
    virtual void ImmediateCommit(std::function<void(ui64)> callback) = 0;
    virtual void Rollback(std::function<void(ui64)> callback) = 0;

    virtual THashSet<ui64> GetShardsIds() const = 0;
    virtual THashMap<ui64, NKikimrDataEvents::TLock> GetLocks() const = 0;

    virtual bool IsFinished() const = 0;

    virtual void Terminate() = 0;
};

struct TKqpBufferWriterSettings {
    ui64 TxId = 0;
    ui64 LockTxId = 0;
    ui64 LockNodeId = 0;
    bool InconsistentTx = false;
    //IKqpWriteBufferCallbacks* Callbacks = nullptr;
};

std::pair<IKqpWriteBuffer*, NActors::IActor*> CreateKqpBufferWriterActor(TKqpBufferWriterSettings&& settings);

void RegisterKqpWriteActor(NYql::NDq::TDqAsyncIoFactory&, TIntrusivePtr<TKqpCounters>);

}
}
