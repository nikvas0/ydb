#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimr {
namespace NKqp {

struct IKqpBufferWriterCallbacks {
    virtual ~IKqpBufferWriterCallbacks() {}

    virtual void OnRuntimeError(
        const TString& message,
        NYql::NDqProto::StatusIds::StatusCode statusCode,
        const NYql::TIssues& subIssues) = 0;
};

// TODO: move somewhere else
class IKqpBufferWriter {
public:
    virtual ~IKqpBufferWriter() = default;

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

    virtual TWriteToken Open(TWriteSettings&& settings) = 0;
    virtual void Write(TWriteToken token, NMiniKQL::TUnboxedValueBatch&& data) = 0;
    virtual void Close(TWriteToken token) = 0;

    virtual i64 GetFreeSpace(TWriteToken token) const = 0;
    virtual i64 GetTotalFreeSpace() const = 0;

    // Only when all writes are closed!
    struct IOnFlushedCallback {
        virtual ~IOnFlushedCallback() {}
        virtual void operator()() = 0;
    };
    virtual void Flush(std::unique_ptr<IOnFlushedCallback>&& callback) = 0;
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

    struct IOnPreparedCallback {
        virtual ~IOnPreparedCallback() {}
        virtual void operator()(TPreparedInfo&& preparedInfo) = 0;
    };
    virtual void Prepare(std::unique_ptr<IOnPreparedCallback>&& callback, TPrepareSettings&& prepareSettings) = 0;

    struct IOnCommittedCallback {
        virtual ~IOnCommittedCallback() {}
        virtual void operator()() = 0;
    };
    virtual void ImmediateCommit(std::unique_ptr<IOnCommittedCallback>&& callback) = 0;

    struct IOnRolledBackCallback {
        virtual ~IOnRolledBackCallback() {}
        virtual void operator()() = 0;
    };
    virtual void Rollback(std::unique_ptr<IOnRolledBackCallback>&& callback) = 0;

    virtual THashSet<ui64> GetShardsIds() const = 0;
    virtual THashMap<ui64, NKikimrDataEvents::TLock> GetLocks() const = 0;
};

struct TKqpBufferWriterSettings {
    ui64 TxId;
    ui64 LockTxId;
    ui64 LockNodeId;
    bool InconsistentTx;
    IKqpBufferWriterCallbacks* Callbacks;
};

std::pair<IKqpBufferWriter*, NActors::IActor*> CreateKqpBufferWriterActor(TKqpBufferWriterSettings&& settings);

void RegisterKqpWriteActor(NYql::NDq::TDqAsyncIoFactory&, TIntrusivePtr<TKqpCounters>);

}
}
