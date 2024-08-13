#pragma once

#include "kqp_tx.h"
#include <ydb/core/protos/data_events.pb.h>


namespace NKikimr {
namespace NKqp {

class IKqpTransactionManager {
public:
    virtual ~IKqpTransactionManager() = default;

    enum EShardState {
        PROCESSING,
        PREPARING,
        PREPARED,
        EXECUTING,
        FINISHED
    };

    enum EAction {
        READ = 1,
        WRITE = 2,
    };

    using TActionFlags = ui8;

    enum ESource {
        Executer = 1,
        BufferWriter = 2,
    };

    using TSource = ui8;

    virtual void AddShard(ui64 shardId, ESource source) = 0;
    virtual void AddAction(ui64 shardId, ui8 action) = 0;
    virtual bool AddLock(ui64 shardId, TKqpTxLock lock) = 0;

    virtual EShardState GetState(ui64 shardId) const = 0;
    virtual void SetState(ui64 shardId, EShardState state) = 0;

    virtual bool IsTxPrepared() const = 0;
    virtual bool IsTxFinished() const = 0;

    virtual bool IsReadOnly() const = 0;
    virtual bool IsSingleShard() const = 0;

    virtual bool HasValidSnapshot() const = 0;
    virtual void SetHasValidSnapshot(bool hasSnapshot) = 0;

    struct TCheckLocksResult {
        bool Ok = false;
        std::vector<TKqpTxLock> BrokenLocks;
        bool LocksAcquireFailure = false;
    };
    virtual TCheckLocksResult CheckLocks() const = 0;

    virtual const THashSet<ui64>& GetShards() const = 0;
    virtual ui64 GetShardsCount() const = 0;

    virtual void StartPrepare() = 0;

    struct TPrepareInfo {
        const THashSet<ui64>& SendingShards;
        const THashSet<ui64>& ReceivingShards;
        std::optional<ui64> Arbiter; // TODO: support volatile
        TVector<TKqpTxLock> Locks;

        ESource Sender;
    };

    virtual TPrepareInfo GetPrepareTransactionInfo(ui64 shardId) = 0;

    struct TPrepareResult {
        ui64 ShardId;
        ui64 MinStep;
        ui64 MaxStep;
        ui64 Coordinator;
    };

    virtual bool ConsumePrepareTransactionResult(TPrepareResult&& result) = 0;

    virtual void StartExecuting() = 0;

    struct TCommitShardInfo {
        ui64 ShardId;
        ui32 AffectedFlags;
    };

    struct TCommitInfo {
        ui64 MinStep;
        ui64 MaxStep;
        ui64 Coordinator;

        TVector<TCommitShardInfo> ShardsInfo;
    };

    virtual TCommitInfo GetCommitInfo() = 0;

    virtual bool ConsumeCommitResult(ui64 shardId) = 0;
};

using IKqpTransactionManagerPtr = std::shared_ptr<IKqpTransactionManager>;

IKqpTransactionManagerPtr CreateKqpTransactionManager();

}
}
