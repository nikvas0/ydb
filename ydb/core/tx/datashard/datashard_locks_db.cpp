#include "datashard_locks_db.h"

namespace NKikimr::NDataShard {

void TDataShardLocksDb::PersistLockCounter(ui64 lockId, ui64 counter) {
    TBase::PersistLockCounter(lockId, counter);
    if (counter == Max<ui64>()) {
        NIceDb::TNiceDb db(DB);
        Self.GetMultiTxIdManager().OnLockBroken(db, lockId);
    }
}

void TDataShardLocksDb::PersistRemoveLockData(ui64 lockId) {
    // Split rejection discards writes but retains the broken parent: deleting
    // that parent would let a WSN=1 retry recreate a rejected no-op transaction.
    // We remove lock changes unless it's managed by volatile tx manager
    bool isVolatile = Self.GetVolatileTxManager().FindByCommitTxId(lockId);
    if (!isVolatile) {
        for (auto& pr : Self.GetUserTables()) {
            auto tid = pr.second->LocalTid;
            if (DB.HasOpenTx(tid, lockId)) {
                DB.RemoveTx(tid, lockId);
                // This helper can run without deleting the Locks row; callers
                // must still observe the localdb mutation through HasChanges().
                HasChanges_ = true;
                Self.GetConflictsCache().GetTableCache(tid).RemoveUncommittedWrites(lockId, DB);
            }
        }
        // Discard pending change records even when the broken lock is retained.
        Self.ScheduleRemoveLockChanges(lockId);
    }
}

void TDataShardLocksDb::PersistRemoveLock(ui64 lockId) {
    PersistRemoveLockData(lockId);
    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(DB);
    db.Table<typename Schema::Locks>().Key(lockId).Delete();
    HasChanges_ = true;
    // This notification belongs to full removal. A retained broken parent's
    // MultiTxId cleanup is already scheduled by PersistLockCounter/OnLockBroken.
    Self.GetMultiTxIdManager().OnLockRemoved(db, lockId);
}

bool TDataShardLocksDb::MayAddLock(ui64 lockId) {
    for (const auto& [_, table] : Self.GetUserTables()) {
        // Untracked ids may belong to lost locks; never resurrect their data.
        if (DB.HasTxData(table->LocalTid, lockId)) {
            return false;
        }
    }
    return true;
}

bool TDataShardLocksDb::MayActivateLock(ui64 lockId) {
    // Unlike MayAddLock, activation has a known tracker proving there are no
    // local deltas. HasTxData would also see harmless references elsewhere in
    // borrowed parts and incorrectly forbid the first local write.
    for (const auto& [_, table] : Self.GetUserTables()) {
        // Commit/abort decisions still apply to future writes with this id.
        if (DB.HasCommittedTx(table->LocalTid, lockId) || DB.HasRemovedTx(table->LocalTid, lockId)) {
            return false;
        }
    }
    return true;
}

} // namespace NKikimr::NDataShard
