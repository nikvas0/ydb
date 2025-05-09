#include "cleanup_portions.h"

#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>

namespace NKikimr::NOlap {

void TCleanupPortionsColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    if (ui32 dropped = PortionsToDrop.size()) {
        out << "drop " << dropped << " portions";
        for (auto& portionInfo : PortionsToDrop) {
            out << portionInfo->DebugString();
        }
    }
}

void TCleanupPortionsColumnEngineChanges::DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) {
    AFL_VERIFY(FetchedDataAccessors);
    PortionsToRemove.ApplyOnExecute(self, context, *FetchedDataAccessors);

    THashSet<TInternalPathId> pathIds;
    if (!self) {
        return;
    }
    THashSet<ui64> usedPortionIds = PortionsToRemove.GetPortionIds();
    auto schemaPtr = context.EngineLogs.GetVersionedIndex().GetLastSchema();

    THashMap<TString, THashSet<TUnifiedBlobId>> blobIdsByStorage;

    for (auto&& p : PortionsToDrop) {
        const auto& accessor = FetchedDataAccessors->GetPortionAccessorVerified(p->GetPortionId());
        accessor.RemoveFromDatabase(context.DBWrapper);
        accessor.FillBlobIdsByStorage(blobIdsByStorage, context.EngineLogs.GetVersionedIndex());
        pathIds.emplace(p->GetPathId());
    }
    for (auto&& i : blobIdsByStorage) {
        auto action = BlobsAction.GetRemoving(i.first);
        for (auto&& b : i.second) {
            action->DeclareRemove((TTabletId)self->TabletID(), b);
        }
    }
}

void TCleanupPortionsColumnEngineChanges::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    PortionsToRemove.ApplyOnComplete(self, context, *FetchedDataAccessors);
    for (auto& portionInfo : PortionsToDrop) {
        if (!context.EngineLogs.ErasePortion(*portionInfo)) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot erase portion")("portion", portionInfo->DebugString());
        }
    }
    if (self) {
        self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_PORTIONS_ERASED, PortionsToDrop.size());
        for (auto&& p : PortionsToDrop) {
            self->Counters.GetTabletCounters()->OnDropPortionEvent(p->GetTotalRawBytes(), p->GetTotalBlobBytes(), p->GetRecordsCount());
        }
    }
}

void TCleanupPortionsColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    self.BackgroundController.StartCleanupPortions();
}

void TCleanupPortionsColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishCleanupPortions();
}

NColumnShard::ECumulativeCounters TCleanupPortionsColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_CLEANUP_SUCCESS : NColumnShard::COUNTER_CLEANUP_FAIL;
}

}   // namespace NKikimr::NOlap
