#pragma once

#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>

namespace NKikimr {
namespace NKqp {

// Index of the partition of `shardId` inside `partitioning`, or
// `partitioning.Size()` when the shard is not in the partitioning.
size_t FindPartitionIndex(const TPartitioning& partitioning, ui64 shardId);

// Full key range of the index-th partition of `partitioning`.
// The first partition starts at the min key (`keyColumnsCount` null cells); every
// partition ends at the next partition's EndKeyPrefix (inclusive) and starts right
// after the previous partition's EndKeyPrefix (exclusive if that boundary was
// inclusive); the last partition has no upper bound.
// Returns a default (undefined) range for out-of-bounds index, a partition without
// range info (Range == Nothing) or a column-table partitioning where every
// partition has an empty EndKeyPrefix.
TOwnedTableRange GetShardKeyRange(const TPartitioning& partitioning, size_t index, size_t keyColumnsCount);

// Borders/version of `shardId` in `partitioning`. Borders stay undefined for a
// shard missing from `partitioning` or a shard without representable key range
// (e.g. column tables); the version is still recorded when `partitioning` is set.
TShardMeta MakeShardMeta(
    const TTableId& tableId,
    const TPartitioning::TCPtr& partitioning,
    ui64 shardId,
    size_t keyColumnsCount);

// Proto wire format of TShardMeta for TEvKqpInputActorResultInfo.
void FillShardPartitioningInfo(NKikimrTxDataShard::TShardPartitioningInfo& proto, ui64 shardId, const TShardMeta& meta);

// Inverse of FillShardPartitioningInfo.
TShardMeta ToShardMeta(const NKikimrTxDataShard::TShardPartitioningInfo& proto, const TTableId& tableId);

} // namespace NKqp
} // namespace NKikimr
