#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/data_events.pb.h>

namespace NKikimr {
namespace NKqp {

struct TKqpStreamLockSettings {
    NKqpProto::TKqpPhyTableId Table;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> KeyColumns;
    ui64 LockTxId = 0;
    ui32 LockNodeId = 0;
    NKikimrDataEvents::ELockMode LockMode = NKikimrDataEvents::ELockMode::PESSIMISTIC_EXCLUSIVE;
    TString Database;
    NKikimrDataEvents::TMvccSnapshot Snapshot;
    ui64 QuerySpanId = 0;
};

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateKqpStreamLockActor(TKqpStreamLockSettings&& settings,
    NYql::NDq::IDqAsyncIoFactory::TInputTransformArguments&& args,
    TIntrusivePtr<TKqpCounters> counters);

} // namespace NKqp
} // namespace NKikimr
