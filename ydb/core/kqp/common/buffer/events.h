#pragma once

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>


namespace NKikimr {
namespace NKqp {

struct TPrepareSettings {
    ui64 TxId;
    THashSet<ui64> SendingShards;
    THashSet<ui64> ReceivingShards;
    std::optional<ui64> ArbiterShard;
};


struct TEvBufferPrepare : public TEventLocal<TEvBufferPrepare, TKqpBufferWriterEvents::EvPrepare> {
    TPrepareSettings Settings;
};

struct TPreparedInfo {
    ui64 ShardId;
    ui64 MinStep;
    ui64 MaxStep;
    TVector<ui64> Coordinators;
};

struct TEvBufferPrepared : public TEventLocal<TEvBufferPrepared, TKqpBufferWriterEvents::EvPrepared> {
    TPreparedInfo Result;
};

struct TEvBufferCommit : public TEventLocal<TEvBufferCommit, TKqpBufferWriterEvents::EvCommit> {
};

struct TEvBufferCommitted : public TEventLocal<TEvBufferCommitted, TKqpBufferWriterEvents::EvCommitted> {
    ui64 ShardId;
};

struct TEvBufferRollback : public TEventLocal<TEvBufferRollback, TKqpBufferWriterEvents::EvRollback> {
};

struct TEvBufferFlush : public TEventLocal<TEvBufferFlush, TKqpBufferWriterEvents::EvFlush> {
};

struct TEvBufferError : public TEventLocal<TEvBufferError, TKqpBufferWriterEvents::EvError> {
};

}
}
