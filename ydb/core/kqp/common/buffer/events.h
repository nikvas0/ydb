#pragma once

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>


namespace NKikimr {
namespace NKqp {

struct TEvBufferPrepare : public TEventLocal<TEvBufferPrepare, TKqpBufferWriterEvents::EvPrepare> {
};

struct TEvBufferPrepared : public TEventLocal<TEvBufferPrepared, TKqpBufferWriterEvents::EvPrepared> {
};

struct TEvBufferCommit : public TEventLocal<TEvBufferCommit, TKqpBufferWriterEvents::EvCommit> {
};

struct TEvBufferCommitted : public TEventLocal<TEvBufferCommitted, TKqpBufferWriterEvents::EvCommitted> {
};

struct TEvBufferRollback : public TEventLocal<TEvBufferRollback, TKqpBufferWriterEvents::EvRollback> {
};

struct TEvBufferFlush : public TEventLocal<TEvBufferFlush, TKqpBufferWriterEvents::EvFlush> {
};

struct TEvBufferError : public TEventLocal<TEvBufferError, TKqpBufferWriterEvents::EvError> {
};

}
}
