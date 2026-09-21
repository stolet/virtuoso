/*
 * Copyright 2019 University of Washington, Max Planck Institute for
 * Software Systems, and The University of Texas at Austin
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/**
 * Full queue manager implementation with rate-limits
 */
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <unistd.h>

#include <rte_config.h>
#include <rte_malloc.h>
#include <rte_cycles.h>

#include <utils.h>

#include "internal.h"

#define dprintf(...) do { } while (0)

#define FLAG_INSKIPLIST 1
#define FLAG_INNOLIMITL 2

/** Skiplist: bits per level */
#define SKIPLIST_BITS 3
/** Index list: invalid index */
#define IDXLIST_INVAL (-1U)

#define RNG_SEED 0x12345678
#define TIMESTAMP_BITS 32
#define TIMESTAMP_MASK 0xFFFFFFFF

/** Queue state */
struct queue {
  /** Next pointers for levels in skip list */
  uint32_t next_idxs[QMAN_SKIPLIST_LEVELS];
  /** Time stamp */
  uint32_t next_ts;
  /** Assigned Rate */
  uint32_t rate;
  /** Number of entries in queue */
  uint32_t avail;
  /** Maximum chunk size when de-queueing */
  uint16_t max_chunk;
  /** Flags: FLAG_INSKIPLIST, FLAG_INNOLIMITL */
  uint16_t flags;
} __attribute__((packed));
STATIC_ASSERT((sizeof(struct queue) == 32), queue_size);


/** Actually update queue state: must run on queue's home core */
static inline void set_impl(struct qman_thread *t, uint32_t id, uint32_t rate,
    uint32_t avail, uint16_t max_chunk, uint8_t flags);

/** Add queue to the no limit list */
static inline void queue_activate_nolimit(struct qman_thread *t,
    struct qman_appctx *qa, struct queue *q, uint32_t idx);
static inline unsigned poll_nolimit(struct qman_thread *t,
    struct qman_appctx *qa, uint32_t cur_ts, unsigned num, unsigned *q_ids,
    uint16_t *q_bytes);

/** Add queue to the skip list list */
static inline void queue_activate_skiplist(struct qman_thread *t,
    struct qman_appctx *qa, struct queue *q, uint32_t idx);
static inline unsigned poll_skiplist(struct qman_thread *t,
    struct qman_appctx *qa, uint32_t cur_ts, unsigned num, unsigned *q_ids,
    uint16_t *q_bytes);
static inline uint8_t queue_level(struct qman_thread *t);

static inline void queue_fire(struct qman_thread *t, struct qman_appctx *qa,
    struct queue *q, uint32_t idx, unsigned *q_id, uint16_t *q_bytes);
static inline void queue_activate(struct qman_thread *t,
    struct qman_appctx *qa, struct queue *q, uint32_t idx);
static inline uint16_t queue_appctx(unsigned q_id);
static inline uint32_t queue_new_ts(struct qman_appctx *qa, struct queue *q,
    uint32_t bytes);
static inline int timestamp_lessthaneq(struct qman_appctx *qa, uint32_t a,
    uint32_t b);
static inline uint32_t timestamp(void);
static inline int64_t rel_time(uint32_t cur_ts, uint32_t ts_in);
static inline unsigned poll_raw(struct qman_thread *t, struct qman_appctx *qa,
    uint32_t ts, unsigned num, unsigned *q_ids, uint16_t *q_bytes);


int tas_qman_thread_init(struct dataplane_context *ctx)
{
  struct qman_thread *t = &ctx->qman;
  unsigned i;

  t->id = ctx->id;

  if ((t->queues = calloc(1, sizeof(*t->queues) * FLEXNIC_NUM_QMQUEUES))
      == NULL)
  {
    fprintf(stderr, "qman_thread_init: queues malloc failed\n");
    return -1;
  }

  for (i = 0; i < FLEXNIC_PL_APPCTX_NUM; i++) {
    unsigned j;

    for (j = 0; j < QMAN_SKIPLIST_LEVELS; j++) {
      t->appctx[i].head_idx[j] = IDXLIST_INVAL;
    }
    t->appctx[i].nolimit_head_idx = IDXLIST_INVAL;
    t->appctx[i].nolimit_tail_idx = IDXLIST_INVAL;
    t->appctx[i].ts_virtual = 0;
    t->appctx[i].ts_real = timestamp();
    t->appctx[i].nolimit_first = false;
  }
  utils_rng_init(&t->rng, RNG_SEED * ctx->id + ctx->id);
  t->appctx_next = ctx->id % FLEXNIC_PL_APPCTX_NUM;

  return 0;
}

uint32_t tas_qman_timestamp(uint64_t cycles)
{
  static uint64_t freq = 0;

  if (freq == 0)
    freq = rte_get_tsc_hz();

  cycles *= 1000000ULL;
  cycles /= freq;
  return cycles;
}

uint32_t tas_qman_next_ts(struct qman_thread *t, uint32_t cur_ts)
{
  struct qman_appctx *qa;
  uint32_t i, idx, ret = -1U;
  int have_ret = 0;
  uint32_t ts = timestamp();
  uint32_t ret_ts;
  (void) cur_ts;

  for (i = 0; i < FLEXNIC_PL_APPCTX_NUM; i++) {
    qa = &t->appctx[i];
    ret_ts = qa->ts_virtual + (ts - qa->ts_real);

    if(qa->nolimit_head_idx != IDXLIST_INVAL) {
      return 0;
    }

    idx = qa->head_idx[0];
    if(idx == IDXLIST_INVAL) {
      continue;
    }

    struct queue *q = &t->queues[idx];
    uint32_t timeout;

    if(timestamp_lessthaneq(qa, q->next_ts, ret_ts)) {
      return 0;
    }

    timeout = rel_time(ret_ts, q->next_ts) / 1000;
    if (!have_ret || timeout < ret) {
      ret = timeout;
      have_ret = 1;
    }
  }

  return (have_ret ? ret : -1U);
}

int tas_qman_poll(struct qman_thread *t, unsigned num, unsigned *q_ids,
    uint16_t *q_bytes)
{
  unsigned i, x;
  uint16_t appctx;
  struct qman_appctx *qa;
  uint32_t ts = timestamp();

  if (num == 0) {
    return 0;
  }

  for (i = 0; i < FLEXNIC_PL_APPCTX_NUM; i++) {
    appctx = (t->appctx_next + i) % FLEXNIC_PL_APPCTX_NUM;
    qa = &t->appctx[appctx];

    x = poll_raw(t, qa, ts, num, q_ids, q_bytes);
    if (x > 0) {
      t->appctx_next = (appctx + 1) % FLEXNIC_PL_APPCTX_NUM;
      return x;
    }
  }

  t->appctx_next = (t->appctx_next + 1) % FLEXNIC_PL_APPCTX_NUM;
  return 0;
}

int tas_qman_poll_tenant(struct qman_thread *t, unsigned num,
    unsigned *q_ids, uint16_t *q_bytes, uint32_t tenant)
{
  unsigned i, x;
  uint16_t appctx;
  struct qman_appctx *qa;
  struct flextcp_pl_appctx *actx;
  uint32_t ts = timestamp();

  if (num == 0) {
    return 0;
  }

  for (i = 0; i < FLEXNIC_PL_APPCTX_NUM; i++) {
    appctx = (t->appctx_next + i) % FLEXNIC_PL_APPCTX_NUM;
    actx = &fp_state->appctx[t->id][appctx];
    if (actx->tx_len == 0 || actx->appst_id != tenant) {
      continue;
    }

    qa = &t->appctx[appctx];
    x = poll_raw(t, qa, ts, num, q_ids, q_bytes);
    if (x > 0) {
      t->appctx_next = (appctx + 1) % FLEXNIC_PL_APPCTX_NUM;
      return x;
    }
  }

  t->appctx_next = (t->appctx_next + 1) % FLEXNIC_PL_APPCTX_NUM;
  return 0;
}

int tas_qman_set(struct qman_thread *t, uint32_t id, uint32_t rate, uint32_t avail,
    uint16_t max_chunk, uint8_t flags)
{
#ifdef FLEXNIC_TRACE_QMAN
  struct flexnic_trace_entry_qman_set evt = {
      .id = id, .rate = rate, .avail = avail, .max_chunk = max_chunk,
      .flags = flags,
    };
  trace_event(FLEXNIC_TRACE_EV_QMSET, sizeof(evt), &evt);
#endif

  dprintf("qman_set: id=%u rate=%u avail=%u max_chunk=%u qidx=%u tid=%u\n",
      id, rate, avail, max_chunk, qidx, tid);

  if (id >= FLEXNIC_NUM_QMQUEUES) {
    fprintf(stderr, "qman_set: invalid queue id: %u >= %u\n", id,
        FLEXNIC_NUM_QMQUEUES);
    return -1;
  }

  set_impl(t, id, rate, avail, max_chunk, flags);

  return 0;
}

/** Actually update queue state: must run on queue's home core */
static void inline set_impl(struct qman_thread *t, uint32_t idx, uint32_t rate,
    uint32_t avail, uint16_t max_chunk, uint8_t flags)
{
  uint16_t appctx = queue_appctx(idx);
  struct qman_appctx *qa = &t->appctx[appctx];
  struct queue *q = &t->queues[idx];
  int new_avail = 0;

  if ((flags & QMAN_SET_RATE) != 0) {
    q->rate = rate;
  }

  if ((flags & QMAN_SET_MAXCHUNK) != 0) {
    q->max_chunk = max_chunk;
  }

  if ((flags & QMAN_SET_AVAIL) != 0) {
    q->avail = avail;
    new_avail = 1;
  } else if ((flags & QMAN_ADD_AVAIL) != 0) {
    q->avail += avail;
    new_avail = 1;
  }

  dprintf("set_impl: t=%p q=%p idx=%u avail=%u rate=%u qflags=%x flags=%x\n", t, q, idx, q->avail, q->rate, q->flags, flags);

  if (new_avail && q->avail > 0
      && ((q->flags & (FLAG_INSKIPLIST | FLAG_INNOLIMITL)) == 0)) {
    queue_activate(t, qa, q, idx);
  }
}

/*****************************************************************************/
/* Managing no-limit queues */

/** Add queue to the no limit list */
static inline void queue_activate_nolimit(struct qman_thread *t,
    struct qman_appctx *qa, struct queue *q, uint32_t idx)
{
  struct queue *q_tail;

  assert((q->flags & (FLAG_INSKIPLIST | FLAG_INNOLIMITL)) == 0);

  dprintf("queue_activate_nolimit: t=%p q=%p avail=%u rate=%u flags=%x\n", t, q, q->avail, q->rate, q->flags);

  q->flags |= FLAG_INNOLIMITL;
  q->next_idxs[0] = IDXLIST_INVAL;
  if (qa->nolimit_tail_idx == IDXLIST_INVAL) {
    qa->nolimit_head_idx = qa->nolimit_tail_idx = idx;
    return;
  }

  q_tail = &t->queues[qa->nolimit_tail_idx];
  q_tail->next_idxs[0] = idx;
  qa->nolimit_tail_idx = idx;
}

static inline int poll_nolimit_debug(struct qman_thread *t,
    struct qman_appctx *qa, uint32_t cur_ts, unsigned num, unsigned *q_ids,
    uint16_t *q_bytes)
{
  unsigned cnt;
  struct queue *q;
  uint32_t idx;
  
  for (cnt = 0; cnt < num && qa->nolimit_head_idx != IDXLIST_INVAL;) {
    idx = qa->nolimit_head_idx;
    q = t->queues + idx;

    qa->nolimit_head_idx = q->next_idxs[0];
    if (q->next_idxs[0] == IDXLIST_INVAL)
      qa->nolimit_tail_idx = IDXLIST_INVAL;

    q->flags &= ~FLAG_INNOLIMITL;
    dprintf("poll_nolimit: t=%p q=%p idx=%u avail=%u rate=%u flags=%x\n", t, q, idx, q->avail, q->rate, q->flags);
    if (q->avail > 0) {
      queue_fire(t, qa, q, idx, q_ids + cnt, q_bytes + cnt);
      cnt++;
    }
  }
  
  return cnt;
}

/** Poll no-limit queues */
static inline unsigned poll_nolimit(struct qman_thread *t,
    struct qman_appctx *qa, uint32_t cur_ts, unsigned num, unsigned *q_ids,
    uint16_t *q_bytes)
{
  unsigned cnt = 0;
  // unsigned cnt;
  // struct queue *q;
  // uint32_t idx;
  
  if (qa->nolimit_head_idx == IDXLIST_INVAL)
    return cnt;
    
  cnt = poll_nolimit_debug(t, qa, cur_ts, num, q_ids, q_bytes);
  
  // for (cnt = 0; cnt < num && qa->nolimit_head_idx != IDXLIST_INVAL;) {
  //   idx = qa->nolimit_head_idx;
  //   q = t->queues + idx;

  //   qa->nolimit_head_idx = q->next_idxs[0];
  //   if (q->next_idxs[0] == IDXLIST_INVAL)
  //     qa->nolimit_tail_idx = IDXLIST_INVAL;

  //   q->flags &= ~FLAG_INNOLIMITL;
  //   dprintf("poll_nolimit: t=%p q=%p idx=%u avail=%u rate=%u flags=%x\n", t, q, idx, q->avail, q->rate, q->flags);
  //   if (q->avail > 0) {
  //     queue_fire(t, qa, q, idx, q_ids + cnt, q_bytes + cnt);
  //     cnt++;
  //   }
  // }

  return cnt;
}

/*****************************************************************************/
/* Managing skiplist queues */

static inline uint32_t queue_new_ts(struct qman_appctx *qa, struct queue *q,
    uint32_t bytes)
{
  return qa->ts_virtual + ((uint64_t) bytes * 8 * 1000000) / q->rate;
}

/** Add queue to the skip list list */
static inline void queue_activate_skiplist(struct qman_thread *t,
    struct qman_appctx *qa, struct queue *q, uint32_t q_idx)
{
  uint8_t level;
  int8_t l;
  uint32_t preds[QMAN_SKIPLIST_LEVELS];
  uint32_t pred, idx, ts, max_ts;

  assert((q->flags & (FLAG_INSKIPLIST | FLAG_INNOLIMITL)) == 0);

  dprintf("queue_activate_skiplist: t=%p q=%p idx=%u avail=%u rate=%u flags=%x ts_virt=%u next_ts=%u\n", t, q, q_idx, q->avail, q->rate, q->flags,
      qa->ts_virtual, q->next_ts);

  /* make sure queue has a reasonable next_ts:
   *  - not in the past
   *  - not more than if it just sent max_chunk at the current rate
   */
  ts = q->next_ts;
  max_ts = queue_new_ts(qa, q, q->max_chunk);
  if (timestamp_lessthaneq(qa, ts, qa->ts_virtual)) {
    ts = q->next_ts = qa->ts_virtual;
  } else if (!timestamp_lessthaneq(qa, ts, max_ts)) {
    ts = q->next_ts = max_ts;
  }
  q->next_ts = ts;

  /* find predecessors at all levels top-down */
  pred = IDXLIST_INVAL;
  for (l = QMAN_SKIPLIST_LEVELS - 1; l >= 0; l--) {
    idx = (pred != IDXLIST_INVAL ? pred : qa->head_idx[l]);
    while (idx != IDXLIST_INVAL &&
        timestamp_lessthaneq(qa, t->queues[idx].next_ts, ts))
    {
      pred = idx;
      idx = t->queues[idx].next_idxs[l];
    }
    preds[l] = pred;
    dprintf("    pred[%u] = %d\n", l, pred);
  }

  /* determine level for this queue */
  level = queue_level(t);
  dprintf("    level = %u\n", level);

  /* insert into skip-list */
  for (l = QMAN_SKIPLIST_LEVELS - 1; l >= 0; l--) {
    if (l > level) {
      q->next_idxs[l] = IDXLIST_INVAL;
    } else {
      idx = preds[l];
      if (idx != IDXLIST_INVAL) {
        q->next_idxs[l] = t->queues[idx].next_idxs[l];
        t->queues[idx].next_idxs[l] = q_idx;
      } else {
        q->next_idxs[l] = qa->head_idx[l];
        qa->head_idx[l] = q_idx;
      }
    }
  }

  q->flags |= FLAG_INSKIPLIST;
}

/** Poll skiplist queues */
static inline unsigned poll_skiplist(struct qman_thread *t,
    struct qman_appctx *qa, uint32_t cur_ts, unsigned num, unsigned *q_ids,
    uint16_t *q_bytes)
{
  unsigned cnt;
  uint32_t idx, max_vts;
  int8_t l;
  struct queue *q;

  /* maximum virtual time stamp that can be reached */
  max_vts = qa->ts_virtual + (cur_ts - qa->ts_real);

  for (cnt = 0; cnt < num;) {
    idx = qa->head_idx[0];

    /* no more queues */
    if (idx == IDXLIST_INVAL) {
      qa->ts_virtual = max_vts;
      break;
    }

    q = &t->queues[idx];

    /* beyond max_vts */
    dprintf("poll_skiplist: next_ts=%u vts=%u rts=%u max_vts=%u cur_ts=%u\n",
        q->next_ts, qa->ts_virtual, qa->ts_real, max_vts, cur_ts);
    if (!timestamp_lessthaneq(qa, q->next_ts, max_vts)) {
      qa->ts_virtual = max_vts;
      break;
    }

    /* remove queue from skiplist */
    for (l = 0; l < QMAN_SKIPLIST_LEVELS && qa->head_idx[l] == idx; l++) {
      qa->head_idx[l] = q->next_idxs[l];
    }
    assert((q->flags & FLAG_INSKIPLIST) != 0);
    q->flags &= ~FLAG_INSKIPLIST;

    /* advance virtual timestamp */
    qa->ts_virtual = q->next_ts;

    dprintf("poll_skiplist: t=%p q=%p idx=%u avail=%u rate=%u flags=%x\n", t, q, idx, q->avail, q->rate, q->flags);

    if (q->avail > 0) {
      queue_fire(t, qa, q, idx, q_ids + cnt, q_bytes + cnt);
      cnt++;
    }
  }

  /* if we reached the limit, update the virtual timestamp correctly */
  if (cnt == num) {
    idx = qa->head_idx[0];
    if (idx != IDXLIST_INVAL &&
        timestamp_lessthaneq(qa, t->queues[idx].next_ts, max_vts))
    {
      qa->ts_virtual = t->queues[idx].next_ts;
    } else {
      qa->ts_virtual = max_vts;
    }
  }

  qa->ts_real = cur_ts;
  return cnt;
}

/** Level for queue added to skiplist */
static inline uint8_t queue_level(struct qman_thread *t)
{
  uint8_t x = (__builtin_ffs(utils_rng_gen32(&t->rng)) - 1) / SKIPLIST_BITS;
  return (x < QMAN_SKIPLIST_LEVELS ? x : QMAN_SKIPLIST_LEVELS - 1);
}

/*****************************************************************************/

static inline void queue_fire(struct qman_thread *t, struct qman_appctx *qa,
    struct queue *q, uint32_t idx, unsigned *q_id, uint16_t *q_bytes)
{
  uint32_t bytes;

  assert(q->avail > 0);

  bytes = (q->avail <= q->max_chunk ? q->avail : q->max_chunk);
  q->avail -= bytes;

  dprintf("queue_fire: t=%p q=%p idx=%u gidx=%u bytes=%u avail=%u rate=%u\n", t, q, idx, idx, bytes, q->avail, q->rate);
  if (q->rate > 0) {
    q->next_ts = queue_new_ts(qa, q, bytes);
  }

  if (q->avail > 0) {
    queue_activate(t, qa, q, idx);
  }

  *q_bytes = bytes;
  *q_id = idx;

#ifdef FLEXNIC_TRACE_QMAN
  struct flexnic_trace_entry_qman_event evt = {
      .id = *q_id, .bytes = bytes,
    };
  trace_event(FLEXNIC_TRACE_EV_QMEVT, sizeof(evt), &evt);
#endif
}

static inline void queue_activate(struct qman_thread *t,
    struct qman_appctx *qa, struct queue *q, uint32_t idx)
{
  if (q->rate == 0) {
    queue_activate_nolimit(t, qa, q, idx);
  } else {
    queue_activate_skiplist(t, qa, q, idx);
  }
}

static inline uint32_t timestamp(void)
{
  static uint64_t freq = 0;
  uint64_t cycles = rte_get_tsc_cycles();

  if (freq == 0)
    freq = rte_get_tsc_hz();

  cycles *= 1000000000ULL;
  cycles /= freq;
  return cycles;
}

/** Relative timestamp, ignoring wrap-arounds */
static inline int64_t rel_time(uint32_t cur_ts, uint32_t ts_in)
{
  uint64_t ts = ts_in;
  const uint64_t middle = (1ULL << (TIMESTAMP_BITS - 1));
  uint64_t start, end;

  if (cur_ts < middle) {
    /* negative interval is split in half */
    start = (cur_ts - middle) & TIMESTAMP_MASK;
    end = (1ULL << TIMESTAMP_BITS);
    if (start <= ts && ts < end) {
      /* in first half of negative interval, smallest timestamps */
      return ts - start - middle;
    } else {
      /* in second half or in positive interval */
      return ts - cur_ts;
    }
  } else if (cur_ts == middle) {
    /* intervals not split */
    return ts - cur_ts;
  } else {
    /* higher interval is split */
    start = 0;
    end = ((cur_ts + middle) & TIMESTAMP_MASK) + 1;
    if (start <= cur_ts && ts < end) {
      /* in second half of positive interval, largest timestamps */
      return ts + ((1ULL << TIMESTAMP_BITS) - cur_ts);
    } else {
      /* in negative interval or first half of positive interval */
      return ts - cur_ts;
    }
  }
}

static inline int timestamp_lessthaneq(struct qman_appctx *qa, uint32_t a,
    uint32_t b)
{
  return rel_time(qa->ts_virtual, a) <= rel_time(qa->ts_virtual, b);
}

static inline unsigned poll_raw(struct qman_thread *t, struct qman_appctx *qa,
    uint32_t ts, unsigned num, unsigned *q_ids, uint16_t *q_bytes)
{
  unsigned x, y;

  /* poll nolimit list and skiplist alternating the order between */
  if (qa->nolimit_first) {
    x = poll_nolimit(t, qa, ts, num, q_ids, q_bytes);
    y = poll_skiplist(t, qa, ts, num - x, q_ids + x, q_bytes + x);
  } else {
    x = poll_skiplist(t, qa, ts, num, q_ids, q_bytes);
    y = poll_nolimit(t, qa, ts, num - x, q_ids + x, q_bytes + x);
  }
  qa->nolimit_first = !qa->nolimit_first;

  return x + y;
}

static inline uint16_t queue_appctx(unsigned q_id)
{
  if (q_id >= FLEXNIC_PL_FLOWST_NUM) {
    return 0;
  }

  uint16_t appctx = fp_state->flowst[q_id].db_id;

  if (appctx >= FLEXNIC_PL_APPCTX_NUM) {
    appctx %= FLEXNIC_PL_APPCTX_NUM;
  }

  return appctx;
}
