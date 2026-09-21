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

#ifndef FASTPATH_H_
#define FASTPATH_H_

#include <stdbool.h>
#include <stdint.h>

#include <rte_interrupts.h>

#include <tas_memif.h>
#include <utils_rng.h>

#define BATCH_SIZE 16
#define BUFCACHE_SIZE 128
#define TXBUF_SIZE (2 * BATCH_SIZE)


struct network_thread {
  struct rte_mempool *pool;
  uint16_t queue_id;
};

/** Skiplist: #levels */
#define QMAN_SKIPLIST_LEVELS 4

struct qman_appctx {
  uint32_t head_idx[QMAN_SKIPLIST_LEVELS];
  uint32_t nolimit_head_idx;
  uint32_t nolimit_tail_idx;
  uint32_t ts_real;
  uint32_t ts_virtual;
  bool nolimit_first;
};

struct qman_thread {
  /************************************/
  /* read-only */
  struct queue *queues;
  uint16_t id;

  /************************************/
  /* modified by owner thread */
  struct qman_appctx appctx[FLEXNIC_PL_APPCTX_NUM];
  uint16_t appctx_next;
  struct utils_rng rng;
};

struct dataplane_batch_stats {
  uint64_t rx_polls;
  uint64_t rx_total;
  uint64_t qm_polls;
  uint64_t qm_total;
  uint64_t qs_polls;
  uint64_t qs_total;
};


struct dataplane_context {
  struct network_thread net;
  struct qman_thread qman;
  struct rte_ring *qman_fwd_ring;
  uint16_t id;
  int evfd;
  struct rte_epoll_event ev;

  /********************************************************/
  /* arx cache */
  struct flextcp_pl_arx arx_cache[BATCH_SIZE];
  uint16_t arx_ctx[BATCH_SIZE];
  uint16_t arx_num;

  /********************************************************/
  /* send buffer */
  struct network_buf_handle *tx_handles[TXBUF_SIZE];
  uint16_t tx_num;

  /********************************************************/
  /* polling queues */
  uint32_t poll_next_ctx;

  /********************************************************/
  /* pre-allocated buffers for polling doorbells and queue manager */
  struct network_buf_handle *bufcache_handles[BUFCACHE_SIZE];
  uint16_t bufcache_num;
  uint16_t bufcache_head;

  uint64_t loadmon_cyc_busy;

  uint64_t kernel_drop;
#ifdef BATCH_SIZE_STATS
  uint64_t stat_batch_rx_polls;
  uint64_t stat_batch_rx_total;
  uint64_t stat_batch_qm_polls;
  uint64_t stat_batch_qm_total;
  uint64_t stat_batch_qs_polls;
  uint64_t stat_batch_qs_total;
#endif
#ifdef DATAPLANE_STATS
  /********************************************************/
  /* Stats */
  uint64_t stat_qm_poll;
  uint64_t stat_qm_empty;
  uint64_t stat_qm_total;

  uint64_t stat_rx_poll;
  uint64_t stat_rx_empty;
  uint64_t stat_rx_total;

  uint64_t stat_qs_poll;
  uint64_t stat_qs_empty;
  uint64_t stat_qs_total;

  uint64_t stat_cyc_db;
  uint64_t stat_cyc_qm;
  uint64_t stat_cyc_rx;
  uint64_t stat_cyc_qs;

  uint64_t stat_cycw_db;
  uint64_t stat_cycw_qm;
  uint64_t stat_cycw_rx;
  uint64_t stat_cycw_qs;
#endif
};

extern struct dataplane_context **ctxs;

int dataplane_init(void);
int dataplane_context_init(struct dataplane_context *ctx);
void dataplane_context_destroy(struct dataplane_context *ctx);
void dataplane_loop(struct dataplane_context *ctx);
#ifdef BATCH_SIZE_STATS
void dataplane_batch_stats_collect(struct dataplane_batch_stats *stats);
#else
static inline void dataplane_batch_stats_collect(
    struct dataplane_batch_stats *stats)
{
  (void) stats;
}
#endif
void dataplane_dump_stats(void);

#endif /* ndef FASTPATH_H_ */
