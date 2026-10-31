# Reliability

## Delivery semantics

The pipeline gives **at-least-once delivery with idempotent writes**: every valid event affects the analytics exactly once, even though Kafka may deliver it more than once.

It does **not** implement Kafka exactly-once semantics (transactional producer plus `read_committed` consumer).
- Those transactions only cover writes to Kafka, and this pipeline's results live in PostgreSQL.
- The guarantee comes from the database instead. `processed_events.event_id` is the primary key, and the rollups are updated in the same transaction, only for ids that insert succeeded for.

The ordering inside the consumer is what makes this hold:

1. Validate the batch.
2. Run **one PostgreSQL transaction**:
   - insert event ids with `ON CONFLICT DO NOTHING RETURNING`;
   - update rollups for the returned (new) ids;
   - record dead letters.
3. Publish dead letters to the DLQ topic and wait for broker acknowledgement.
4. **Commit offsets synchronously.**

What a failure at each point leads to:

| Crash point | Database state | Kafka offsets | On restart |
|---|---|---|---|
| Before step 2 commits | rolled back | not committed | batch redelivered and processed normally |
| After step 2, before step 4 | committed | not committed | batch redelivered; every id is already stored and counted as a duplicate; rollups unchanged |
| After step 4 | committed | committed | continues with the next batch |

**What can still go wrong:**
- **Exceptional DLQ duplicates.** A crash between steps 3 and 4 can publish the same dead letter to the DLQ topic twice. The `dead_letters` table stays unique on `(topic, partition, offset)`, and each DLQ message carries its source coordinates in headers, so a downstream reader can de-duplicate.
- **Duplicates with different ids.** Two different `event_id`s describing the same real-world event cannot be recognised as duplicates. De-duplication is by event identity, not content.

## Failure modes and how each is tested

| Failure | Behaviour | Evidence |
|---|---|---|
| Duplicate publish or producer retry | Stored once, counted once | `test_end_to_end_counts_match_ground_truth`: duplicates injected by the producer equal `stats.duplicates`, and database totals equal ground truth |
| Crash after DB commit, before offset commit | Redelivered, no double counting | `test_crash_before_offset_commit_redelivers_without_double_counting` |
| Normal restart | Resumes from committed offsets, nothing redelivered | `test_restart_resumes_from_committed_offsets` (`duplicates == 0`) |
| `SIGKILL` during live traffic (compose stack) | Nothing lost, nothing double-counted | Manual run recorded in [testing](testing.md#crash-recovery-in-the-compose-stack) |
| Transient DB error | Retried with exponential backoff (`STREAM_DB_MAX_RETRIES`, `STREAM_DB_RETRY_BASE_DELAY_S`) | `test_transient_database_errors_are_retried` (`db_retries == 2`, totals correct) |
| DB outage longer than the retries | Consumer exits **without committing**; a later run catches up | `test_database_outage_stops_consumer_without_losing_events` |
| Malformed JSON or schema violation | Dead-lettered with reason, partition keeps flowing | end-to-end test checks the DLQ count and headers |
| Event timestamp far in the future | Dead-lettered as `future_timestamp` | `test_future_timestamp_is_dead_lettered` |
| Shipment delivery before dispatch | Same final row in either order | `test_shipment_state_is_correct_in_any_order` |
| Two consumers in one group | Partitions split; every event stored once | `test_two_consumers_in_one_group_share_partitions` |
| Rolled-back transaction | Nothing partially written | `test_failed_transaction_leaves_nothing_behind` |
| API database outage | `/health/live` stays 200; readiness and data endpoints return 503 | `test_database_outage_returns_503` |

## Ordering

Kafka only orders messages within a partition.
- Shipments are keyed by `shipment_id`, so a dispatch normally precedes its delivery.
- The store does **not** rely on that, because replays, DLQ re-injection or a producer bug could reorder them.
- Dispatch and delivery upserts write disjoint columns of the same row, so either arrival order gives the same result.
- A delivery whose dispatch has not been seen is reported by the API as `deliveries_without_dispatch` rather than hidden.

Rollups are keyed by event-time minute, so late events are attributed to the minute they occurred.

## Graceful shutdown and rebalancing

- `SIGTERM` and `SIGINT` set a stop flag. The consumer finishes the current batch, then closes, which leaves the group and triggers a prompt rebalance.
- With `cooperative-sticky` assignment, surviving members keep their partitions during a rebalance.
- `max.poll.interval.ms` (300 s) must stay above the worst-case batch time, including database retries. Otherwise the member is evicted mid-batch; the batch is then redelivered to another member and de-duplicated.

**Observed trade-off.** After a `SIGKILL`, the dead member does not leave the group cleanly. Its partitions stay assigned until `session.timeout.ms` (45 s) expires, so processing pauses for up to that long. A lower timeout recovers faster but risks evicting healthy members during GC pauses or network blips.

## Data quality guarantees

| Guarantee | Mechanism |
|---|---|
| No event counted twice | `event_id` primary key; rollups only for newly inserted ids, in the same transaction |
| No silent drops | Every message is either stored or dead-lettered before its offset is committed |
| Dead letters recorded once | Unique `(topic, partition, offset)` |
| Rates never divide by zero | API returns `0.0` for an empty denominator, or `null` for on-time rate when nothing has been assessed |
