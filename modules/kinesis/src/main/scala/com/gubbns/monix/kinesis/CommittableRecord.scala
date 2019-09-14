package com.gubbns.monix.kinesis

import cats.effect.Sync
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

final case class CommittableRecord(
  shardId: String,
  recordProcessorStartingSequenceNumber: ExtendedSequenceNumber,
  millisBehindLatest: Long,
  record: KinesisClientRecord,
  recordProcessor: ShardRecordProcessorSubscriber,
  checkpointer: RecordProcessorCheckpointer
) {
  def sequenceNumber: String = record.sequenceNumber()
  def subSequenceNumber: Long = record.subSequenceNumber()
  def canCheckpoint: Boolean = !recordProcessor.isShutdown

  def checkpoint[F[_]: Sync](): F[Unit] =
    Sync[F].delay(checkpointer.checkpoint(record.sequenceNumber(), record.subSequenceNumber()))
}
