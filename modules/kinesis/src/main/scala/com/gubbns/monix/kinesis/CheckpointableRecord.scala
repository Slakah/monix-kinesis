package com.gubbns.monix.kinesis

import cats.effect.Sync
import software.amazon.kinesis.processor.PreparedCheckpointer
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

final case class CheckpointableRecord(
  shardId: String,
  millisBehindLatest: Long,
  record: KinesisClientRecord,
  recordProcessor: RecordProcessorSubscriber,
  checkpointer: PreparedCheckpointer
) {
  def extendedSequenceNumber: ExtendedSequenceNumber = new ExtendedSequenceNumber(record.sequenceNumber(), record.subSequenceNumber())
  def sequenceNumber: String = record.sequenceNumber()
  def subSequenceNumber: Long = record.subSequenceNumber()
  def canCheckpoint: Boolean = !recordProcessor.isShutdown

  def checkpoint[F[_]: Sync](): F[Unit] = Sync[F].delay(checkpointer.checkpoint())
}
