package com.gubbns.monix.kinesis

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import monix.execution.cancelables.BooleanCancelable
import monix.execution.{Ack, Scheduler}
import monix.reactive.Observer
import monix.reactive.observers.Subscriber
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.ShardRecordProcessor
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

private[kinesis] final class RecordProcessorAutoCheckpoint(
  out: Subscriber[KinesisClientRecord],
  cancel: BooleanCancelable
) extends ShardRecordProcessor {

  private implicit val s: Scheduler = out.scheduler

  // scalafix:off DisableSyntax.var
  private var pendingCheckpointSequenceNumber: ExtendedSequenceNumber = ExtendedSequenceNumber.TRIM_HORIZON
  // scalafix:on

  override def initialize(initializationInput: InitializationInput): Unit =
    pendingCheckpointSequenceNumber = Option(initializationInput.pendingCheckpointSequenceNumber())
      .getOrElse(ExtendedSequenceNumber.TRIM_HORIZON)

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit =
    if (!cancel.isCanceled) {
      val records = processRecordsInput
        .records()
        .asScala
        .filter { record =>
          val sequenceNumber = new ExtendedSequenceNumber(record.sequenceNumber(), record.subSequenceNumber())
          sequenceNumber.compareTo(pendingCheckpointSequenceNumber) > 0
        }
      try {
        // TODO: settings for individual checkpointing of records
        val preparedCheckpointer = processRecordsInput.checkpointer().prepareCheckpoint()
        val ackFuture = Observer.feed(out, records)
        ackFuture.syncOnComplete {
          case Success(Ack.Continue) =>
            // TODO: handle checkpoint throttling
            preparedCheckpointer.checkpoint()
          case Success(Ack.Stop) =>
            // TODO: handle checkpoint throttling
//            preparedCheckpointer.checkpoint()
            cancel.cancel()
          case Failure(ex) =>
            s.reportFailure(ex)
            cancel.cancel()
        }
      } catch {
        case NonFatal(ex) =>
          s.reportFailure(ex)
          cancel.cancel()
      }
    }

  override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {}

  override def shardEnded(shardEndedInput: ShardEndedInput): Unit =
    shardEndedInput.checkpointer().checkpoint()

  override def shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput): Unit = {}
}
