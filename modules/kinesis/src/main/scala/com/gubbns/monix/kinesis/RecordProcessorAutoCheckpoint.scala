package com.gubbns.monix.kinesis

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import monix.execution.{Ack, Cancelable, Scheduler}
import monix.reactive.Observer
import monix.reactive.observers.Subscriber
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.ShardRecordProcessor
import software.amazon.kinesis.retrieval.KinesisClientRecord

private[kinesis] final class RecordProcessorAutoCheckpoint(
  out: Subscriber[KinesisClientRecord],
  cancel: Cancelable
) extends ShardRecordProcessor {

  private implicit val s: Scheduler = out.scheduler

  override def initialize(initializationInput: InitializationInput): Unit = {}

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    val records = processRecordsInput.records().asScala
    try {
      // TODO: settings for individual checkpointing of records
      val preparedCheckpointer = processRecordsInput.checkpointer().prepareCheckpoint()
      val ackFuture = Observer.feed(out, records)
      ackFuture.syncOnComplete {
        case Success(Ack.Continue) =>
          preparedCheckpointer.checkpoint()
        case Success(Ack.Stop) =>
          preparedCheckpointer.checkpoint()
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
