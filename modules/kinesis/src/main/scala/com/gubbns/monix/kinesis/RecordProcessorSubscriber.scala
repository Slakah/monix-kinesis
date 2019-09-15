package com.gubbns.monix.kinesis

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import monix.execution.{Ack, Cancelable, Scheduler}
import monix.reactive.Observer
import monix.reactive.observers.Subscriber
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.ShardRecordProcessor
import software.amazon.kinesis.retrieval.KinesisClientRecord

private[kinesis] final class RecordProcessorSubscriber(
  out: Subscriber[CheckpointableRecord],
  terminateGracePeriod: FiniteDuration,
  cancel: Cancelable
) extends ShardRecordProcessor {

  private implicit val s: Scheduler = out.scheduler

  // scalafix:off DisableSyntax.var
  private var shardId: String = _
  private[kinesis] var isShutdown = false
  // scalafix:on

  override def initialize(initializationInput: InitializationInput): Unit =
    shardId = initializationInput.shardId()

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    val checkpointableRecords = ArrayBuffer[CheckpointableRecord]()
    processRecordsInput.records().asScala.foreach { record =>
      checkpointableRecords += prepareCheckpointableRecord(record, processRecordsInput)
    }

    try {
      val ackFuture = Observer.feed(out, checkpointableRecords)
      ackFuture.syncOnComplete {
        case Success(Ack.Continue) => ()
        case Success(Ack.Stop) => cancel.cancel()
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

  private def prepareCheckpointableRecord(record: KinesisClientRecord, processRecordsInput: ProcessRecordsInput) = {
    val preparedCheckpointer =
      processRecordsInput.checkpointer().prepareCheckpoint(record.sequenceNumber(), record.subSequenceNumber())
    CheckpointableRecord(
      shardId = shardId,
      millisBehindLatest = processRecordsInput.millisBehindLatest(),
      record = record,
      recordProcessor = this,
      checkpointer = preparedCheckpointer
    )
  }

  override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {}

  override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
    isShutdown = true
    Thread.sleep(terminateGracePeriod.toMillis)
    shardEndedInput.checkpointer().checkpoint()
  }

  override def shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput): Unit = {
    isShutdown = true
    Thread.sleep(terminateGracePeriod.toMillis)
    shutdownRequestedInput.checkpointer().checkpoint()
  }
}
