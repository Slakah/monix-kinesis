package com.gubbns.monix.kinesis

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
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

final private[kinesis] class ShardRecordProcessorSubscriber(
  out: Subscriber[CommittableRecord],
  terminateGracePeriod: FiniteDuration,
  cancel: Cancelable
) extends ShardRecordProcessor {

  implicit private val s: Scheduler = out.scheduler

  // scalafix:off DisableSyntax.var
  private var shardId: String = _
  private[kinesis] var isShutdown = false
  // scalafix:on

  override def initialize(initializationInput: InitializationInput): Unit =
    shardId = initializationInput.shardId()

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    val commitableRecords = processRecordsInput.records().asScala.map(recordToCommittableRecord(processRecordsInput, _))
    try {
      val ackFuture = Observer.feed(out, commitableRecords)
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

  private def recordToCommittableRecord(
    processRecordsInput: ProcessRecordsInput,
    record: KinesisClientRecord
  ): CommittableRecord =
    CommittableRecord(
      shardId = shardId,
      recordProcessorStartingSequenceNumber =
        new ExtendedSequenceNumber(record.sequenceNumber(), record.subSequenceNumber()),
      millisBehindLatest = processRecordsInput.millisBehindLatest(),
      record = record,
      recordProcessor = this,
      checkpointer = processRecordsInput.checkpointer()
    )

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
