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

final private[kinesis] class ShardRecordProcessorAutoCheckpoint(
  out: Subscriber[KinesisClientRecord],
  cancel: Cancelable
) extends ShardRecordProcessor {

  implicit private val s: Scheduler = out.scheduler

  override def initialize(initializationInput: InitializationInput): Unit = {}

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    val records = processRecordsInput.records().asScala
    try {
      processRecordsInput.checkpointer().prepareCheckpoint()
      val ackFuture = Observer.feed(out, records)
      ackFuture.syncOnComplete {
        case Success(Ack.Continue) =>
          processRecordsInput.checkpointer().checkpoint()
        case Success(Ack.Stop) =>
          processRecordsInput.checkpointer().checkpoint()
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
