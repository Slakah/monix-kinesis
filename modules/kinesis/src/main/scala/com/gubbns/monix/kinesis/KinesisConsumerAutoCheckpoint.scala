package com.gubbns.monix.kinesis

import scala.concurrent.blocking
import scala.util.control.NonFatal

import monix.execution.Cancelable
import monix.execution.cancelables.BooleanCancelable
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import software.amazon.kinesis.coordinator.{Scheduler => KScheduler}
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.KinesisClientRecord

private[kinesis] final class KinesisConsumerAutoCheckpoint(
  f: () => ShardRecordProcessorFactory => KScheduler
) extends Observable[KinesisClientRecord] {

  override def unsafeSubscribeFn(out: Subscriber[KinesisClientRecord]): Cancelable = {

    lazy val kSchedulerAndCancel: (KScheduler, Cancelable) = {
      val cancel = BooleanCancelable(
        () => blocking { val _ = kSchedulerAndCancel._1.createGracefulShutdownCallable().call() }
      )
      val kScheduler = f()(() => new RecordProcessorAutoCheckpoint(out, cancel))
      (kScheduler, cancel)
    }
    val (kScheduler, cancel) = kSchedulerAndCancel

    out.scheduler.executeAsync { () =>
      try {
        blocking(kScheduler.run())
        out.onComplete() // Indicate the worker has exited
      } catch {
        case NonFatal(ex) =>
          out.onError(ex)
      }
    }

    cancel
  }
}
