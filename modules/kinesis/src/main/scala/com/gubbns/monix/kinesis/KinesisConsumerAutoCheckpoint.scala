package com.gubbns.monix.kinesis

import scala.util.control.NonFatal

import monix.execution.Cancelable
import monix.execution.cancelables.SingleAssignCancelable
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import software.amazon.kinesis.coordinator.{Scheduler => KScheduler}
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.KinesisClientRecord

private[kinesis] final class KinesisConsumerAutoCheckpoint(
  f: ShardRecordProcessorFactory => KScheduler
) extends Observable[KinesisClientRecord] {

  override def unsafeSubscribeFn(out: Subscriber[KinesisClientRecord]): Cancelable = {
    val cancel = SingleAssignCancelable()
    val kScheduler = f(() => new RecordProcessorAutoCheckpoint(out, cancel))
    val gracefulShutdown = kScheduler.createGracefulShutdownCallable()
    cancel := Cancelable { () =>
      val _ = gracefulShutdown.call()
      ()
    }

    out.scheduler.executeAsync { () =>
      try {
        kScheduler.run()
        out.onComplete() // indicate the worker has exited
      } catch {
        case NonFatal(ex) =>
          out.onError(ex)
      }
    }

    cancel
  }
}
