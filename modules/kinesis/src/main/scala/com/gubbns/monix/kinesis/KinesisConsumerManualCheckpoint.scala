package com.gubbns.monix.kinesis

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import monix.execution.Cancelable
import monix.execution.cancelables.SingleAssignCancelable
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import software.amazon.kinesis.coordinator.{Scheduler => KScheduler}
import software.amazon.kinesis.processor.ShardRecordProcessorFactory

private[kinesis] final class KinesisConsumerManualCheckpoint(
  f: ShardRecordProcessorFactory => KScheduler,
  terminateGracePeriod: FiniteDuration
) extends Observable[CheckpointableRecord] {

  override def unsafeSubscribeFn(out: Subscriber[CheckpointableRecord]): Cancelable = {
    val cancel = SingleAssignCancelable()
    val kScheduler = f(() => new RecordProcessorSubscriber(out, terminateGracePeriod, cancel))
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
