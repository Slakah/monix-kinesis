package com.gubbns.monix.kinesis

import scala.concurrent.blocking
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import monix.execution.Cancelable
import monix.reactive.observers.Subscriber
import monix.reactive.Observable
import software.amazon.kinesis.coordinator.{Scheduler => KScheduler}
import software.amazon.kinesis.processor.ShardRecordProcessorFactory

final class KinesisConsumerManualCheckpoint(
  f: ShardRecordProcessorFactory => KScheduler,
  terminateGracePeriod: FiniteDuration
) extends Observable[CheckpointableRecord] {

  override def unsafeSubscribeFn(out: Subscriber[CheckpointableRecord]): Cancelable = {
    lazy val kSchedulerAndCancel: (KScheduler, Cancelable) = {
      val cancel = Cancelable(() => blocking(kSchedulerAndCancel._1.shutdown()))
      val kScheduler = f(() => new RecordProcessorSubscriber(out, terminateGracePeriod, cancel))
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
