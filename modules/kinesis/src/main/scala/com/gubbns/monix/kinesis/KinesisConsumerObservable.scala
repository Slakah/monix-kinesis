package com.gubbns.monix.kinesis

import scala.concurrent.blocking
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import cats.effect.{IO, Resource}
import cats.implicits._
import monix.execution.Cancelable
import monix.reactive.observers.Subscriber
import monix.reactive.{Consumer, Observable}
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common._
import software.amazon.kinesis.coordinator.{Scheduler => KScheduler}
import software.amazon.kinesis.metrics.MetricsFactory
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.polling.PollingConfig

final class KinesisConsumerObservable(
  f: ShardRecordProcessorFactory => KScheduler,
  terminateGracePeriod: FiniteDuration
) extends Observable[CommittableRecord] {

  override def unsafeSubscribeFn(out: Subscriber[CommittableRecord]): Cancelable = {
    lazy val kSchedulerAndCancel: (KScheduler, Cancelable) = {
      val cancel = Cancelable(() => blocking(kSchedulerAndCancel._1.shutdown()))
      val kScheduler = f(() => new ShardRecordProcessorSubscriber(out, terminateGracePeriod, cancel))
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

object KinesisConsumerObservable {

  def apply(
    streamName: String,
    applicationName: String,
    workerIdentifier: String,
    tableName: String
  ): Observable[KinesisClientRecord] = {
    val makeClients = (makeKinesisClient, makeDynamoDbClient, makeCloudWatchClient).tupled
    Observable.fromResource(makeClients).flatMap {
      case (kinesis, dynamoDb, cloudWatch) =>
        apply(
          streamName,
          applicationName,
          workerIdentifier,
          tableName,
          kinesis,
          dynamoDb,
          cloudWatch,
          metricsFactory = None
        )
    }
  }

  private def makeKinesisClient: Resource[IO, KinesisAsyncClient] =
    Resource.fromAutoCloseable(IO(KinesisAsyncClient.create()))
  private def makeDynamoDbClient: Resource[IO, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable(IO(DynamoDbAsyncClient.create()))
  private def makeCloudWatchClient: Resource[IO, CloudWatchAsyncClient] =
    Resource.fromAutoCloseable(IO(CloudWatchAsyncClient.create()))

  def apply(
    streamName: String,
    applicationName: String,
    workerIdentifier: String,
    tableName: String,
    kinesis: KinesisAsyncClient,
    dynamoDb: DynamoDbAsyncClient,
    cloudWatch: CloudWatchAsyncClient,
    metricsFactory: Option[MetricsFactory]
  ): Observable[KinesisClientRecord] = {
    def kScheduler(factory: ShardRecordProcessorFactory) = {
      val configsBuilder = new ConfigsBuilder(
        streamName,
        applicationName,
        kinesis,
        dynamoDb,
        cloudWatch,
        workerIdentifier,
        factory
      ).tableName(tableName)

      new KScheduler(
        configsBuilder.checkpointConfig(),
        configsBuilder.coordinatorConfig(),
        configsBuilder.leaseManagementConfig(),
        configsBuilder.lifecycleConfig(),
        metricsFactory match {
          case None => configsBuilder.metricsConfig()
          case Some(mFactory) => configsBuilder.metricsConfig().metricsFactory(mFactory)
        },
        configsBuilder.processorConfig(),
        configsBuilder
          .retrievalConfig()
          .retrievalSpecificConfig(new PollingConfig(streamName, kinesis))
          .initialPositionInStreamExtended(
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
          )
      )
    }
    apply(kScheduler)
  }

  def apply(f: ShardRecordProcessorFactory => KScheduler): Observable[KinesisClientRecord] =
    new KinesisConsumerObservableAutoCheckpoint(f)

  def checkpoint(
    obs: Observable[CommittableRecord],
    timespan: FiniteDuration,
    maxCount: Int
  ): Observable[Unit] =
    obs
      .groupBy(_.shardId)
      .mapEval { group =>
        group
          .bufferTimedAndCounted(timespan, maxCount)
          .map(_.maxBy(_.recordProcessorStartingSequenceNumber))
          .consumeWith(Consumer.foreachEval { cr =>
            if (cr.canCheckpoint) {
              cr.checkpoint[IO]()
            } else {
              IO.unit
            }
          })
      }
}
