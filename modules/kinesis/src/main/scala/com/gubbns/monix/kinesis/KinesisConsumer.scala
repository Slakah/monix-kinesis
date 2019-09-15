package com.gubbns.monix.kinesis

import scala.concurrent.duration._

import cats.effect.{IO, Resource}
import cats.implicits._
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

object KinesisConsumer {

  def apply(
    streamName: String,
    applicationName: String,
    workerIdentifier: String,
    tableName: String
  ): Observable[KinesisClientRecord] =
    Observable.fromResource(makeAwsClients).flatMap {
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

  def apply(
    streamName: String,
    applicationName: String,
    workerIdentifier: String,
    tableName: String,
    kinesis: KinesisAsyncClient,
    dynamoDb: DynamoDbAsyncClient,
    cloudWatch: CloudWatchAsyncClient,
    metricsFactory: Option[MetricsFactory]
  ): Observable[KinesisClientRecord] =
    apply(
      buildKinesisScheduler(
        streamName = streamName,
        applicationName = applicationName,
        workerIdentifier = workerIdentifier,
        tableName = tableName,
        kinesis = kinesis,
        dynamoDb = dynamoDb,
        cloudWatch = cloudWatch,
        metricsFactory
      )
    )

  def apply(f: ShardRecordProcessorFactory => KScheduler): Observable[KinesisClientRecord] =
    new KinesisConsumerAutoCheckpoint(f)

  private val terminateGracePeriod = 10.seconds

  def manualCheckpoint(
    streamName: String,
    applicationName: String,
    workerIdentifier: String,
    tableName: String
  ): Observable[CheckpointableRecord] =
    Observable.fromResource(makeAwsClients).flatMap {
      case (kinesis, dynamoDb, cloudWatch) =>
        manualCheckpoint(
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

  def manualCheckpoint(
    streamName: String,
    applicationName: String,
    workerIdentifier: String,
    tableName: String,
    kinesis: KinesisAsyncClient,
    dynamoDb: DynamoDbAsyncClient,
    cloudWatch: CloudWatchAsyncClient,
    metricsFactory: Option[MetricsFactory]
  ): Observable[CheckpointableRecord] =
    manualCheckpoint(
      buildKinesisScheduler(
        streamName = streamName,
        applicationName = applicationName,
        workerIdentifier = workerIdentifier,
        tableName = tableName,
        kinesis = kinesis,
        dynamoDb = dynamoDb,
        cloudWatch = cloudWatch,
        metricsFactory
      )
    )

  def manualCheckpoint(f: ShardRecordProcessorFactory => KScheduler): Observable[CheckpointableRecord] =
    new KinesisConsumerManualCheckpoint(f, terminateGracePeriod)

  private def buildKinesisScheduler(
    streamName: String,
    applicationName: String,
    workerIdentifier: String,
    tableName: String,
    kinesis: KinesisAsyncClient,
    dynamoDb: DynamoDbAsyncClient,
    cloudWatch: CloudWatchAsyncClient,
    metricsFactory: Option[MetricsFactory]
  )(factory: ShardRecordProcessorFactory): KScheduler = {
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

  private def makeAwsClients =
    (
      Resource.fromAutoCloseable(IO(KinesisAsyncClient.create())),
      Resource.fromAutoCloseable(IO(DynamoDbAsyncClient.create())),
      Resource.fromAutoCloseable(IO(CloudWatchAsyncClient.create()))
    ).tupled

  def checkpoint(
    obs: Observable[CheckpointableRecord],
    timespan: FiniteDuration,
    maxCount: Int
  ): Observable[Unit] =
    obs
      .groupBy(_.shardId)
      .mapEval { group =>
        group
          .bufferTimedAndCounted(timespan, maxCount)
          .map(_.maxBy(_.extendedSequenceNumber))
          .consumeWith(Consumer.foreachEval { cr =>
            if (cr.canCheckpoint) {
              cr.checkpoint[IO]()
            } else {
              IO.unit
            }
          })
      }
}
