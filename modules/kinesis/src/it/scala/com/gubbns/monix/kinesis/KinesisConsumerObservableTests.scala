package com.gubbns.monix.kinesis

import java.nio.charset.StandardCharsets

import utest.{test, Tests}
import cats.implicits._
import cats.effect._
import cats.effect.utest._
import com.gubbns.monix.kinesis.aws.{AwsClients, DynamoDbOps, KinesisOps}
import monix.execution.Scheduler.Implicits.global
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.kinesis.{KinesisAsyncClient, model => kmodel}
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, model => dynomodel}
import software.amazon.kinesis.retrieval.KinesisClientRecord

import scala.concurrent.duration._

object KinesisConsumerObservableTests extends IOTestSuite {
  override def timeout: FiniteDuration = 40.seconds

  private val makeAwsClients = (
    AwsClients.makeKinesisLocal,
    AwsClients.makeDynamoDbLocal,
    AwsClients.makeCloudWatchLocal
  ).tupled

  private val tableName = "test-checkpoint-table"
  private val streamName = "test-stream"
  private val applicationName = "test-application"

  override val tests = Tests {
    test("consume a record, checkpoint, then consume the next record") - {
      makeAwsClients.use {
        case (kinesis, dynamoDb, cloudWatch) =>
          val consumer = kinesisObs(kinesis.client, dynamoDb.client, cloudWatch)
          for {
            _ <- cleanAwsResources(kinesis, dynamoDb)
            _ <- kinesis.createStream(_.streamName(streamName).shardCount(1)) *> kinesis
              .describeStream(_.streamName(streamName))
              .iterateUntil(_.streamDescription().streamStatus() == kmodel.StreamStatus.ACTIVE)
            _ <- kinesis.putRecord(
              _.streamName(streamName)
                .partitionKey("pk-1")
                .data(SdkBytes.fromUtf8String("record1"))
            )
            records1 <- consumer.take(1).toListL.to[IO]
            _ <- kinesis.putRecord(
              _.streamName(streamName)
                .partitionKey("pk-2")
                .data(SdkBytes.fromUtf8String("record2"))
            )
            records2 <- consumer.take(1).toListL.to[IO]
          } yield {
            val messages = records1.map(decodeStringRecord)
            assert(messages === List("pk-1" -> "record1"))
            val messages2 = records2.map(decodeStringRecord)
            assert(messages2 === List("pk-2" -> "record2"))
          }
      }
    }
  }

  private def kinesisObs(
    kinesis: KinesisAsyncClient,
    dynamoDb: DynamoDbAsyncClient,
    cloudWatch: CloudWatchAsyncClient
  ) =
    KinesisConsumerObservable(
      streamName,
      applicationName,
      applicationName,
      tableName,
      kinesis,
      dynamoDb,
      cloudWatch,
      None
    )

  private def cleanAwsResources(kinesis: KinesisOps, dynamoDb: DynamoDbOps): IO[Unit] = {
    val deleteTable =
      dynamoDb.deleteTable(_.tableName(tableName)).void.recover { case _: dynomodel.ResourceNotFoundException => () }
    val deleteStreamAndWait = kinesis
      .deleteStream(_.streamName(streamName))
      .void
      .recover { case _: kmodel.ResourceNotFoundException => () } *>
      kinesis
        .describeStream(_.streamName(streamName))
        .attempt
        .iterateUntil {
          case Left(_: kmodel.ResourceNotFoundException) => true
          case _ => false
        }
        .void
    (deleteTable, deleteStreamAndWait).parTupled.void
  }

  private def decodeStringRecord(record: KinesisClientRecord) =
    record.partitionKey() -> StandardCharsets.UTF_8.decode(record.data()).toString

}
