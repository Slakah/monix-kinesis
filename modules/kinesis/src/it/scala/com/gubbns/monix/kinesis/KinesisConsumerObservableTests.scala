package com.gubbns.monix.kinesis

import java.nio.charset.StandardCharsets
import java.util.UUID

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
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry
import software.amazon.kinesis.retrieval.KinesisClientRecord

import scala.concurrent.duration._

object KinesisConsumerObservableTests extends IOTestSuite {
  override def timeout: FiniteDuration = 60.seconds

  private val applicationName = "test-application"

  override val tests = Tests {

    test("consume multiple records") - {
      val pk2body = (1 to 10).map(i => show"pk-$i" -> show"record$i").toList
      makeAwsClientAndResources.use { case (kinesis, dynamoDb, cloudWatch, awsResources) =>
        val consumer = kinesisObs(kinesis.client, dynamoDb.client, cloudWatch, awsResources)
        for {
          // put the test records into the kinesis stream
          _ <- kinesis.putRecords(_
            .streamName(awsResources.streamName)
            .records(pk2body.map { case (pk, body) => PutRecordsRequestEntry.builder()
              .partitionKey(pk).data(SdkBytes.fromUtf8String(body))
              .build()}: _*)
          )
          // consume the records
          records <- consumer.take(pk2body.length.toLong).toListL.to[IO]
        } yield {
          val messages = records.map(decodeStringRecord)
          assert(messages === pk2body)
        }
      }
    }
    test("consume a record, checkpoint, then consume the next record") - {
      makeAwsClientAndResources.use { case (kinesis, dynamoDb, cloudWatch, awsResources) =>
        val consumer = kinesisObs(kinesis.client, dynamoDb.client, cloudWatch, awsResources)
        for {
          // put and then consume a record in the stream
          _ <- kinesis.putRecord(
            _.streamName(awsResources.streamName)
              .partitionKey("pk-1")
              .data(SdkBytes.fromUtf8String("record1"))
          )
          records1 <- consumer.take(1).toListL.to[IO]
          // check checkpointing works, by checking that only the second record is read
          _ <- kinesis.putRecord(
            _.streamName(awsResources.streamName)
              .partitionKey("pk-2")
              .data(SdkBytes.fromUtf8String("record2"))
          )
          records2 <- consumer.take(1).toListL.to[IO]
        } yield {
          val messages1 = records1.map(decodeStringRecord)
          assert(messages1 === List("pk-1" -> "record1"))
          val messages2 = records2.map(decodeStringRecord)
          assert(messages2 === List("pk-2" -> "record2"))
        }
      }
    }
  }

  private def makeAwsClientAndResources = for {
    (kinesis, dynamoDb, cloudWatch) <- makeAwsClients
    awsResources <- makeAwsResources(kinesis, dynamoDb)
  } yield (kinesis, dynamoDb, cloudWatch, awsResources)

  private val makeAwsClients = (
    AwsClients.makeKinesisLocal,
    AwsClients.makeDynamoDbLocal,
    AwsClients.makeCloudWatchLocal
    ).tupled

  private def makeAwsResources(kinesis: KinesisOps, dynamoDb: DynamoDbOps): Resource[IO, AwsResources] = {
    val acquire = for {
      testId <- IO(UUID.randomUUID().show)
      awsResources = AwsResources(show"test-stream-$testId", show"test-table-$testId")
      _ <- kinesis.createStream(_.streamName(awsResources.streamName).shardCount(1)) *> kinesis
        .describeStream(_.streamName(awsResources.streamName))
        .iterateUntil(_.streamDescription().streamStatus() == kmodel.StreamStatus.ACTIVE)
        .void
    } yield awsResources
    Resource.make(acquire)(cleanAwsResources(kinesis, dynamoDb, _))
  }

  private def cleanAwsResources(kinesis: KinesisOps, dynamoDb: DynamoDbOps, awsResources: AwsResources): IO[Unit] = {
    val deleteTable =
      dynamoDb.deleteTable(_.tableName(awsResources.tableName)).void.recover { case _: dynomodel.ResourceNotFoundException => () }
    val deleteStreamAndWait = kinesis
      .deleteStream(_.streamName(awsResources.streamName))
      .void
      .recover { case _: kmodel.ResourceNotFoundException => () } *>
      kinesis
        .describeStream(_.streamName(awsResources.streamName))
        .attempt
        .iterateUntil {
          case Left(_: kmodel.ResourceNotFoundException) => true
          case _ => false
        }
        .void
    (deleteTable, deleteStreamAndWait).parTupled.void
  }

  private def kinesisObs(
    kinesis: KinesisAsyncClient,
    dynamoDb: DynamoDbAsyncClient,
    cloudWatch: CloudWatchAsyncClient,
    awsResources: AwsResources
  ) =
    KinesisConsumerObservable(
      awsResources.streamName,
      applicationName,
      applicationName,
      awsResources.tableName,
      kinesis,
      dynamoDb,
      cloudWatch,
      None
    )

  private def decodeStringRecord(record: KinesisClientRecord) =
    record.partitionKey() -> StandardCharsets.UTF_8.decode(record.data()).toString

}

private final case class AwsResources(
  streamName: String,
  tableName: String
)

