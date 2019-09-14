package com.gubbns.monix.kinesis

import java.nio.charset.StandardCharsets
import java.util.UUID

import utest.{Tests, test}
import cats.implicits._
import cats.effect._
import cats.effect.utest._
import monix.execution.Scheduler.Implicits.global
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model._

import scala.concurrent.duration._

object KinesisConsumerTests extends IOTestSuite {
  override def timeout: FiniteDuration = 30.seconds

  private val makeAwsClients = (
    AwsClients.makeKinesisLocal,
    AwsClients.makeDynamoDbLocal,
    AwsClients.makeCloudWatchLocal
  ).tupled

  override val tests = Tests {
    test("consume a record from kinesis") - {
      val streamName = show"test-stream-${UUID.randomUUID()}"
      makeAwsClients.use { case (kinesis, dynamoDb, cloudWatch) =>
        for {
          _ <- kinesis.createStream(_.streamName(streamName).shardCount(1))
          _ <- kinesis.describeStream(_.streamName(streamName))
            .iterateUntil(_.streamDescription().streamStatus() == StreamStatus.ACTIVE)
          _ <- kinesis.putRecord(_
            .streamName(streamName)
            .partitionKey("foo")
            .data(SdkBytes.fromUtf8String("Hello")))
          records <- KinesisConsumer
            .create(
              streamName,
              "foo",
              UUID.randomUUID().show,
              "table-name",
              kinesis.kinesis,
              dynamoDb,
              cloudWatch,
              None,
              5.seconds
            )
            .take(1)
            .toListL
            .to[IO]
        } yield {
          val messages = records.map(cr => StandardCharsets.UTF_8.decode(cr.record.data()).toString)
          assert(messages == List("Hello"))
        }
      }
    }
  }
}
