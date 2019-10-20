package com.gubbns.monix.kinesis.aws

import cats.implicits._
import cats.effect.{IO, Resource}
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import java.net.URI

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.utils.AttributeMap
import software.amazon.awssdk.http.{Protocol, SdkHttpConfigurationOption}
import software.amazon.awssdk.http.async.SdkAsyncHttpClient

object AwsClients {

  private val awsHost: IO[String] = IO(sys.env.isDefinedAt("CI")).map {
    case true => "aws"
    case false => "localhost"
  }

  def makeKinesisLocal: Resource[IO, KinesisOps] =
    for {
      httpClient <- makeLocalHttpClient
      host <- Resource.liftF(awsHost)
      client <- Resource.fromAutoCloseable(
        IO(
          localAwsClient(
            KinesisAsyncClient
              .builder()
              .httpClient(httpClient),
            show"https://$host:4568"
          ).build()
        )
      )
    } yield KinesisOps(client)

  def makeDynamoDbLocal: Resource[IO, DynamoDbOps] =
    for {
      httpClient <- makeLocalHttpClient
      host <- Resource.liftF(awsHost)
      client <- Resource.fromAutoCloseable(
        IO(
          localAwsClient(
            DynamoDbAsyncClient
              .builder()
              .httpClient(httpClient),
            show"https://$host:4569"
          ).build()
        )
      )
    } yield DynamoDbOps(client)

  def makeCloudWatchLocal: Resource[IO, CloudWatchAsyncClient] =
    for {
      httpClient <- makeLocalHttpClient
      host <- Resource.liftF(awsHost)
      client <- Resource.fromAutoCloseable(
        IO(
          localAwsClient(
            CloudWatchAsyncClient
              .builder()
              .httpClient(httpClient),
            show"https://$host:4582"
          ).build()
        )
      )
    } yield client

  private def makeLocalHttpClient: Resource[IO, SdkAsyncHttpClient] =
    Resource.fromAutoCloseable(
      IO(
        NettyNioAsyncHttpClient
          .builder()
          .protocol(Protocol.HTTP1_1)
          .buildWithDefaults(
            AttributeMap
              .builder()
              .put(
                SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES,
                java.lang.Boolean.TRUE
              )
              .build()
          )
      )
    )

  private def localAwsClient[Builder <: AwsClientBuilder[Builder, _]](builder: Builder, endpoint: String): Builder = {
    val creds = AwsBasicCredentials.create("dummy", "dummy")
    builder
      .endpointOverride(URI.create(endpoint))
      .region(Region.US_EAST_1)
      .credentialsProvider(StaticCredentialsProvider.create(creds))
  }
}
