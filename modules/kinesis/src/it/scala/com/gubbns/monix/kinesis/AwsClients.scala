package com.gubbns.monix.kinesis

import java.net.URI

import cats.effect._
import cats.implicits._
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.http.{Protocol, SdkHttpConfigurationOption}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.utils.AttributeMap

object AwsClients {

  def makeKinesisLocal: Resource[IO, KinesisOps] =
    for {
      httpClient <- makeLocalHttpClient
      client <- Resource.fromAutoCloseable(
        IO(
          localAwsClient(
            KinesisAsyncClient
              .builder()
              .httpClient(httpClient),
            show"https://localhost:4568"
          ).build()
        )
      )
    } yield KinesisOps(client)

  def makeDynamoDbLocal: Resource[IO, DynamoDbAsyncClient] =
    for {
      httpClient <- makeLocalHttpClient
      client <- Resource.fromAutoCloseable(
        IO(
          localAwsClient(
            DynamoDbAsyncClient
              .builder()
              .httpClient(httpClient),
            show"https://localhost:4569"
          ).build()
        )
      )
    } yield client

  def makeCloudWatchLocal: Resource[IO, CloudWatchAsyncClient] =
    for {
      httpClient <- makeLocalHttpClient
      client <- Resource.fromAutoCloseable(
        IO(
          localAwsClient(
            CloudWatchAsyncClient
              .builder()
              .httpClient(httpClient),
            show"https://localhost:4582"
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
