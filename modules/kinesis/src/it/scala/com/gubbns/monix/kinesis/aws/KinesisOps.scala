package com.gubbns.monix.kinesis.aws

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model._
import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.utils.builder.{CopyableBuilder, ToCopyableBuilder}
import cats.effect.IO
import com.disneystreaming.cxeng.utils.IOFromCompletableFuture

final class KinesisOps private (val client: KinesisAsyncClient) extends AnyVal {

  def createStream(reqB: CreateStreamRequest.Builder => CreateStreamRequest.Builder): IO[CreateStreamResponse] =
    runReq(client.createStream(_: CreateStreamRequest), reqB(CreateStreamRequest.builder()))

  def deleteStream(reqB: DeleteStreamRequest.Builder => DeleteStreamRequest.Builder): IO[DeleteStreamResponse] =
    runReq(client.deleteStream(_: DeleteStreamRequest), reqB(DeleteStreamRequest.builder()))

  def describeStream(reqB: DescribeStreamRequest.Builder => DescribeStreamRequest.Builder): IO[DescribeStreamResponse] =
    runReq(client.describeStream(_: DescribeStreamRequest), reqB(DescribeStreamRequest.builder()))

  def putRecord(reqB: PutRecordRequest.Builder => PutRecordRequest.Builder): IO[PutRecordResponse] =
    runReq(client.putRecord(_: PutRecordRequest), reqB(PutRecordRequest.builder()))

  private def runReq[Req <: ToCopyableBuilder[Builder, Req], Builder <: CopyableBuilder[Builder, Req], Resp](
    reqF: Req => CompletableFuture[Resp],
    req: Builder
  ): IO[Resp] = IOFromCompletableFuture(reqF(req.build()))
}

object KinesisOps {
  def apply(client: KinesisAsyncClient): KinesisOps = new KinesisOps(client)
}
