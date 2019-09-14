package com.gubbns.monix.kinesis

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model._
import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.utils.builder.{CopyableBuilder, ToCopyableBuilder}
import cats.effect.IO
import com.disneystreaming.cxeng.utils.IOFromCompletableFuture

final class KinesisOps private (val kinesis: KinesisAsyncClient) extends AnyVal {

  def createStream(reqB: CreateStreamRequest.Builder => CreateStreamRequest.Builder): IO[CreateStreamResponse] =
    kinesisReq(kinesis.createStream(_: CreateStreamRequest), reqB(CreateStreamRequest.builder()))

  def describeStream(reqB: DescribeStreamRequest.Builder => DescribeStreamRequest.Builder): IO[DescribeStreamResponse] =
    kinesisReq(kinesis.describeStream(_: DescribeStreamRequest), reqB(DescribeStreamRequest.builder()))

  def putRecord(reqB: PutRecordRequest.Builder => PutRecordRequest.Builder): IO[PutRecordResponse] =
    kinesisReq(kinesis.putRecord(_: PutRecordRequest), reqB(PutRecordRequest.builder()))

  private def kinesisReq[Req <: ToCopyableBuilder[Builder, Req], Builder <: CopyableBuilder[Builder, Req], Resp](
    reqF: Req => CompletableFuture[Resp],
    req: Builder
  ): IO[Resp] = IOFromCompletableFuture(reqF(req.build()))
}

object KinesisOps {
  def apply(kinesis: KinesisAsyncClient): KinesisOps = new KinesisOps(kinesis)
}
