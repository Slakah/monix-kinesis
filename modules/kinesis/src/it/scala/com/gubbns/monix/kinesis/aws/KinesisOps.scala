package com.gubbns.monix.kinesis.aws

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model._
import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.utils.builder.{CopyableBuilder, ToCopyableBuilder}
import cats.effect._
import com.gubbns.monix.kinesis.IOFromCompletableFuture

final class KinesisOps private (val client: KinesisAsyncClient) extends AnyVal {

  def createStream(reqB: CreateStreamRequest.Builder => CreateStreamRequest.Builder)(implicit cs: ContextShift[IO]): IO[CreateStreamResponse] =
    runReq(client.createStream(_: CreateStreamRequest), reqB(CreateStreamRequest.builder()))

  def deleteStream(reqB: DeleteStreamRequest.Builder => DeleteStreamRequest.Builder)(implicit cs: ContextShift[IO]): IO[DeleteStreamResponse] =
    runReq(client.deleteStream(_: DeleteStreamRequest), reqB(DeleteStreamRequest.builder()))

  def describeStream(reqB: DescribeStreamRequest.Builder => DescribeStreamRequest.Builder)(implicit cs: ContextShift[IO]): IO[DescribeStreamResponse] =
    runReq(client.describeStream(_: DescribeStreamRequest), reqB(DescribeStreamRequest.builder()))

  def putRecord(reqB: PutRecordRequest.Builder => PutRecordRequest.Builder)(implicit cs: ContextShift[IO]): IO[PutRecordResponse] =
    runReq(client.putRecord(_: PutRecordRequest), reqB(PutRecordRequest.builder()))

  def putRecords(reqB: PutRecordsRequest.Builder => PutRecordsRequest.Builder)(implicit cs: ContextShift[IO]): IO[PutRecordsResponse] =
    runReq(client.putRecords(_: PutRecordsRequest), reqB(PutRecordsRequest.builder()))

  private def runReq[Req <: ToCopyableBuilder[Builder, Req], Builder <: CopyableBuilder[Builder, Req], Resp](
    reqF: Req => CompletableFuture[Resp],
    req: Builder
  )(implicit cs: ContextShift[IO]): IO[Resp] = IOFromCompletableFuture(reqF(req.build()))
}

object KinesisOps {
  def apply(client: KinesisAsyncClient): KinesisOps = new KinesisOps(client)
}
