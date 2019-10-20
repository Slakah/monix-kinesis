package com.gubbns.monix.kinesis.aws

import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.utils.builder.{CopyableBuilder, ToCopyableBuilder}
import cats.effect.IO
import com.gubbns.monix.kinesis.IOFromCompletableFuture
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

final class DynamoDbOps private (val client: DynamoDbAsyncClient) extends AnyVal {

  def deleteTable(reqB: DeleteTableRequest.Builder => DeleteTableRequest.Builder): IO[DeleteTableResponse] =
    runReq(client.deleteTable(_: DeleteTableRequest), reqB(DeleteTableRequest.builder()))

  private def runReq[Req <: ToCopyableBuilder[Builder, Req], Builder <: CopyableBuilder[Builder, Req], Resp](
    reqF: Req => CompletableFuture[Resp],
    req: Builder
  ): IO[Resp] = IOFromCompletableFuture(reqF(req.build()))
}

object DynamoDbOps {
  def apply(client: DynamoDbAsyncClient): DynamoDbOps = new DynamoDbOps(client)
}
