package com.gubbns.monix.kinesis

import cats.effect._
import java.util.concurrent.{CompletableFuture, CompletionException}

@SuppressWarnings(Array("DisableSyntax.null"))
object IOFromCompletableFuture {

  def apply[A](cf: => CompletableFuture[A])(implicit cs: ContextShift[IO]): IO[A] = apply(IO(cf))

  def apply[A](iof: IO[CompletableFuture[A]])(implicit cs: ContextShift[IO]): IO[A] =
    iof.flatMap(
      f =>
        IO.cancelable[A] { cb =>
          f.handle[Unit] {
            case (null, err: CompletionException) if err.getCause != null => cb(Left(err.getCause))
            case (null, err) => cb(Left(err))
            case (a, _) => cb(Right(a))
          }
          IO { val _ = f.cancel(true) }
        }
    )
    // Make sure to shift back to the caller's context
    // not shifting is a concurrency bug, https://github.com/typelevel/cats-effect/pull/546
    .guarantee(cs.shift)
}
