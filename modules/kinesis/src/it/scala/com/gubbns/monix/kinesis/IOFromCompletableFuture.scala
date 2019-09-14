package com.disneystreaming.cxeng.utils

import cats.effect.IO
import java.util.concurrent.{CompletableFuture, CompletionException}

@SuppressWarnings(Array("DisableSyntax.null"))
object IOFromCompletableFuture {

  def apply[A](cf: => CompletableFuture[A]): IO[A] = apply(IO(cf))

  def apply[A](iof: IO[CompletableFuture[A]]): IO[A] =
    iof.flatMap(
      f =>
        IO.cancelable { cb =>
          f.handle[Unit] {
            case (null, err: CompletionException) if err.getCause != null => cb(Left(err.getCause))
            case (null, err) => cb(Left(err))
            case (a, _) => cb(Right(a))
          }
          IO { val _ = f.cancel(true) }
        }
    )

}
