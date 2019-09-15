package com.gubbns.monix.kinesis

import cats.Order
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

package object instances {
  implicit val extendedSequenceNumberOrder: Order[ExtendedSequenceNumber] = Order.fromOrdering
}
