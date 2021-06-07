package fr.cls.bigdata.resource

import scala.collection.generic.CanBuildFrom

/**
  * Resource[A] is a wrapper around a value of type A that depends on a system resource (file, network etc...)
  * that must be closed. A Resource[A] implements the [[java.lang.AutoCloseable]] interface.
  *
  * In java you could write something like
  *
  * {{{
  * try (Resource[A] resource = openResource()) {
  *   // do something with resource.get
  * }
  * }}}
  *
  * In scala, the Resource[A] trait is extended with the implicit [[Resource.FuncResource]] class from its companion object.
  * This extension class provides support for monadic style (for-comprehension)
  *
  * {{{
  * for {
  *   a: A <- openResourceA()
  *   b: B <- openResourceB(resourceA.get)
  * } {
  *   // do something with a and b
  * }
  * }}}
  *
  * At the end of the block both resourceA and resourceB are safely closed
  *
  * The safeGet method can be used when the resource is no longer needed
  * that is, the value A cannot access the resource anymore
  */
trait Resource[+A] extends AutoCloseable {
  @throws[Exception]
  def close(): Unit

  def get: A
}

object Resource {
  def apply[A <: AutoCloseable](value: A): Resource[A] = new Resource[A] {
    def close(): Unit = value.close()
    def get: A = value
  }

  def free[A](value: A): Resource[A] = new Resource[A] {
    def close(): Unit = ()
    def get: A = value
  }


  /**
    * Converts a `Seq[Resource[X]]` to `Resource[Seq[X]]`
    */
  def sequence[A, M[X] <: Traversable[X]](in: M[Resource[A]])(implicit cbf: CanBuildFrom[M[Resource[A]], A, M[A]]): Resource[M[A]] = new Resource[M[A]] {
    override def get: M[A] = {
      val builder = cbf(in)

      in.foreach { x =>
        builder += x.get
      }

      builder.result()
    }

    override def close(): Unit = in.foreach(_.close())
  }

  implicit class FuncResource[A](resource: Resource[A]) {
    def map[B](f: A => B): Resource[B] = new Resource[B] {
      private val value = f(resource.get)
      def close(): Unit = resource.close()
      def get: B = value
    }

    def flatMap[B](f: A => Resource[B]): Resource[B] = new Resource[B] {
      private val otherResource = f(resource.get)
      def close(): Unit = {
        try {
          otherResource.close()
        } finally {
          resource.close()
        }
      }

      def get: B = otherResource.get
    }

    def foreach(f: A => Unit): Unit = {
      try {
        f(resource.get)
      } finally {
        resource.close()
      }
    }

    def safeGet: A = {
      try {
        resource.get
      } finally {
        resource.close()
      }
    }

    def acquire[B](f: A => B): B = map(f).safeGet
  }
}
