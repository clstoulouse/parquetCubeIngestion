package fr.cls.bigdata.georef.model

import DataStorage._

sealed trait DataType {
  def store(values: Seq[Any]): DataStorage
  val sizeInBytes: Option[Int]
}

object DataType {
  case object Boolean extends DataType {
    def store(values: Seq[Any]): BooleanStorage = BooleanStorage(values.map(_.asInstanceOf[Boolean]).toArray[Boolean])
    val sizeInBytes: Option[Int] = Some(1)
  }
  case object String extends DataType {
    def store(values: Seq[Any]): StringStorage = StringStorage(values.map(_.asInstanceOf[String]).toArray[String])
    val sizeInBytes: Option[Int] = None
  }

  sealed trait NumericType extends DataType {
    def of(value: Any): Any
    def toDouble(value: Any): Double = numeric(value).toDouble(value)
  }

  case object Byte extends NumericType {
    override def of(value: Any): Byte = numeric(value).toInt(value).toByte
    def store(values: Seq[Any]): ByteStorage = ByteStorage(values.asInstanceOf[Seq[Byte]].toArray[Byte])
    val sizeInBytes: Option[Int] = Some(1)
  }

  case object Short extends NumericType {
    override def of(value: Any): Short = numeric(value).toInt(value).toShort
    def store(values: Seq[Any]): ShortStorage = ShortStorage(values.asInstanceOf[Seq[Short]].toArray[Short])
    val sizeInBytes: Option[Int] = Some(2)
  }

  case object Int extends NumericType {
    override def of(value: Any): Int = numeric(value).toInt(value)
    def store(values: Seq[Any]): IntStorage = IntStorage(values.asInstanceOf[Seq[Int]].toArray[Int])
    val sizeInBytes: Option[Int] = Some(4)
  }

  case object Long extends NumericType {
    override def of(value: Any): Long = numeric(value).toLong(value)
    def store(values: Seq[Any]): LongStorage = LongStorage(values.asInstanceOf[Seq[Long]].toArray[Long])
    val sizeInBytes: Option[Int] = Some(8)
  }

  case object Float extends NumericType {
    override def of(value: Any): Float = numeric(value).toFloat(value)
    def store(values: Seq[Any]): FloatStorage = FloatStorage(values.asInstanceOf[Seq[Float]].toArray[Float])
    val sizeInBytes: Option[Int] = Some(4)
  }

  case object Double extends NumericType {
    override def of(value: Any): Double = numeric(value).toDouble(value)
    def store(values: Seq[Any]): DoubleStorage = DoubleStorage(values.asInstanceOf[Seq[Double]].toArray[Double])
    val sizeInBytes: Option[Int] = Some(8)
  }

  private def numeric(value: Any): Numeric[Any] = {
    value match {
      case _: Byte => implicitly[Numeric[Byte]].asInstanceOf[Numeric[Any]]
      case _: Short => implicitly[Numeric[Short]].asInstanceOf[Numeric[Any]]
      case _: Int => implicitly[Numeric[Int]].asInstanceOf[Numeric[Any]]
      case _: Long => implicitly[Numeric[Long]].asInstanceOf[Numeric[Any]]
      case _: Float => implicitly[Numeric[Float]].asInstanceOf[Numeric[Any]]
      case _: Double => implicitly[Numeric[Double]].asInstanceOf[Numeric[Any]]
    }
  }
}
