package fr.cls.bigdata.georef.model

import fr.cls.bigdata.georef.model.DataType.NumericType

sealed trait DataStorage extends IndexedSeq[Any] with Serializable {
  def dataType: DataType
  def storage: Storage
}

object DataStorage {
  def apply(dataType: DataType, storage: Storage): DataStorage = {
    dataType match {
      case DataType.Byte => ByteStorage(storage.asInstanceOf[Array[Byte]])
      case DataType.Short => ShortStorage(storage.asInstanceOf[Array[Short]])
      case DataType.Int => IntStorage(storage.asInstanceOf[Array[Int]])
      case DataType.Long => LongStorage(storage.asInstanceOf[Array[Long]])
      case DataType.Float => FloatStorage(storage.asInstanceOf[Array[Float]])
      case DataType.Double => DoubleStorage(storage.asInstanceOf[Array[Double]])
      case DataType.Boolean => BooleanStorage(storage.asInstanceOf[Array[Boolean]])
      case DataType.String => StringStorage(storage.asInstanceOf[Array[String]])
    }
  }

  case class BooleanStorage(storage: Array[Boolean]) extends DataStorage {
    override def dataType: DataType = DataType.Boolean
    override def length: Int = storage.length
    override def iterator: Iterator[Boolean] = storage.toIterator
    override def apply(idx: Int): Boolean = storage(idx)
  }

  case class StringStorage(storage: Array[String]) extends DataStorage {
    override def dataType: DataType = DataType.String
    override def length: Int = storage.length
    override def iterator: Iterator[String] = storage.toIterator
    override def apply(idx: Int): String = storage(idx)
  }

  sealed trait NumericStorage extends DataStorage {
    override def dataType: NumericType
  }

  case class ByteStorage(storage: Array[Byte]) extends NumericStorage {
    override def dataType: NumericType = DataType.Byte
    override def length: Int = storage.length
    override def iterator: Iterator[Byte] = storage.toIterator
    override def apply(idx: Int): Byte = storage(idx)
  }

  case class ShortStorage(storage: Array[Short]) extends NumericStorage {
    override def dataType: NumericType = DataType.Short
    override def length: Int = storage.length
    override def iterator: Iterator[Short] = storage.toIterator
    override def apply(idx: Int): Short = storage(idx)
  }

  case class IntStorage(storage: Array[Int]) extends NumericStorage {
    override def dataType: NumericType = DataType.Int
    override def length: Int = storage.length
    override def iterator: Iterator[Int] = storage.toIterator
    override def apply(idx: Int): Int = storage(idx)
  }

  case class LongStorage(storage: Array[Long]) extends NumericStorage {
    override def dataType: NumericType = DataType.Long
    override def length: Int = storage.length
    override def iterator: Iterator[Long] = storage.toIterator
    override def apply(idx: Int): Long = storage(idx)
  }

  case class FloatStorage(storage: Array[Float]) extends NumericStorage {
    override def dataType: NumericType = DataType.Float
    override def length: Int = storage.length
    override def iterator: Iterator[Float] = storage.toIterator
    override def apply(idx: Int): Float = storage(idx)
  }

  case class DoubleStorage(storage: Array[Double]) extends NumericStorage {
    override def dataType: NumericType = DataType.Double
    override def length: Int = storage.length
    override def iterator: Iterator[Double] = storage.toIterator
    override def apply(idx: Int): Double = storage(idx)
  }

}