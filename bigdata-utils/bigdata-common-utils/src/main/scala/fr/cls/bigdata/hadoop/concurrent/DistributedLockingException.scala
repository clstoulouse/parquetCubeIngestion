package fr.cls.bigdata.hadoop.concurrent

/**
  * Raised when unable to acquire/release a lock when manipulating a distributed file.
  *
  * @param msg Error message.
  * @param t   Root cause.
  */
class DistributedLockingException(msg: String, t: Throwable = null) extends Exception(msg, t)
