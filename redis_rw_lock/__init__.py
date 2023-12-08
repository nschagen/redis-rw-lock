import logging
from contextlib import contextmanager
import redis_lock


LOCK_EXPIRE_TIME = 60


class AcquisitionTimeout(Exception):
    """
    Raised when it took too long to acquire the lock
    """
    pass


class AcquisitionTimeoutLock(redis_lock.Lock):
    """
    Specialized lock that always uses a predefined acquisition time-out when
    acquire() is called. This simplifies the rest of the code as it ensures
    that this timeout is always applied. By raising an exception when the
    timeout occurs, we ensure that all locks previously acquired in the
    implementation are properly released.
    """

    def __init__(
        self,
        redis_client,
        name,
        expire=None,
        id=None,
        auto_renewal=False,
        strict=True,
        signal_expire=1000,
        acquire_timeout=None,
    ):
        super().__init__(
            redis_client,
            name,
            expire=expire,
            auto_renewal=auto_renewal,
            strict=strict,
            signal_expire=signal_expire,
        )
        self.__acquire_timeout = acquire_timeout

    def acquire(self):
        if not super().acquire(timeout=self.__acquire_timeout):
            raise AcquisitionTimeout()


@contextmanager
def acquire_lock(lock):
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


class RWLock:
    """
    Read-write lock with writers priority.

    Allows either a single writer or multiple readers to acquire the lock simultaneously.
    """

    READ = "R"
    WRITE = "W"

    def __init__(
        self,
        redis_conn,
        name,
        mode,
        expire=LOCK_EXPIRE_TIME,
        auto_renew=False,
        acquire_timeout=None,
    ):
        assert mode in (self.READ, self.WRITE), "Invalid mode."
        assert name, "Invalid name. Should not be empty"

        self.__mode = mode

        # Counts simultaneous readers that have the lock.
        self.__read_switch = _LightSwitch(
            redis_conn, f"read_switch:{name}", expire=expire, auto_renew=auto_renew
        )

        # Counts the number of writers waiting to get the lock
        self.__write_switch = _LightSwitch(
            redis_conn, f"write_switch:{name}", expire=expire, auto_renew=auto_renew
        )

        # Blocks readers from getting the lock. Locked when write lock is
        # acquired.
        self.__no_readers = AcquisitionTimeoutLock(
            redis_conn,
            f"lock:no_readers:{name}",
            expire=expire,
            auto_renewal=auto_renew,
            acquire_timeout=acquire_timeout,
        )

        # Blocks writers from getting the lock. Locked when read lock is
        # acquired.
        self.__no_writers = AcquisitionTimeoutLock(
            redis_conn,
            f"lock:no_writers:{name}",
            expire=expire,
            auto_renewal=auto_renew,
            acquire_timeout=acquire_timeout,
        )

        # Serializes concurrent attempts of readers to get the read lock. Read
        # lock acquisition takes multiple steps and only one reader is allowed
        # to execute these at a time.
        self.__readers_queue = AcquisitionTimeoutLock(
            redis_conn,
            f"lock:readers_queue:{name}",
            expire=expire,
            auto_renewal=auto_renew,
            acquire_timeout=acquire_timeout,
        )

    def __reader_acquire(self):
        with acquire_lock(self.__readers_queue):
            with acquire_lock(self.__no_readers):
                self.__read_switch.acquire(self.__no_writers)

    def __reader_release(self):
        self.__read_switch.release(self.__no_writers)

    def __writer_acquire(self):
        self.__write_switch.acquire(self.__no_readers)
        try:
            self.__no_writers.acquire()
        except Exception as ex:
            self.__write_switch.release(self.__no_readers)
            raise ex

    def __writer_release(self):
        self.__no_writers.release()
        self.__write_switch.release(self.__no_readers)

    def acquire(self):
        if self.__mode == self.READ:
            return self.__reader_acquire()
        return self.__writer_acquire()

    def release(self):
        if self.__mode == self.READ:
            return self.__reader_release()
        return self.__writer_release()

    def reset(self):
        self.__read_switch.reset()
        self.__write_switch.reset()
        self.__no_readers.reset()
        self.__no_readers.reset()
        self.__readers_queue.reset()


class _LightSwitch:
    """
    An auxiliary "light switch"-like object.

    The first process that calls `acquire()` turns on the "switch", the last
    one turns it off when calling 'release()'.
    """

    def __init__(self, redis_conn, name, expire=None, auto_renew=False, acquire_timeout=None):
        self.__counter_name = "lock:switch:counter:{}".format(name)
        self.__name = name
        self.__expire = expire
        self.__acquire_timeout = acquire_timeout
        self.__redis_conn = redis_conn
        self.__redis_conn.set(self.__counter_name, 0, nx=True, ex=self.__expire)
        counter_value = int(self.__redis_conn.get(self.__counter_name))
        logging.debug(f"Counter - Initial Value - {self.__counter_name}: {counter_value}")
        self.__mutex = redis_lock.Lock(
            redis_conn, "lock:switch:{}".format(name), expire=expire, auto_renewal=auto_renew
        )

    def acquire(self, lock):
        self.__mutex.acquire()
        self.__redis_conn.incr(self.__counter_name)
        self.__redis_conn.expire(self.__counter_name, self.__expire)
        counter_value = int(self.__redis_conn.get(self.__counter_name))
        logging.debug(f"Counter {self.__counter_name}: {counter_value}")
        if counter_value == 1:
            lock.acquire()
        self.__mutex.release()

    def release(self, lock):
        self.__mutex.acquire()
        self.__redis_conn.decr(self.__counter_name)
        self.__redis_conn.expire(self.__counter_name, self.__expire)
        counter_value = int(self.__redis_conn.get(self.__counter_name))
        logging.debug(f"Counter {self.__counter_name}: {counter_value}")
        if counter_value == 0:
            lock.reset()
        self.__mutex.release()

    def reset(self):
        self.__mutex.reset()
        self.__redis_conn.set(self.__counter_name, 0)
