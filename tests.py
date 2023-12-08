import copy
import threading
import time
import redis
from unittest import TestCase

from redis_rw_lock import RWLock, AcquisitionTimeout


class Writer(threading.Thread):
    def __init__(self, buffer_, rw_lock, init_sleep, locked_sleep, to_write):
        """
        @param buffer_: common buffer_ shared by the readers and writers
        @type buffer_: list
        @type rw_lock: L{RWLock}
        @param init_sleep: sleep time before doing any action
        @type init_sleep: C{float}
        @param locked_sleep: sleep time while in critical section
        @type locked_sleep: C{float}
        @param to_write: data that will be appended to the buffer
        """
        threading.Thread.__init__(self)
        self.__buffer = buffer_
        self.__rw_lock = rw_lock
        self.__init_sleep = init_sleep
        self.__locked_sleep = locked_sleep
        self.__to_write = to_write
        self.entry_time = None
        """Time of entry to the critical section"""
        self.exit_time = None
        """Time of exit from the critical section"""
        self.acquisition_timeout_time = None

    def run(self):
        time.sleep(self.__init_sleep)
        try:
            self.__rw_lock.acquire()
            self.entry_time = time.time()
            time.sleep(self.__locked_sleep)
            self.__buffer.append(self.__to_write)
            self.exit_time = time.time()
            self.__rw_lock.release()
        except AcquisitionTimeout:
            self.acquisition_timeout_time = time.time()


class Reader(threading.Thread):
    def __init__(self, buffer_, rw_lock, init_sleep, locked_sleep):
        """
        @param buffer_: common buffer shared by the readers and writers
        @type buffer_: list
        @type rw_lock: L{RWLock}
        @param init_sleep: sleep time before doing any action
        @type init_sleep: C{float}
        @param locked_sleep: sleep time while in critical section
        @type locked_sleep: C{float}
        """
        threading.Thread.__init__(self)
        self.__buffer = buffer_
        self.__rw_lock = rw_lock
        self.__init_sleep = init_sleep
        self.__locked_sleep = locked_sleep
        self.buffer_read = None
        """a copy of a the buffer read while in critical section"""
        self.entry_time = None
        """Time of entry to the critical section"""
        self.exit_time = None
        """Time of exit from the critical section"""
        self.acquisition_timeout_time = None

    def run(self):
        time.sleep(self.__init_sleep)
        try:
            self.__rw_lock.acquire()
            self.entry_time = time.time()
            time.sleep(self.__locked_sleep)
            self.buffer_read = copy.deepcopy(self.__buffer)
            self.exit_time = time.time()
            self.__rw_lock.release()
        except TimeoutLock.AcquisitionTimeout:
            self.acquisition_timeout_time = time.time()


class RWLockTestCase(TestCase):
    def setUp(self):
        self.buffer_ = []

    def test_readers_nonexclusive_access(self):
        """
        (all thread indices are zero-based)

        After thread 0 has started, thread 1 and 2 will compete for the lock. 1
        will get it (writer priority) and will write '1' to the buffer, which
        will be observed by thread 2 and 3 which are readers.
        """
        threads = [
            self.make_reader_thread(init_sleep=0, locked_sleep=1),
            self.make_writer_thread(init_sleep=0.4, locked_sleep=1, to_write=1),
            self.make_reader_thread(init_sleep=1, locked_sleep=1),
            self.make_reader_thread(init_sleep=1.2, locked_sleep=0.2),
        ]

        self.start_and_join_threads(threads)

        self.assertEqual([], threads[0].buffer_read)
        self.assertEqual([1], threads[2].buffer_read)
        self.assertEqual([1], threads[3].buffer_read)
        self.assertTrue(threads[1].exit_time <= threads[2].entry_time)
        # Thread 2 starts before thread 3, but thread 3 exits before thread 2,
        # ensuring that thread 3 holds the read lock at the same time as thread 2
        self.assertTrue(threads[2].entry_time <= threads[3].entry_time)
        self.assertTrue(threads[3].exit_time < threads[2].exit_time)

    def test_writers_exclusive_access(self):
        """
        (all thread indices are zero-based)

        Test that writers get exclusive access. Thread 1 will attempt to get
        the lock early but has to wait for thread 0 to finish. Thread 2 will
        observe values written by both thread 0 and 1 in the correct order.
        """
        threads = [
            self.make_writer_thread(init_sleep=0, locked_sleep=0.4, to_write=1),
            self.make_writer_thread(init_sleep=0.1, locked_sleep=0, to_write=2),
            self.make_reader_thread(init_sleep=0.2, locked_sleep=0),
        ]

        self.start_and_join_threads(threads)

        self.assertEqual([1, 2], threads[2].buffer_read)
        self.assertTrue(threads[0].exit_time <= threads[1].entry_time)
        self.assertTrue(threads[1].exit_time <= threads[2].exit_time)

    def test_writer_priority(self):
        """
        (all thread indices are zero-based)

        Thread 1 will hold the lock until T=0.5. At that point, threads 2, 3
        and 4 will all compete for the lock. Only thread 2 is a writer, who
        must acquire the lock first.

        All reader threads will observe the initial value '1' written by thread
        0 but thread 3 and 4 will observe '2', guaranteeing that they will get
        the lock after the writer thread 2 acquired it first.
        """
        threads = [
            self.make_writer_thread(init_sleep=0, locked_sleep=0, to_write=1),
            self.make_reader_thread(init_sleep=0.1, locked_sleep=0.4),
            self.make_writer_thread(init_sleep=0.2, locked_sleep=0, to_write=2),
            self.make_reader_thread(init_sleep=0.3, locked_sleep=0),
            self.make_reader_thread(init_sleep=0.3, locked_sleep=0),
        ]

        self.start_and_join_threads(threads)

        self.assertEqual([1], threads[1].buffer_read)
        self.assertEqual([1, 2], threads[3].buffer_read)
        self.assertEqual([1, 2], threads[4].buffer_read)
        self.assertTrue(threads[0].exit_time < threads[1].entry_time)
        self.assertTrue(threads[1].exit_time <= threads[2].entry_time)
        self.assertTrue(threads[2].exit_time <= threads[3].entry_time)
        self.assertTrue(threads[2].exit_time <= threads[4].entry_time)

    def test_many_writers_priority(self):
        """
        (all thread indices are zero-based)

        Reader thread will release the lock at T=0.7, at which point threads 2,
        3, 4 and 5 will be competing for the lock. Thread 2 and 5 are writers,
        which must both have priority, despite the fact that 3 and 4 tried to
        get the lock before 5.
        """
        threads = [
            self.make_writer_thread(init_sleep=0, locked_sleep=0, to_write=1),
            self.make_reader_thread(init_sleep=0.1, locked_sleep=0.6),
            self.make_writer_thread(init_sleep=0.2, locked_sleep=0.1, to_write=2),
            self.make_reader_thread(init_sleep=0.3, locked_sleep=0),
            self.make_reader_thread(init_sleep=0.4, locked_sleep=0),
            self.make_writer_thread(init_sleep=0.5, locked_sleep=0.1, to_write=3),
        ]

        self.start_and_join_threads(threads)

        self.assertEqual([1], threads[1].buffer_read)
        self.assertEqual([1, 2, 3], threads[3].buffer_read)
        self.assertEqual([1, 2, 3], threads[4].buffer_read)
        self.assertTrue(threads[0].exit_time < threads[1].entry_time)
        # After thread 1 terminates, threads 2 and 5 may run (in any order)
        self.assertTrue(threads[1].exit_time <= threads[2].entry_time)
        self.assertTrue(threads[1].exit_time <= threads[5].entry_time)
        # Threads 3 and 4 must start only when both 2 and 5 have finished
        self.assertTrue(threads[2].exit_time <= threads[3].entry_time)
        self.assertTrue(threads[2].exit_time <= threads[4].entry_time)
        self.assertTrue(threads[5].exit_time <= threads[3].entry_time)
        self.assertTrue(threads[5].exit_time <= threads[4].entry_time)

    def test_write_lock_acquisition_timeout(self):
        """
        (all thread indices are zero-based)

        Thread 1 will keep the read-lock until T=1.5. Thread 3 will attempt to
        get an exclusive write lock but will fail at T=1.2 so its value '2' will
        not be written to the buffer.

        Note that due to the underlying Locking API, we cannot express the
        acquisition timeout in fractions of a second.
        """
        threads = [
            self.make_writer_thread(init_sleep=0, locked_sleep=0, to_write=1),
            self.make_reader_thread(init_sleep=0.1, locked_sleep=1.4),
            self.make_writer_thread(init_sleep=0.2, locked_sleep=0, acq_timeout=1, to_write=2),
            self.make_reader_thread(init_sleep=0.3, locked_sleep=0),
        ]

        self.start_and_join_threads(threads)

        self.assertEqual([1], threads[1].buffer_read)
        self.assertEqual([1], threads[3].buffer_read)

        # Acquisition timeout for thread 2 occured while thread 1 was holding the lock
        self.assertTrue(threads[2].acquisition_timeout_time > threads[1].entry_time)
        self.assertTrue(threads[2].acquisition_timeout_time < threads[1].exit_time)

    def test_read_lock_acquisition_timeout(self):
        """
        (all thread indices are zero-based)

        Thread 0 will keep the write-lock until T=1.5. Thread 1 will attempt to get
        a read lock but will fail at T=1.1 with an acquisition timeout.

        Note that due to the underlying Locking API, we cannot express the
        acquisition timeout in fractions of a second.
        """
        threads = [
            self.make_writer_thread(init_sleep=0, locked_sleep=1.5, to_write=1),
            self.make_reader_thread(init_sleep=0.1, locked_sleep=0, acq_timeout=1),
        ]

        self.start_and_join_threads(threads)

        # Thread 1 didn't get to set the buffer
        self.assertEqual(None, threads[1].buffer_read)

        # Acquisition timeout for thread 1 occured while thread 0 was holding the lock
        self.assertTrue(threads[1].acquisition_timeout_time > threads[0].entry_time)
        self.assertTrue(threads[1].acquisition_timeout_time < threads[0].exit_time)

    def test_multiple_write_lock_acquisition_timeouts(self):
        """
        (all thread indices are zero-based)

        Thread 0 will hold the read-lock until T=1.5 while 3 writers attempt to
        get it. The first two will time-out and only the last writer will get
        it and write '3', which is observed by thread 4.

        Note that due to the underlying Locking API, we cannot express the
        acquisition timeout in fractions of a second.
        """
        threads = [
            self.make_reader_thread(init_sleep=0, locked_sleep=1.5),
            self.make_writer_thread(init_sleep=0.2, locked_sleep=0, to_write=1, acq_timeout=1),
            self.make_writer_thread(init_sleep=0.4, locked_sleep=0, to_write=2, acq_timeout=1),
            self.make_writer_thread(init_sleep=0.6, locked_sleep=0, to_write=3, acq_timeout=1),
            self.make_reader_thread(init_sleep=0.7, locked_sleep=0),
        ]

        self.start_and_join_threads(threads)

        # Thread 4 will only read what thread 3 wrote
        self.assertEqual([3], threads[4].buffer_read)

        # Thread 1 and 2 will time out before thread 0 finishes
        self.assertTrue(threads[1].acquisition_timeout_time < threads[0].exit_time)
        self.assertTrue(threads[2].acquisition_timeout_time < threads[0].exit_time)

        # Thread 3 will succeed and gets the lock when thread 0 finishes
        self.assertTrue(threads[3].entry_time > threads[0].exit_time)

    def make_reader_thread(
        self, init_sleep, locked_sleep, acq_timeout=None, name="RWLock", auto_renew=False
    ):
        redis_conn = redis.StrictRedis()
        return Reader(
            self.buffer_,
            RWLock(
                redis_conn,
                name,
                mode=RWLock.READ,
                acquire_timeout=acq_timeout,
                auto_renew=auto_renew,
            ),
            init_sleep=init_sleep,
            locked_sleep=locked_sleep,
        )

    def make_writer_thread(
        self, init_sleep, locked_sleep, to_write, acq_timeout=None, name="RWLock", auto_renew=False
    ):
        redis_conn = redis.StrictRedis()
        return Writer(
            self.buffer_,
            RWLock(
                redis_conn,
                name,
                mode=RWLock.WRITE,
                acquire_timeout=acq_timeout,
                auto_renew=auto_renew,
            ),
            init_sleep=init_sleep,
            locked_sleep=locked_sleep,
            to_write=to_write,
        )

    def start_and_join_threads(self, threads):
        for t in threads:
            t.start()
        for t in threads:
            t.join()
