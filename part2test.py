import unittest

from kvstore import InMemoryKVStore
from student import DEADLOCK, USER, TransactionCoordinator, TransactionHandler

class Part2Test(unittest.TestCase):
    def test_deadlock_rw_rw(self):
        # Should pass after 2
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        t2 = TransactionHandler(lock_table, 2, store)
        coordinator = TransactionCoordinator(lock_table)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_get('b'), 'No such key')
        self.assertEqual(t0.perform_put('a', 'a0'), 'Success')
        self.assertEqual(t0.perform_put('b', 'b0'), 'Success')
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t1.perform_get('a'), 'a0')                  # T1 R(a)
        self.assertEqual(t2.perform_get('b'), 'b0')                  # T2 R(b)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t1.perform_put('b', 'b1'), None)            # T1 W(b)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t1.check_lock(), None)
        self.assertEqual(t2.perform_put('a', 'a1'), None)            # T2 W(a)
        abort_id = coordinator.detect_deadlocks()
        self.assertTrue(abort_id == 1 or abort_id == 2)

    def test_deadlock_wr_rw(self):
        # Should pass after 2
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        t2 = TransactionHandler(lock_table, 2, store)
        coordinator = TransactionCoordinator(lock_table)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_get('b'), 'No such key')
        self.assertEqual(t0.perform_put('a', 'a0'), 'Success')
        self.assertEqual(t0.perform_put('b', 'b0'), 'Success')
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t1.perform_put('a', 'a1'), 'Success')       # T1 W(a)
        self.assertEqual(t2.perform_get('b'), 'b0')                  # T2 R(b)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t1.perform_put('b', 'b1'), None)            # T1 W(b)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t1.check_lock(), None)
        self.assertEqual(t2.perform_get('a'), None)                  # T2 R(a)
        abort_id = coordinator.detect_deadlocks()
        self.assertTrue(abort_id == 1 or abort_id == 2)

    def test_deadlock_wr_ww_rr(self):
        # Should pass after 2
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        t2 = TransactionHandler(lock_table, 2, store)
        t3 = TransactionHandler(lock_table, 3, store)
        coordinator = TransactionCoordinator(lock_table)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_get('b'), 'No such key')
        self.assertEqual(t0.perform_put('a', 'a0'), 'Success')
        self.assertEqual(t0.perform_put('b', 'b0'), 'Success')
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t1.perform_put('a', 'a1'), 'Success')       # T1 W(a)
        self.assertEqual(t2.perform_get('b'), 'b0')                  # T2 R(b)
        self.assertEqual(t3.perform_put('c', 'c1'), 'Success')       # T3 W(c)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t1.perform_put('b', 'b1'), None)            # T1 W(b)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t1.check_lock(), None)
        self.assertEqual(t2.perform_get('c'), None)                  # T2 R(c)
        self.assertEqual(t3.perform_get('a'), None)                  # T3 R(a)
        abort_id = coordinator.detect_deadlocks()
        self.assertTrue(abort_id == 1 or abort_id == 2 or abort_id == 3)

    def test_deadlock_ww_rw(self):
        # Should pass after 2
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        t2 = TransactionHandler(lock_table, 2, store)
        coordinator = TransactionCoordinator(lock_table)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_get('b'), 'No such key')
        self.assertEqual(t0.perform_put('a', 'a0'), 'Success')
        self.assertEqual(t0.perform_put('b', 'b0'), 'Success')
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t1.perform_put('a', 'a1'), 'Success')       # T1 W(a)
        self.assertEqual(t2.perform_get('b'), 'b0')                  # T2 R(b)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t1.perform_put('b', 'b1'), None)            # T1 W(b)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        self.assertEqual(t1.check_lock(), None)
        self.assertEqual(t2.perform_put('a', 'a2'), None)            # T2 W(a)
        abort_id = coordinator.detect_deadlocks()
        self.assertTrue(abort_id == 1 or abort_id == 2)


    def test_unlock_rrr(self):
        # Should pass after 1.3
        lock_table = {}
        store = InMemoryKVStore()
        coordinator = TransactionCoordinator(lock_table)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        t2 = TransactionHandler(lock_table, 2, store)
        t3 = TransactionHandler(lock_table, 3, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')        # T0 R(a)
        self.assertEqual(t1.perform_get('a'), 'No such key')        # T1 R(a)
        self.assertEqual(t2.perform_get('a'), 'No such key')
        self.assertEqual(t3.perform_put('a', '0'), None)            # T0 W(a)
        self.assertEqual(coordinator.detect_deadlocks(), None)

########################################################################
############## STAFF TEST ERRORS #######################################

    def test_staff_deadlock_rw_rw(self):
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        coordinator = TransactionCoordinator(lock_table)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        # t0 has X lock on a; t1 wants X lock
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t1.perform_put('a', 'a1'), None)
        # t1 has S lock on b; t0 wants X lock
        self.assertEqual(t1.perform_get('b'), 'No such key')
        self.assertEqual(t0.perform_put('b', 'b0'), None)

        abort_id = coordinator.detect_deadlocks()
        self.assertTrue(abort_id == 0 or abort_id == 1)

    def test_staff_deadlock_wr_rw(self):
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        coordinator = TransactionCoordinator(lock_table)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        # t0 has X lock on a; t1 wants X lock
        self.assertEqual(t0.perform_put('a', 'a1'), 'Success')
        self.assertEqual(t1.perform_get('a'), None)
        # t1 has S lock on b; t0 wants X lock
        self.assertEqual(t1.perform_get('b'), 'No such key')
        self.assertEqual(t0.perform_put('b', 'b0'), None)

        abort_id = coordinator.detect_deadlocks()
        self.assertTrue(abort_id == 0 or abort_id == 1)

    def test_staff_deadlock_ww_rw(self):
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        coordinator = TransactionCoordinator(lock_table)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        # t0 has X lock on a; t1 wants X lock
        self.assertEqual(t0.perform_put('a', 'a0'), 'Success')
        self.assertEqual(t1.perform_put('a', 'a1'), None)
        # t1 has S lock on b; t0 wants X lock
        self.assertEqual(t1.perform_get('b'), 'No such key')
        self.assertEqual(t0.perform_put('b', 'b0'), None)

        abort_id = coordinator.detect_deadlocks()
        self.assertTrue(abort_id == 0 or abort_id == 1)


########################################################################
############## STAFF TEST FAILURES #####################################

    def test_staff_deadlock_rw_rw_rw(self):
        lock_table = {}
        store = InMemoryKVStore()
        t2 = TransactionHandler(lock_table, 2, store)
        t4 = TransactionHandler(lock_table, 4, store)
        t6 = TransactionHandler(lock_table, 6, store)
        coordinator = TransactionCoordinator(lock_table)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        # t2 has S lock on a; t6 wants X lock
        self.assertEqual(t2.perform_get('a'), 'No such key')
        self.assertEqual(t6.perform_put('a', 'a1'), None)
        # t4 has S lock on b; t2 wants X lock
        self.assertEqual(t4.perform_get('b'), 'No such key')
        self.assertEqual(t2.perform_put('b', 'b1'), None)
        # t6 has S lock on c; t4 wants X lock
        self.assertEqual(t6.perform_get('c'), 'No such key')
        self.assertEqual(t4.perform_put('c', 'c1'), None)

        abort_id = coordinator.detect_deadlocks()
        self.assertTrue(abort_id == 2 or abort_id == 4 or abort_id == 6)

    def test_staff_deadlock_two_cycles(self):
        lock_table = {}
        store = InMemoryKVStore()
        t2 = TransactionHandler(lock_table, 2, store)
        t4 = TransactionHandler(lock_table, 4, store)
        t6 = TransactionHandler(lock_table, 6, store)
        t8 = TransactionHandler(lock_table, 8, store)
        coordinator = TransactionCoordinator(lock_table)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        # t2 has S lock on a; t4 wants X lock
        self.assertEqual(t2.perform_get('a'), 'No such key')
        self.assertEqual(t4.perform_put('a', 'a1'), None)
        # t4 has S lock on b; t2 wants X lock
        self.assertEqual(t4.perform_get('b'), 'No such key')
        self.assertEqual(t2.perform_put('b', 'b1'), None)
        # t6 has S lock on a; t8 wants X lock
        self.assertEqual(t6.perform_get('c'), 'No such key')
        self.assertEqual(t8.perform_put('c', 'c1'), None)
        # t8 has S lock on b; t6 wants X lock
        self.assertEqual(t8.perform_get('d'), 'No such key')
        self.assertEqual(t6.perform_put('d', 'd1'), None)

        abort_id = coordinator.detect_deadlocks()
        self.assertTrue(abort_id == 2 or abort_id == 4 or abort_id == 6 or abort_id == 8)

        abort_id = coordinator.detect_deadlocks()
        self.assertTrue(abort_id == 2 or abort_id == 4 or abort_id == 6 or abort_id == 8)

    def test_staff_deadlock_upgrade(self):
        lock_table = {}
        store = InMemoryKVStore()
        t2 = TransactionHandler(lock_table, 2, store)
        t4 = TransactionHandler(lock_table, 4, store)
        coordinator = TransactionCoordinator(lock_table)
        self.assertEqual(coordinator.detect_deadlocks(), None)
        # t2, t4 has S lock on a; t2 wants X lock
        self.assertEqual(t2.perform_get('a'), 'No such key')
        self.assertEqual(t4.perform_get('a'), 'No such key')
        self.assertEqual(t2.perform_put('a', 'a1'), None)
        # t4 now wants X lock
        self.assertEqual(t4.perform_put('a', 'a2'), None)

        abort_id = coordinator.detect_deadlocks()
        self.assertTrue(abort_id == 2 or abort_id == 4)




if __name__ == '__main__':
    unittest.main()
