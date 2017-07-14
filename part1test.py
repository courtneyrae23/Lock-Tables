import unittest

from kvstore import InMemoryKVStore
from student import USER, TransactionHandler

class Part1Test(unittest.TestCase):
    def test_commit(self):
        # Sanity check
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_put('a', '0'), 'Success')
        self.assertEqual(t0.perform_get('a'), '0')
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(store.get('a'), '0')

    def test_abort(self):
        # Sanity check
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_put('a', '0'), 'Success')
        self.assertEqual(t0.perform_get('a'), '0')
        self.assertEqual(t0.abort(USER), 'User Abort')
        self.assertEqual(store.get('a'), None)

    def test_multiple_read(self):
        # Sanity check
        lock_table = {}
        store = InMemoryKVStore()
        store.put('a', '0')
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        t2 = TransactionHandler(lock_table, 2, store)
        self.assertEqual(t0.perform_get('a'), '0')
        self.assertEqual(t1.perform_get('a'), '0')
        self.assertEqual(t2.perform_get('a'), '0')
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(t1.abort(USER), 'User Abort')
        self.assertEqual(t2.perform_get('a'), '0')

    def test_rw(self):
        # Should pass after 1.1
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t1.perform_get('a'), 'No such key')
        self.assertEqual(t1.perform_put('a', '0'), None)
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(store.get('a'), None)

    def test_wr(self):
        # Should pass after 1.1
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_put('a', '0'), 'Success')
        self.assertEqual(t1.perform_get('a'), None)
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(store.get('a'), '0')

    def test_ww(self):
        # Should pass after 1.1
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_put('a', '0'), 'Success')
        self.assertEqual(t1.perform_put('a', '1'), None)
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(store.get('a'), '0')

    def test_commit_commit(self):
        # Should pass after 1.2
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_put('a', '0'), 'Success')
        self.assertEqual(t0.perform_get('a'), '0')
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(t1.perform_get('a'), '0')
        self.assertEqual(t1.perform_put('a', '1'), 'Success')
        self.assertEqual(t1.perform_get('a'), '1')
        self.assertEqual(t1.commit(), 'Transaction Completed')
        self.assertEqual(store.get('a'), '1')

    def test_abort_commit(self):
        # Should pass after 1.2
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_put('a', '0'), 'Success')
        self.assertEqual(t0.perform_get('a'), '0')
        self.assertEqual(t0.abort(USER), 'User Abort')
        self.assertEqual(t1.perform_get('a'), 'No such key')
        self.assertEqual(t1.perform_put('a', '1'), 'Success')
        self.assertEqual(t1.perform_get('a'), '1')
        self.assertEqual(t1.commit(), 'Transaction Completed')
        self.assertEqual(store.get('a'), '1')

    def test_commit_abort_commit(self):
        # Should pass after 1.2
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        t2 = TransactionHandler(lock_table, 2, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_put('a', '0'), 'Success')
        self.assertEqual(t0.perform_get('a'), '0')
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(t1.perform_get('a'), '0')
        self.assertEqual(t1.perform_put('a', '1'), 'Success')
        self.assertEqual(t1.perform_get('a'), '1')
        self.assertEqual(t1.abort(USER), 'User Abort')
        self.assertEqual(t2.perform_get('a'), '0')
        self.assertEqual(t2.perform_put('a', '2'), 'Success')
        self.assertEqual(t2.perform_get('a'), '2')
        self.assertEqual(t2.commit(), 'Transaction Completed')
        self.assertEqual(store.get('a'), '2')

    def test_abort_queue(self):
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_put('a', '0'), 'Success')
        self.assertEqual(t0.perform_get('a'), '0')
        self.assertEqual(t1.perform_get('a'), None)
        self.assertEqual(t1.perform_put('b', '1'), 'Success')
        self.assertEqual(t1.abort(USER), 'User Abort')
        self.assertEqual(store.get('a'), '0')
        self.assertEqual(store.get('b'), None)

    def test_upgrades(self):
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        t2 = TransactionHandler(lock_table, 2, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t1.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_put('a', '0'), None)
        self.assertEqual(t1.perform_put('a', '1'), None)
        self.assertEqual(t2.perform_put('a', '2'), None)
        self.assertEqual(t1.abort(USER), 'User Abort')
        self.assertEqual(t0.check_lock(), 'Success')
        self.assertEqual(store.get('a'), '0')

    def test_rrr(self):
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        t2 = TransactionHandler(lock_table, 2, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t1.perform_get('a'), 'No such key')
        self.assertEqual(t2.perform_get('a'), 'No such key')

    def test_rwr(self):
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        t2 = TransactionHandler(lock_table, 2, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t1.perform_put('a', '1'), None)
        self.assertEqual(t2.perform_get('a'), None)

    def test_unlock_rw(self):
        # Should pass after 1.3
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')         # T0 R(a)
        self.assertEqual(t1.perform_get('a'), 'No such key')
        self.assertEqual(t1.perform_put('a', '0'), None)          # T1 W(a)
        self.assertEqual(t1.check_lock(), None)
        self.assertEqual(t1.check_lock(), None)
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(t1.check_lock(), 'Success')
        self.assertEqual(t1.perform_get('a'), '0')

    def test_unlock_wr(self):
        # Should pass after 1.3
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_put('a', '0'), 'Success')        # T0 W(a)
        self.assertEqual(t1.perform_get('a'), None)                  # T1 R(a)
        self.assertEqual(t1.check_lock(), None)
        self.assertEqual(t1.check_lock(), None)
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(t1.check_lock(), '0')
        self.assertEqual(t1.perform_get('a'), '0')


    def test_unlock_ww(self):
        # Should pass after 1.3
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_put('a', '0'), 'Success')        # T0 W(a)
        self.assertEqual(t1.perform_put('a', '1'), None)             # T1 W(a)
        self.assertEqual(t1.check_lock(), None)
        self.assertEqual(t1.check_lock(), None)
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(t1.check_lock(), 'Success')
        self.assertEqual(t1.perform_get('a'), '1')

    def test_unlock_rrw(self):
        # Should pass after 1.3
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')        # T0 R(a)
        self.assertEqual(t1.perform_get('a'), 'No such key')        # T1 R(a)
        self.assertEqual(t0.perform_put('a', '0'), None)            # T0 W(a)
        self.assertEqual(t0.check_lock(), None)
        self.assertEqual(t0.check_lock(), None)
        self.assertEqual(t1.commit(), 'Transaction Completed')
        self.assertEqual(t0.check_lock(), 'Success')
        self.assertEqual(t0.perform_get('a'), '0')



###############################################################
####### STAFF TEST ERRORS #####################################
    def test_staff_block_read_read_read(self):
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        t2 = TransactionHandler(lock_table, 2, store)
        t3 = TransactionHandler(lock_table, 3, store)

        self.assertEqual(t0.perform_put('a', '0'), 'Success')

        self.assertEqual(t1.perform_get('a'), None)
        self.assertEqual(t2.perform_get('a'), None)  
        self.assertEqual(t3.perform_get('a'), None)

        self.assertEqual(t0.commit(), 'Transaction Completed')

        self.assertEqual(t2.check_lock(), '0')
        self.assertEqual(t1.check_lock(), '0')
        self.assertEqual(t3.check_lock(), '0')

    def test_staff_block_read_write_read(self):
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        t2 = TransactionHandler(lock_table, 2, store)
        t3 = TransactionHandler(lock_table, 3, store)

        self.assertEqual(t0.perform_put('a', '0'), 'Success')

        self.assertEqual(t1.perform_get('a'), None)
        self.assertEqual(t2.perform_put('a', '2'), None)  
        self.assertEqual(t3.perform_get('a'), None)

        self.assertEqual(t0.commit(), 'Transaction Completed')

        self.assertEqual(t1.check_lock(), '0')
        self.assertEqual(t2.check_lock(), None)
        self.assertEqual(t3.check_lock(), None)

        self.assertEqual(t1.commit(), 'Transaction Completed')

        self.assertEqual(t2.check_lock(), 'Success')
        self.assertEqual(t3.check_lock(), None)

        self.assertEqual(t2.commit(), 'Transaction Completed')

        self.assertEqual(t3.check_lock(), '2')


    def test_staff_fifo_queue(self):
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        t2 = TransactionHandler(lock_table, 2, store)
        t3 = TransactionHandler(lock_table, 3, store)
        t4 = TransactionHandler(lock_table, 4, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t0.perform_put('a', '0'), 'Success')        # T0 W(a)
        self.assertEqual(t1.perform_get('a'), None)                  # T1 R(a)
        self.assertEqual(t2.perform_put('a', '2'), None)
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(t1.check_lock(), '0')
        self.assertEqual(t1.perform_get('a'), '0')
        self.assertEqual(t3.perform_get('a'), None)
        self.assertEqual(t4.perform_get('a'), None)
        self.assertEqual(t1.commit(), 'Transaction Completed')
        self.assertEqual(t2.check_lock(), 'Success')
        self.assertEqual(t2.commit(), 'Transaction Completed')
        self.assertEqual(t3.check_lock(), '2')
        self.assertEqual(t4.check_lock(), '2')

    def test_staff_multiple_write_abort(self):
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        self.assertEqual(t0.perform_put('a', '0'), 'Success')
        self.assertEqual(t0.perform_put('a', '1'), 'Success') 
        self.assertEqual(t0.perform_put('a', '2'), 'Success') 
        self.assertEqual(t0.perform_put('a', '3'), 'Success')
        self.assertEqual(t0.abort(USER), 'User Abort')
        self.assertEqual(store.get('a'), None) 

    def test_staff_multiple_write_commit(self):
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        self.assertEqual(t0.perform_put('a', '0'), 'Success')
        self.assertEqual(t0.perform_put('a', '1'), 'Success') 
        self.assertEqual(t0.perform_put('a', '2'), 'Success') 
        self.assertEqual(t0.perform_put('a', '3'), 'Success')
        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(store.get('a'), '3')

    def test_staff_lock_upgrade(self):
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        t1 = TransactionHandler(lock_table, 1, store)
        t2 = TransactionHandler(lock_table, 2, store)
        self.assertEqual(t0.perform_get('a'), 'No such key')
        self.assertEqual(t1.perform_get('a'), 'No such key')
        self.assertEqual(t2.perform_put('a', '1'), None)
        self.assertEqual(t0.perform_put('a', '1'), None)

        self.assertEqual(t1.commit(), 'Transaction Completed')
        self.assertEqual(t0.check_lock(), 'Success')

        self.assertEqual(t0.commit(), 'Transaction Completed')
        self.assertEqual(t2.check_lock(), 'Success')

    def test_staff_read_x_lock(self):
        lock_table = {}
        store = InMemoryKVStore()
        t0 = TransactionHandler(lock_table, 0, store)
        self.assertEqual(t0.perform_put('a', '0'), 'Success')
        self.assertEqual(t0.perform_put('a', '1'), 'Success') 
        self.assertEqual(t0.perform_get('a'), '1') 
        self.assertEqual(t0.perform_put('a', '3'), 'Success')

if __name__ == '__main__':
    unittest.main()
