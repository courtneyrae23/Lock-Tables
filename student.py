import logging

from kvstore import DBMStore, InMemoryKVStore

LOG_LEVEL = logging.WARNING

KVSTORE_CLASS = InMemoryKVStore

"""
Possible abort modes.
"""
USER = 0
DEADLOCK = 1

"""
Part I: Implementing request handling methods for the transaction handler

The transaction handler has access to the following objects:

self._lock_table: the global lock table. More information in the README.

self._acquired_locks: a list of locks acquired by the transaction. Used to
release locks when the transaction commits or aborts. This list is initially
empty.

self._desired_lock: the lock that the transaction is waiting to acquire as well
as the operation to perform. This is initialized to None.

self._xid: this transaction's ID. You may assume each transaction is assigned a
unique transaction ID.

self._store: the in-memory key-value store. You may refer to kvstore.py for
methods supported by the store.

self._undo_log: a list of undo operations to be performed when the transaction
is aborted. The undo operation is a tuple of the form (@key, @value). This list
is initially empty.

You may assume that the key/value inputs to these methods are already type-
checked and are valid.
"""
class TransactionHandler:

    def __init__(self, lock_table, xid, store):
        self._lock_table = lock_table
        self._acquired_locks = []
        self._desired_lock = None
        self._xid = xid
        self._store = store
        self._undo_log = []

    def perform_put(self, key, value):
        """
        Handles the PUT request. You should first implement the logic for
        acquiring the exclusive lock. If the transaction can successfully
        acquire the lock associated with the key, insert the key-value pair
        into the store.

        Hint: if the lock table does not contain the key entry yet, you should
        create one.
        Hint: be aware that lock upgrade may happen.
        Hint: remember to update self._undo_log so that we can undo all the
        changes if the transaction later gets aborted. See the code in abort()
        for the exact format.

        @param self: the transaction handler.
        @param key, value: the key-value pair to be inserted into the store.

        @return: if the transaction successfully acquires the lock and performs
        the insertion/update, returns 'Success'. If the transaction cannot
        acquire the lock, returns None, and saves the lock that the transaction
        is waiting to acquire in self._desired_lock.

        lock_table = {key, ([(id, X) (id, S)], [])}
        """
        # Part 1.1: your code here!
        if self._lock_table.has_key(key):
            upgrade = False
            only_this_x = True
            for lock in self._lock_table[key][0]:
                if lock[0] == self._xid:
                    upgrade = True
                else:
                    only_this_x = False 
            # It's an upgrade or I already have this lock!
            if upgrade and only_this_x:
                self._lock_table[key][0] = [(self._xid, "X")]
                self._undo_log.append((key, self._store.get(key)))
                if (key, "S") in self._acquired_locks:
                    self._acquired_locks.remove((key, "S"))
                    self._acquired_locks.append((key, "X"))
                self._store.put(key, value)
                return 'Success'
            # Something else has this key, but so do we and we want to upgrade
            elif upgrade:
                self._lock_table[key][1].insert(0, (self._xid, "X"))
                self._desired_lock = (key, "X", value)
                return
            # I need to get in the back of the line
            else:
                self._lock_table[key][1].append((self._xid, "X"))
                self._desired_lock = (key, "X", value)
                return
        else:
            self._lock_table[key] = [[(self._xid, "X")], []] 
            self._undo_log.append((key, self._store.get(key)))
            self._acquired_locks.append((key, "X"))
            self._store.put(key, value)
            return 'Success'

    def perform_get(self, key):
        """
        Handles the GET request. You should first implement the logic for
        acquiring the shared lock. If the transaction can successfully acquire
        the lock associated with the key, read the value from the store.

        Hint: if the lock table does not contain the key entry yet, you should
        create one.

        @param self: the transaction handler.
        @param key: the key to look up from the store.

        @return: if the transaction successfully acquires the lock and reads
        the value, returns the value. If the key does not exist, returns 'No
        such key'. If the transaction cannot acquire the lock, returns None,
        and saves the lock that the transaction is waiting to acquire in
        self._desired_lock.
        """
        # Part 1.1: your code here!
        if self._lock_table.has_key(key):
            only_shared = True
            only_this_x = True
            for lock in self._lock_table[key][0]:
                if lock[0] != self._xid:
                    only_this_x = False
                if lock[1] != "S":
                    only_shared = False
            if only_this_x:
                #Special case when I hold the X lock:
                value = self._store.get(key)
                if value is None:
                    return 'No such key'
                else:
                    return value
            # Others are sharing
            elif only_shared:
                # No one else is waiting
                if len(self._lock_table[key][1]) == 0:
                    self._lock_table[key][0].append((self._xid, "S"))
                    self._acquired_locks.append((key, "S"))
                    value = self._store.get(key)
                    if value is None:
                        return 'No such key'
                    else:
                        return value
                #Someone else is waiting -> Go to the back of the line
                else:
                    self._lock_table[key][1].append((self._xid, "S"))
                    self._desired_lock = (key, "S")
                    return
            # I need to get in the back of the line
            else:
                self._lock_table[key][1].append((self._xid, "S"))
                self._desired_lock = (key, "S")
                return
        else:
            self._lock_table[key] = [[(self._xid, "S")], []]
            self._acquired_locks.append((key, "S"))
            value = self._store.get(key)
            if value is None:
                return 'No such key'
            else:
                return value

    def release_and_grant_locks(self):
        """
        Releases all locks acquired by the transaction and grants them to the
        next transactions in the queue. This is a helper method that is called
        during transaction commits or aborts. 

        Hint: you can use self._acquired_locks to get a list of locks acquired
        by the transaction.
        Hint: be aware that lock upgrade may happen.

        @param self: the transaction handler.
        """
        for l in self._acquired_locks:
            only_this_x = True
            only_shared = True
            for lock in self._lock_table[l[0]][0]:
                if lock[0] != self._xid:
                    only_this_x = False
                if lock[1] == "X":
                    only_shared = False

            # I am the only lock so I need to pass control to the next item in queue 
            if only_this_x:
                # If nothing in the queue, remove from lock table! No more locks!
                if len(self._lock_table[l[0]][1]) == 0:
                    self._lock_table.pop(l[0], None)
                else:
                    #if the next item in queue is "X", pop it and make that the lock in the list
                    if self._lock_table[l[0]][1][0][1] == "X":
                        self._lock_table[l[0]][0] = [self._lock_table[l[0]][1].pop(0)]
                    else: 
                        # otherwise, add as many "S" locks from the queue as you can!
                        while len(self._lock_table[l[0]][1]) != 0:
                            if self._lock_table[l[0]][1][0][1] == "S":
                                self._lock_table[l[0]][0].append(self._lock_table[l[0]][1].pop(0))
                            else:
                                break
                        self._lock_table[l[0]][0].remove((self._xid, l[1]))
            else:
                # I am not the only lock, so I will just quietly bow out
                self._lock_table[l[0]][0].remove((self._xid, l[1]))
                # If now there is only one lock left and the queue has an upgrade, make the change
                if len(self._lock_table[l[0]][0]) == 1 and len(self._lock_table[l[0]][1]) != 0:
                    for i in range(len(self._lock_table[l[0]][1])):
                        # Make sure the xids match
                        if self._lock_table[l[0]][0][0][0] == self._lock_table[l[0]][1][i][0]:
                            self._lock_table[l[0]][0] = [self._lock_table[l[0]][1].pop(i)]
                            break

        self._acquired_locks = []

        #Clean up the queue too!
        if self._desired_lock != None:
            key = self._desired_lock[0]
            lock_type = self._desired_lock[1]
            if (self._xid, lock_type) in self._lock_table[key][1]:
                self._lock_table[key][1].remove((self._xid, lock_type))                
            self._desired_lock = None

    def commit(self):
        """
        Commits the transaction.

        Note: This method is already implemented for you, and you only need to
        implement the subroutine release_locks().

        @param self: the transaction handler.

        @return: returns 'Transaction Completed'
        """
        self.release_and_grant_locks()
        return 'Transaction Completed'

    def abort(self, mode):
        """
        Aborts the transaction.

        Note: This method is already implemented for you, and you only need to
        implement the subroutine release_locks().

        @param self: the transaction handler.
        @param mode: mode can either be USER or DEADLOCK. If mode == USER, then
        it means that the abort is issued by the transaction itself (user
        abort). If mode == DEADLOCK, then it means that the transaction is
        aborted by the coordinator due to deadlock (deadlock abort).

        @return: if mode == USER, returns 'User Abort'. If mode == DEADLOCK,
        returns 'Deadlock Abort'.
        """
        while (len(self._undo_log) > 0):
            k,v = self._undo_log.pop()
            self._store.put(k, v)
        self.release_and_grant_locks()
        if (mode == USER):
            return 'User Abort'
        else:
            return 'Deadlock Abort'

    def check_lock(self):
        """
        If perform_get() or perform_put() returns None, then the transaction is
        waiting to acquire a lock. This method is called periodically to check
        if the lock has been granted due to commit or abort of other
        transactions. If so, then this method returns the string that would 
        have been returned by perform_get() or perform_put() if the method had
        not been blocked. Otherwise, this method returns None.

        As an example, suppose Joe is trying to perform 'GET a'. If Nisha has an
        exclusive lock on key 'a', then Joe's transaction is blocked, and
        perform_get() returns None. Joe's server handler starts calling
        check_lock(), which keeps returning None. While this is happening, Joe
        waits patiently for the server to return a response. Eventually, Nisha
        decides to commit his transaction, releasing his exclusive lock on 'a'.
        Now, when Joe's server handler calls check_lock(), the transaction
        checks to make sure that the lock has been acquired and returns the
        value of 'a'. The server handler then sends the value back to Joe.

        Hint: self._desired_lock contains the lock that the transaction is
        waiting to acquire.
        Hint: remember to update the self._acquired_locks list if the lock has
        been granted.
        Hint: if the transaction has been granted an exclusive lock due to lock
        upgrade, remember to clean up the self._acquired_locks list.
        Hint: remember to update self._undo_log so that we can undo all the
        changes if the transaction later gets aborted.

        @param self: the transaction handler.

        @return: if the lock has been granted, then returns whatever would be
        returned by perform_get() and perform_put() when the transaction
        successfully acquired the lock. If the lock has not been granted,
        returns None.
        """

        key = self._desired_lock[0]
        lock_type = self._desired_lock[1]

        for lock in self._lock_table[key][0]:
            # I HAVE THE LOCK!!! WHOOO!!!
            if lock[0] == self._xid and lock_type == lock[1]:
                if lock_type == "S":
                    self._acquired_locks.append((key, "S"))
                    value = self._store.get(key)
                    if value is None:
                        return 'No such key'
                    else:
                        return value
                elif lock_type == "X":
                    # There's been an upgrade
                    if self._acquired_locks.count((key, "S")) > 0:
                        self._acquired_locks.remove((key, "S"))
                    self._undo_log.append((key, self._store.get(key)))
                    self._store.put(key, self._desired_lock[2])
                    self._acquired_locks.append((key, "X"))
                    return 'Success'

        return

"""
Part II: Implement deadlock detection method for the transaction coordinator

The transaction coordinator has access to the following object:

self._lock_table: see description from Part I
"""

class TransactionCoordinator:

    def __init__(self, lock_table):
        self._lock_table = lock_table

    def detect_deadlocks(self):
        """
        Constructs a waits-for graph from the lock table, and runs a cycle
        detection algorithm to determine if a transaction needs to be aborted.
        You may choose which one transaction you plan to abort, as long as your
        choice is deterministic. For example, if transactions 1 and 2 form a
        cycle, you cannot return transaction 1 sometimes and transaction 2 the
        other times.

        This method is called periodically to check if any operations of any
        two transactions conflict. If this is true, the transactions are in
        deadlock - neither can proceed. If there are multiple cycles of
        deadlocked transactions, then this method will be called multiple
        times, with each call breaking one of the cycles, until it returns None
        to indicate that there are no more cycles. Afterward, the surviving
        transactions will continue to run as normal.

        Note: in this method, you only need to find and return the xid of a
        transaction that needs to be aborted. You do not have to perform the
        actual abort.

        @param self: the transaction coordinator.

        @return: If there are no cycles in the waits-for graph, returns None.
        Otherwise, returns the xid of a transaction in a cycle.
        
        waiting {a, [b]} where b waits on a 
        """
        waiting = {}
        for key in self._lock_table.keys():
            lock_info = self._lock_table[key]
            for lock in lock_info[0]:
                if not waiting.has_key(lock[0]):
                    waiting[lock[0]] = []
                for waiting_lock in lock_info[1]:
                    if waiting_lock[0] != lock[0]:
                        waiting[lock[0]].append(waiting_lock[0])

        def find_cycle(waiting, start, already_seen):
            stack = [start]
            while stack:
                vertex = stack.pop()
                if vertex in already_seen:
                    return vertex
                else:
                    already_seen.append(vertex)
                    for item in waiting[vertex]:
                        if waiting.has_key(item):
                            stack.append(item)
            return

        if waiting != {}:
            return find_cycle(waiting, waiting.keys()[0], [])
        return
