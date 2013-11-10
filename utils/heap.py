import math


class Heap(object):
    '''
    This class will hold the heap to allow us to query items by some criteria
    which will most likely involve queries to the items list like: "Give me
    the item which has the lowest attribute P" or "Give me the item which has
    the lowest total sum of its attributes P1 and P2".
    '''
    def __init__(self, items=[], key=lambda x: x, reverse=False):
        '''
        Initialize a heap of slaves.

        :type items: list
        :param items: A list of items to be heapified.

        :type key: function or None
        :param key: A function that defines what to heapify the items by.

        :type reverse: Boolean
        :param reverse: Determines whether to construct a min heap (for
        reverse = False) or to contruct a max heap (for reverse = True)
        '''

        self.reverse = reverse
        if key is None:
            raise Exception("A key must be provided to create a Heap.")
        else:
            self.key = key
        if items is []:
            self.items = []
            return

        self.items = items
        for index in range(math.floor(len(self.items) / 2), -1, -1):
            self.__bubble_down(index)

    def parent(self, index):
        '''
        Get the parent of the element at index ``index``.
        '''
        if index == 0:
            return None
        else:
            return math.ceil(index / 2) - 1

    def insert(self, item):
        '''
        Insert the item into the heap that we already have.
        '''
        self.items.append(item)
        self.__bubble_up(len(self.items) - 1)

    def pop(self):
        '''
        Remove the item at the root node and return it.
        '''
        return_item = self.items[0]
        self.items = self.items[1:]
        if len(self.items) > 0:
            self.items.insert(0, self.items.pop())
            self.__bubble_down(0)
        return return_item

    def __bubble_up(self, index):
        '''
        Bubble up the element at index ``index``.
        '''
        key = self.key
        items = self.items

        parent_index = self.parent(index)
        if parent_index is None:
            return
        if self.reverse:  # If its a max-heap
            while key(items[index]) > key(items[parent_index]):
                # Swap this with parent
                temp = items[index]
                items[index] = items[parent_index]
                items[parent_index] = temp
                index = parent_index
                parent_index = self.parent(index)
                if parent_index is None:
                    break
        else:  # If its a min heap
            while key(items[index]) < key(items[parent_index]):
                # Swap this with parent
                temp = items[index]
                items[index] = items[parent_index]
                items[parent_index] = temp
                index = parent_index
                parent_index = self.parent(index)
                if parent_index is None:
                    break

    def __bubble_down(self, index):
        '''
        Bubble down the element at index ``index``.
        '''
        key = self.key
        items = self.items

        lchild_index = (2 * index) + 1
        rchild_index = (2 * index) + 2
        if lchild_index >= len(items):
            lchild_index = None
        if rchild_index >= len(items):
            rchild_index = None
        if lchild_index is None and rchild_index is None:
            return

        if self.reverse:
            while True:
                if rchild_index is None and lchild_index is None:
                    break
                if rchild_index is None:
                    larger_child_index = lchild_index
                elif lchild_index is None:
                    larger_child_index = rchild_index
                else:
                    if key(items[lchild_index]) > key(items[rchild_index]):
                        larger_child_index = lchild_index
                    else:
                        larger_child_index = rchild_index

                if key(items[index]) >= key(items[larger_child_index]):
                    break

                temp = items[index]
                items[index] = items[larger_child_index]
                items[larger_child_index] = temp
                index = larger_child_index
                lchild_index = (2 * index) + 1
                rchild_index = (2 * index) + 2
                if lchild_index >= len(items):
                    lchild_index = None
                if rchild_index >= len(items):
                    rchild_index = None
        else:
            while True:
                if rchild_index is None and lchild_index is None:
                    break
                if rchild_index is None:
                    smaller_child_index = lchild_index
                elif lchild_index is None:
                    smaller_child_index = rchild_index
                else:
                    if key(items[lchild_index]) < key(items[rchild_index]):
                        smaller_child_index = lchild_index
                    else:
                        smaller_child_index = rchild_index

                if key(items[index]) <= key(items[smaller_child_index]):
                    break

                temp = items[index]
                items[index] = items[smaller_child_index]
                items[smaller_child_index] = temp
                index = smaller_child_index
                lchild_index = (2 * index) + 1
                rchild_index = (2 * index) + 2
                if lchild_index >= len(items):
                    lchild_index = None
                if rchild_index >= len(items):
                    rchild_index = None
