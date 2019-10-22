from collections import UserList


class CogniteJobError(Exception, UserList):
    def __init__(self, ex_list=[]):
        UserList.__init__(self, ex_list)
        Exception.__init__(self)

    def __str__(self):
        return f"{len(self)} Exceptions occurred:\n" + "".join([str(ex) for ex in self])
