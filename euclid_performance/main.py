from RessourceUser import RessourceUser

__author__ = 'cansik'

user = RessourceUser()

user.use_cpu(8)
user.use_memory(0)
user.use_io(0, 0)

user.start(120)
print("finished!")
