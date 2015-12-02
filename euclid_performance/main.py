from RessourceUser import RessourceUser

__author__ = 'cansik'

user = RessourceUser()

user.use_cpu(4)
user.use_memory(10000)
user.use_io(1000, 2)

user.start(20)
print("finished!")
