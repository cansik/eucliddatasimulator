from RessourceUser import RessourceUser

user = RessourceUser()

user.use_cpu(0)
user.use_memory(12000)
user.use_io(0, 0)

user.start(120)
print("finished!")
