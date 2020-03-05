import bcrypt

passwd = b'8762985V'



salt = bcrypt.gensalt()
for i in range(0,30):
    hashed = bcrypt.hashpw(passwd, salt)
    print(str(i) + "    " + str(hashed))

print(salt)
print(hashed)

hashed = bcrypt.hashpw(passwd, salt)

print(hashed)