import hashlib

def hasher(key):
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**16)

print(hasher("localhost"+str(65000)))
print(hasher("localhost"+str(65001)))
print(hasher("localhost"+str(65002)))
print(hasher("localhost"+str(65003)))
print(hasher("localhost"+str(65004)))