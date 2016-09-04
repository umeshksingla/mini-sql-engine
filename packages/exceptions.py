class SqlException(Exception):
	"""
		SQL Custom Exception class
	"""
	def __init__(self, arg):
		self.message = arg