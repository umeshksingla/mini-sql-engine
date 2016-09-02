import os
import itertools

METADATA_FILE = "metadata.txt"

class Database(object):
	"""
		the Database class
	"""
	def __init__(self, name, tables):
		"""
			initialize the database with name and given (or zero) tables
		"""
		self.name = name
		self.tables = tables

	def load_contents(self):
		"""
			load the metadata, create and fill tables
		"""

		with open(METADATA_FILE) as metafile:
			metalines = metafile.read().splitlines()

		metalines = map(lambda x: x.strip(), metalines)

		list_of_tables_and_columns = []
		groups = []
		useless = ['<begin_table>', 'end_table']

		# example - 
		# if metalines = ['begin','6','8','end','begin','4','begin','1','4','end']
		# and useless = ['begin', 'end'], then
		# groups = [['begin'], ['6', '8'], ['end', 'begin'], ['4'], ['begin'], ['1', '4'], ['end']]
		# after the following loop

		for key, value in itertools.groupby(metalines, useless):
			groups.append(list(value))

		list_of_tables_and_columns = [x for x in groups if x[0] not in useless] 

		for each in list_of_tables_and_columns:
			self.create_empty_table(each[0], each[1:])

		for table in self.tables():
			table.load_contents()

	def create_empty_table(self, tablename, columns):
		"""
			initializes a table given in metadata with name, columns and rows being empty
		"""

		table = Table(tablename, columns, rows = [])
		self.tables.append(table)

	def print_contents(self):
		"""
			prints the entire database
		"""
		print self.name
		for table in self.tables:
			table.print_contents()
		print







