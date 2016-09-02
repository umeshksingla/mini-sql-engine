import os
import itertools

METADATA_FILE = "metadata.txt"

class Table(object):
		"""
			the Table Class
		"""
		def __init__(self, name, columns, rows):
			"""
				initializes the table with
				name : table name
				columns : list of names of column of this table
				rows : list of dicts of type {corresponding column name : value, ..}
			"""
			self.name = name
			self.columns = map(lambda x: x.strip(), columns)
			self.rows = rows

		def __make_row(self, row):
			"""
				returns a row in key value form
			"""
			values = row.split(",")
			values = map(lambda x: int(x), values)

			if len(values) != len(self.columns):
				raise Exception("One of the rows is not of appropriate length")
			
			return dict(zip(self.columns, values))

		def __print_row(self, row):
			
			for each in self.columns:
				print str(row[each]) + "\t",
			print

		def load_contents(self):
			"""
				loads the content from the file
			"""			
			with open(self.name + ".csv") as f:
				rows = f.read().splitlines()

			rows = map(lambda x: x.replace("\"",""), rows)
			rows = map(lambda x: x.strip(), rows)

			for i in rows:
				row = self.__make_row(i)
				self.rows.append(row)


		def print_contents(self):
			"""
				print the contents of entire table
			"""
			print self.name

			for each in self.columns:
				print each + "\t",
			print

			for each in self.rows:
				self.__print_row(each)


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
		useless = ['<begin_table>', '<end_table>']

		# example - 
		# if metalines = ['begin','6','8','end','begin','4','begin','1','4','end']
		# and useless = ['begin', 'end'], then
		# groups = [['begin'], ['6', '8'], ['end', 'begin'], ['4'], ['begin'], ['1', '4'], ['end']]
		# after the following loop

		for key, value in itertools.groupby(metalines, lambda x: x in useless):
			groups.append(list(value))
		list_of_tables_and_columns = [x for x in groups if x[0] not in useless] 

		for each in list_of_tables_and_columns:
			self.create_empty_table(each[0], each[1:])

		for table in self.tables:
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
		print "Database: " + self.name
		for table in self.tables:
			table.print_contents()
			print











