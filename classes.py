import os
import itertools
from packages.exceptions import SqlException
from packages import debug

METADATA_FILE = "metadata.txt"

def check_overlapping_fields(columns, key):
	fl = False
	old_key = key
	for col in columns:
		if col.split(".")[1] == old_key:
			if fl:
				raise SqlException("EC: Joined tables have one of the fields overlapping. So, you need to specify in <table_name>.<column_name> format")
			key = col
			fl = True
	return key


class Table(object):
		"""
			the Table Class
		"""
		def __init__(self, name, columns, rows=[]):
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
				returns a row in dict form
			"""
			values = row.split(",")
			values = map(lambda x: int(x), values)

			if len(values) != len(self.columns):
				raise SqlException("One of the rows is not of appropriate length in table " + self.name)
			
			return dict(zip(self.columns, values))

		def add_row(self, row):
			"""
				adds a row to the table when in ready form
			"""
			self.rows.append(dict(zip(self.columns, row)))

		def __print_row(self, row):
			
			for each in self.columns:
				print str(row[each]) + "\t",
			print

		def prefix_table_name_to_columns(self):
			"""
				returns a list of columns with table name prefixed e.g. Book.name, Book.author
			"""
			return map(lambda x: self.name + "." + x, self.columns)

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
			for each in self.columns:
				print each + "\t",
			print

			for each in self.rows:
				self.__print_row(each)

		def get_rows(self):
			"""
				returns the list of rows with values in int form
			"""
			# [[637, 328],[78, 432],[38, 849],...]
			return [[row[each] for each in self.columns] for row in self.rows]

		def delete_rows_by_int(self, key, value, condition):
			"""
				delete rows where column 'key' has value 'value' and the condition doesn't hold
			"""
			try:
				if condition.strip() == "!=" or condition.strip() == "<>":
					self.rows = filter(lambda x: x[key] != value, self.rows)
				elif condition.strip() == "=" :
					self.rows = filter(lambda x: x[key] == value, self.rows)
				elif condition.strip() == ">=" :
					self.rows = filter(lambda x: x[key] >= value, self.rows)
				elif condition.strip() == "<=" :
					self.rows = filter(lambda x: x[key] <= value, self.rows)
				elif condition.strip() == ">" :
					self.rows = filter(lambda x: x[key] > value, self.rows)
				elif condition.strip() == "<" :
					self.rows = filter(lambda x: x[key] < value, self.rows)
			except:
				raise SqlException("Invalid column name")

		def delete_rows_by_col(self, key, value, condition):
			"""
				delete rows where column 'key' has value 'value' and the condition doesn't hold
			"""
			try:
				if condition.strip() == "!=" or condition.strip() == "<>":
					self.rows = filter(lambda x: x[key] != x[value], self.rows)
				elif condition.strip() == "=" :
					self.rows = filter(lambda x: x[key] == x[value], self.rows)
				elif condition.strip() == ">=" :
					self.rows = filter(lambda x: x[key] >= x[value], self.rows)
				elif condition.strip() == "<=" :
					self.rows = filter(lambda x: x[key] <= x[value], self.rows)
				elif condition.strip() == ">" :
					self.rows = filter(lambda x: x[key] > x[value], self.rows)
				elif condition.strip() == "<" :
					self.rows = filter(lambda x: x[key] < x[value], self.rows)
			except:
				raise SqlException("Invalid column name")

		def delete_rows_by_both_ints(self, key1, value1, condition1, key2, value2, condition2):
			"""
				delete rows where column 'key' has value 'value' and the condition doesn't hold
			"""
			try:
				if condition1.strip() == "!=" or condition1.strip() == "<>":
					if condition2.strip() == "!=" or condition2.strip() == "<>":
						self.rows = filter(lambda x: x[key1] != value1 or x[key2] != value2, self.rows)
					elif condition2.strip() == "=" :
						self.rows = filter(lambda x: x[key1] != value1 or x[key2] == value2, self.rows)
					elif condition2.strip() == ">=" :
						self.rows = filter(lambda x: x[key1] != value1 or x[key2] >= value2, self.rows)
					elif condition2.strip() == "<=" :
						self.rows = filter(lambda x: x[key1] != value1 or x[key2] <= value2, self.rows)
					elif condition2.strip() == ">" :
						self.rows = filter(lambda x: x[key1] != value1 or x[key2] > value2, self.rows)
					elif condition2.strip() == "<" :
						self.rows = filter(lambda x: x[key1] != value1 or x[key2] < value2, self.rows)
				elif condition1.strip() == "=":
					if condition2.strip() == "!=" or condition2.strip() == "<>":
						self.rows = filter(lambda x: x[key1] == value1 or x[key2] != value2, self.rows)
					elif condition2.strip() == "=" :
						self.rows = filter(lambda x: x[key1] == value1 or x[key2] == value2, self.rows)
					elif condition2.strip() == ">=" :
						self.rows = filter(lambda x: x[key1] == value1 or x[key2] >= value2, self.rows)
					elif condition2.strip() == "<=" :
						self.rows = filter(lambda x: x[key1] == value1 or x[key2] <= value2, self.rows)
					elif condition2.strip() == ">" :
						self.rows = filter(lambda x: x[key1] == value1 or x[key2] > value2, self.rows)
					elif condition2.strip() == "<" :
						self.rows = filter(lambda x: x[key1] == value1 or x[key2] < value2, self.rows)
				elif condition1.strip() == ">=":
					if condition2.strip() == "!=" or condition2.strip() == "<>":
						self.rows = filter(lambda x: x[key1] >= value1 or x[key2] != value2, self.rows)
					elif condition2.strip() == "=" :
						self.rows = filter(lambda x: x[key1] >= value1 or x[key2] == value2, self.rows)
					elif condition2.strip() == ">=" :
						self.rows = filter(lambda x: x[key1] >= value1 or x[key2] >= value2, self.rows)
					elif condition2.strip() == "<=" :
						self.rows = filter(lambda x: x[key1] >= value1 or x[key2] <= value2, self.rows)
					elif condition2.strip() == ">" :
						self.rows = filter(lambda x: x[key1] >= value1 or x[key2] > value2, self.rows)
					elif condition2.strip() == "<" :
						self.rows = filter(lambda x: x[key1] >= value1 or x[key2] < value2, self.rows)
				elif condition1.strip() == "<=":
					if condition2.strip() == "!=" or condition2.strip() == "<>":
						self.rows = filter(lambda x: x[key1] <= value1 or x[key2] != value2, self.rows)
					elif condition2.strip() == "=" :
						self.rows = filter(lambda x: x[key1] <= value1 or x[key2] == value2, self.rows)
					elif condition2.strip() == ">=" :
						self.rows = filter(lambda x: x[key1] <= value1 or x[key2] >= value2, self.rows)
					elif condition2.strip() == "<=" :
						self.rows = filter(lambda x: x[key1] <= value1 or x[key2] <= value2, self.rows)
					elif condition2.strip() == ">" :
						self.rows = filter(lambda x: x[key1] <= value1 or x[key2] > value2, self.rows)
					elif condition2.strip() == "<" :
						self.rows = filter(lambda x: x[key1] <= value1 or x[key2] < value2, self.rows)
				elif condition1.strip() == ">":
					if condition2.strip() == "!=" or condition2.strip() == "<>":
						self.rows = filter(lambda x: x[key1] > value1 or x[key2] != value2, self.rows)
					elif condition2.strip() == "=" :
						self.rows = filter(lambda x: x[key1] > value1 or x[key2] == value2, self.rows)
					elif condition2.strip() == ">=" :
						self.rows = filter(lambda x: x[key1] > value1 or x[key2] >= value2, self.rows)
					elif condition2.strip() == "<=" :
						self.rows = filter(lambda x: x[key1] > value1 or x[key2] <= value2, self.rows)
					elif condition2.strip() == ">" :
						self.rows = filter(lambda x: x[key1] > value1 or x[key2] > value2, self.rows)
					elif condition2.strip() == "<" :
						self.rows = filter(lambda x: x[key1] > value1 or x[key2] < value2, self.rows)
				elif condition1.strip() == "<":
					if condition2.strip() == "!=" or condition2.strip() == "<>":
						self.rows = filter(lambda x: x[key1] < value1 or x[key2] != value2, self.rows)
					elif condition2.strip() == "=" :
						self.rows = filter(lambda x: x[key1] < value1 or x[key2] == value2, self.rows)
					elif condition2.strip() == ">=" :
						self.rows = filter(lambda x: x[key1] < value1 or x[key2] >= value2, self.rows)
					elif condition2.strip() == "<=" :
						self.rows = filter(lambda x: x[key1] < value1 or x[key2] <= value2, self.rows)
					elif condition2.strip() == ">" :
						self.rows = filter(lambda x: x[key1] < value1 or x[key2] > value2, self.rows)
					elif condition2.strip() == "<" :
						self.rows = filter(lambda x: x[key1] < value1 or x[key2] < value2, self.rows)
			except:
				raise SqlException("Invalid column name")

		def delete_rows_by_both_cols(self, key1, value1, condition1, key2, value2, condition2):
			"""
				delete rows where column 'key' has value 'value' and the condition doesn't hold
			"""
			try:
				if condition1.strip() == "!=" or condition1.strip() == "<>":
					if condition2.strip() == "!=" or condition2.strip() == "<>":
						self.rows = filter(lambda x: x[key1] != x[value1] or x[key2] != x[value2], self.rows)
					elif condition2.strip() == "=" :
						self.rows = filter(lambda x: x[key1] != x[value1] or x[key2] == x[value2], self.rows)
					elif condition2.strip() == ">=" :
						self.rows = filter(lambda x: x[key1] != x[value1] or x[key2] >= x[value2], self.rows)
					elif condition2.strip() == "<=" :
						self.rows = filter(lambda x: x[key1] != x[value1] or x[key2] <= x[value2], self.rows)
					elif condition2.strip() == ">" :
						self.rows = filter(lambda x: x[key1] != x[value1] or x[key2] > x[value2], self.rows)
					elif condition2.strip() == "<" :
						self.rows = filter(lambda x: x[key1] != x[value1] or x[key2] < x[value2], self.rows)
				elif condition1.strip() == "=":
					if condition2.strip() == "!=" or condition2.strip() == "<>":
						self.rows = filter(lambda x: x[key1] == x[value1] or x[key2] != x[value2], self.rows)
					elif condition2.strip() == "=" :
						self.rows = filter(lambda x: x[key1] == x[value1] or x[key2] == x[value2], self.rows)
					elif condition2.strip() == ">=" :
						self.rows = filter(lambda x: x[key1] == x[value1] or x[key2] >= x[value2], self.rows)
					elif condition2.strip() == "<=" :
						self.rows = filter(lambda x: x[key1] == x[value1] or x[key2] <= x[value2], self.rows)
					elif condition2.strip() == ">" :
						self.rows = filter(lambda x: x[key1] == x[value1] or x[key2] > x[value2], self.rows)
					elif condition2.strip() == "<" :
						self.rows = filter(lambda x: x[key1] == x[value1] or x[key2] < x[value2], self.rows)
				elif condition1.strip() == ">=":
					if condition2.strip() == "!=" or condition2.strip() == "<>":
						self.rows = filter(lambda x: x[key1] >= x[value1] or x[key2] != x[value2], self.rows)
					elif condition2.strip() == "=" :
						self.rows = filter(lambda x: x[key1] >= x[value1] or x[key2] == x[value2], self.rows)
					elif condition2.strip() == ">=" :
						self.rows = filter(lambda x: x[key1] >= x[value1] or x[key2] >= x[value2], self.rows)
					elif condition2.strip() == "<=" :
						self.rows = filter(lambda x: x[key1] >= x[value1] or x[key2] <= x[value2], self.rows)
					elif condition2.strip() == ">" :
						self.rows = filter(lambda x: x[key1] >= x[value1] or x[key2] > x[value2], self.rows)
					elif condition2.strip() == "<" :
						self.rows = filter(lambda x: x[key1] >= x[value1] or x[key2] < x[value2], self.rows)
				elif condition1.strip() == "<=":
					if condition2.strip() == "!=" or condition2.strip() == "<>":
						self.rows = filter(lambda x: x[key1] <= x[value1] or x[key2] != x[value2], self.rows)
					elif condition2.strip() == "=" :
						self.rows = filter(lambda x: x[key1] <= x[value1] or x[key2] == x[value2], self.rows)
					elif condition2.strip() == ">=" :
						self.rows = filter(lambda x: x[key1] <= x[value1] or x[key2] >= x[value2], self.rows)
					elif condition2.strip() == "<=" :
						self.rows = filter(lambda x: x[key1] <= x[value1] or x[key2] <= x[value2], self.rows)
					elif condition2.strip() == ">" :
						self.rows = filter(lambda x: x[key1] <= x[value1] or x[key2] > x[value2], self.rows)
					elif condition2.strip() == "<" :
						self.rows = filter(lambda x: x[key1] <= x[value1] or x[key2] < x[value2], self.rows)
				elif condition1.strip() == ">":
					if condition2.strip() == "!=" or condition2.strip() == "<>":
						self.rows = filter(lambda x: x[key1] > x[value1] or x[key2] != x[value2], self.rows)
					elif condition2.strip() == "=" :
						self.rows = filter(lambda x: x[key1] > x[value1] or x[key2] == x[value2], self.rows)
					elif condition2.strip() == ">=" :
						self.rows = filter(lambda x: x[key1] > x[value1] or x[key2] >= x[value2], self.rows)
					elif condition2.strip() == "<=" :
						self.rows = filter(lambda x: x[key1] > x[value1] or x[key2] <= x[value2], self.rows)
					elif condition2.strip() == ">" :
						self.rows = filter(lambda x: x[key1] > x[value1] or x[key2] > x[value2], self.rows)
					elif condition2.strip() == "<" :
						self.rows = filter(lambda x: x[key1] > x[value1] or x[key2] < x[value2], self.rows)
				elif condition1.strip() == "<":
					if condition2.strip() == "!=" or condition2.strip() == "<>":
						self.rows = filter(lambda x: x[key1] < x[value1] or x[key2] != x[value2], self.rows)
					elif condition2.strip() == "=" :
						self.rows = filter(lambda x: x[key1] < x[value1] or x[key2] == x[value2], self.rows)
					elif condition2.strip() == ">=" :
						self.rows = filter(lambda x: x[key1] < x[value1] or x[key2] >= x[value2], self.rows)
					elif condition2.strip() == "<=" :
						self.rows = filter(lambda x: x[key1] < x[value1] or x[key2] <= x[value2], self.rows)
					elif condition2.strip() == ">" :
						self.rows = filter(lambda x: x[key1] < x[value1] or x[key2] > x[value2], self.rows)
					elif condition2.strip() == "<" :
						self.rows = filter(lambda x: x[key1] < x[value1] or x[key2] < x[value2], self.rows)
			except:
				raise SqlException("Invalid column name")

		def get_col(self, col_name):
			"""
				returns the column col of the table
			"""
			if "(" in col_name:		# some aggregate function exists
				aggregate = col_name.split("(")[0].upper()
				key = col_name.split("(")[1].split(")")[0]
			else:
				key = col_name
				aggregate = None

			# for queries where columns are specified without <table_name>.<column_name> format
			if "." not in key:
				key = check_overlapping_fields(self.columns, key)

			if key not in self.columns:
				raise SqlException("No such column: " + key)

			column = [row[key] for row in self.rows]
			if len(column) is 0:
				return [None]
			if aggregate is not None:
				if aggregate == "MAX":
					return [max(column)]
				elif aggregate == "MIN":
					return [min(column)]
				elif aggregate == "AVG":
					return [float(sum(column))/len(column)]
				elif aggregate == "SUM":
					return [sum(column)]
				elif aggregate == "DISTINCT":
					return list(set(column))
				else:
					raise SqlException("Unknown aggregate function")
			return column

class Database(object):
	"""
		the Database class
	"""
	def __init__(self, name, tables):
		"""
			initializes the database with
			name : database name
			tables : list of tables of Table type
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

	def get_table(self, name):
		"""
			returns the table with name name from this database
		"""
		t = filter(lambda x: x.name == name, self.tables)
		if len(t) == 0:
			raise SqlException("No such table")
		return t[0]
