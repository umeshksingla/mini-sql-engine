from packages.exceptions import SqlException
from packages import debug
import sqlparse
import classes
import itertools

db = classes.Database(name="Himalayan Database", tables=[])

def check_overlapping_fields(columns, key):
	fl = False
	old_key = key
	for col in columns:
		if col.split(".")[1] == old_key:
			if fl:
				raise SqlException("EM: Joined tables have one of the fields overlapping. So, you need to specify in <table_name>.<column_name> format")
			key = col
			fl = True
	return key

def where_helper(temp_table, all_columns, where):
	"""
		Helper function for where; returns only first comparison results,
		thereby helping in only where and AND conditions
	"""
	comparison = where.tokens[2]						# comparison = "A=8";
	comparison.tokens = [x for x in comparison.tokens if not x.is_whitespace()]		# No more white spaces			
	key = str(comparison.tokens[0])						# key = "A"
	
	if '.' not in key:
		key = check_overlapping_fields(all_columns, key)
	try:
		value = int(str(comparison.tokens[2]))			# whether it is an integer value on RHS of comparison or some column
		temp_table.delete_rows_by_int(key, value, str(comparison.tokens[1]))
	except:
		value = str(comparison.tokens[2])
		if '.' not in value:
			value = check_overlapping_fields(all_columns, value)
		temp_table.delete_rows_by_col(key, value, str(comparison.tokens[1]))
	return temp_table


def where_select_query(temp_table, all_columns, where):
	"""
		filter where condition on the basis of AND, OR or none
	"""
	if len(where.tokens) >= 7:								# AND or OR are present
		if str(where.tokens[4]) == "AND":
			temp_table = where_helper(temp_table, all_columns, where)
			
			comparison = where.tokens[6]					# comparison = "A=8";
			comparison.tokens = [x for x in comparison.tokens if not x.is_whitespace()]		# No more white spaces			
			key = str(comparison.tokens[0])					# key = "A"
			
			if '.' not in key:
				key = check_overlapping_fields(all_columns, key)
			try:
				value = int(str(comparison.tokens[2]))		# whether it is an int value on RHS of comparison or some column
				temp_table.delete_rows_by_int(key, value, str(comparison.tokens[1]))
			except:
				value = str(comparison.tokens[2])
				if '.' not in value:
					value = check_overlapping_fields(all_columns, value)
				temp_table.delete_rows_by_col(key, value, str(comparison.tokens[1]))

		elif str(where.tokens[4]) == "OR":
			
			comparison1 = where.tokens[2]						# comparison = "A=8";
			comparison1.tokens = [x for x in comparison1.tokens if not x.is_whitespace()]		# No more white spaces			
			key1 = str(comparison1.tokens[0])						# key = "A"
			
			if '.' not in key1:
				key1 = check_overlapping_fields(all_columns, key1)
			try:
				value1 = int(str(comparison1.tokens[2]))
			except:
				value1 = str(comparison1.tokens[2])
			
			comparison2 = where.tokens[6]						# comparison = "A=8";
			comparison2.tokens = [x for x in comparison2.tokens if not x.is_whitespace()]		# No more white spaces			
			key2 = str(comparison2.tokens[0])						# key = "A"
	
			if '.' not in key2:
				key2 = check_overlapping_fields(all_columns, key2)
			try:
				value2 = int(str(comparison2.tokens[2]))
			except:
				value2 = str(comparison2.tokens[2])

			if type(value1) == int and type(value2) == int:
				temp_table.delete_rows_by_both_ints(key1, value1, str(comparison1.tokens[1]), key2, value2, str(comparison2.tokens[2]))
			elif type(value1) == str and type(value2) == str:
				temp_table.delete_rows_by_both_cols(key1, value1, str(comparison1.tokens[1]), key2, value2, str(comparison2.tokens[2]))
			else:
				raise SqlException("Only OR on joins with either comparisons with int or columns in both conditions supported.")
		else:
			raise SqlException("Invalid where condition")
	elif len(where.tokens) <= 5:													# Only where is present
		temp_table = where_helper(temp_table, all_columns, where)
	else:
		raise SqlException("Invalid where syntax")
	return temp_table


def select_query(stmt):
	"""
		returns results of select query
		We'll make a temporary table, store the result and then print it
	"""
	try:
		column_list = str(stmt[2]).split(",")
		column_list = [x.strip() for x in column_list]
		table_list = str(stmt[6]).split(",")
		table_list = [x.strip() for x in table_list]
	except:
		raise SqlException("Invalid Syntax")
	else:
		all_columns = map(lambda x: db.get_table(x).prefix_table_name_to_columns(), table_list)

		# upperbound columns of the new table
		all_columns = reduce(lambda x, y: x + y, all_columns)	# make all column lists in table_list as one

		# temporary table with all columns
		temp_table = classes.Table(
			name="temp", 
			columns=all_columns, 
			rows=[]
		)
		
		all_tables_rows = map(lambda x: db.get_table(x).get_rows(), table_list)	

		# cross product of all rows between tables
		rows = list(itertools.product(*all_tables_rows))		# NOTE: product() takes the elements of a list, and not the list
																# So, here it will be all_tables_rows's elements											
		for each in rows:										# each is a tuple of rows i.e. lists, therefore, 
			each = reduce(lambda x, y: x + y, each)				# reduce simply concatenates all the rows to form one row
			temp_table.add_row(each)							# of temporary table	

		#temp_table.print_contents()

		if len(stmt) >= 9:										# 'where' is present
			where = stmt[8]										# where = "WHERE A=8"
			if str(where.tokens[0]) == "WHERE":
				temp_table = where_select_query(temp_table, all_columns, where)
			else:
				raise SqlException("Invalid Syntax")


		if '*' in column_list:
			temp_table.print_contents()
		else:
			temp = []
			for col in column_list:
				temp.append(temp_table.get_col(col))

			l = len(temp[0])
			for row in temp:
				if len(row) != l:
					raise SqlException("Incompatible column lengths: Generally, this happens when there's an aggregated query wihtout GROUP BY having non-aggregated column")

			print "Table: "
			for col in column_list:
				print col + "\t",
			print

			for row in zip(*temp):
				for i in row:
					print str(i) + "\t",
				print


def main():
	db.load_contents()
	db.print_contents()
	cont = True
	while cont:
		query = raw_input("hdsql>> ")
		try:
			for command in sqlparse.split(query):
				stmt = sqlparse.parse(sqlparse.format(command, keyword_case='upper'))
				stmt = stmt[0].tokens
				qtype = str(stmt[0])
				if len(stmt) < 7:
					raise SqlException("Invalid Syntax")
				if qtype == "SELECT":
					select_query(stmt)
				elif qtype == "QUIT":
					cont = False
				else:
					raise SqlException(qtype + " not supported.")
		except SqlException, e:
			print e.message


if __name__ == '__main__':
	main()