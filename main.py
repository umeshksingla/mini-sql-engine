import classes
import sqlparse
import itertools

db = classes.Database(name="Himalayan Database", tables=[])

def debug(*args):
	for i in args:
		print i

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
		raise Exception("Invalid Syntax")
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
				comparison = where.tokens[2]					# comparison = "A=8";
				comparison.tokens = [x for x in comparison.tokens if str(x) != " "]		# No more white spaces			
				key = str(comparison.tokens[0])					# key = "A"
				try:
					value = int(str(comparison.tokens[2]))		# whether it is an int value on RHS of comparison or some column
					temp_table.delete_rows_by_int(key, value, str(comparison.tokens[1]))
				except:
					value = str(comparison.tokens[2])
					temp_table.delete_rows_by_col(key, value, str(comparison.tokens[1]))

				# if AND
				try:
					if str(where.tokens[4]) == "AND":
						comparison = where.tokens[6]					# comparison = "A=8";
						comparison.tokens = [x for x in comparison.tokens if str(x) != " "]		# No more white spaces			
						key = str(comparison.tokens[0])					# key = "A"
						try:
							value = int(str(comparison.tokens[2]))		# whether it is an int value on RHS of comparison or some column
							temp_table.delete_rows_by_int(key, value, str(comparison.tokens[1]))
						except:
							value = str(comparison.tokens[2])
							temp_table.delete_rows_by_col(key, value, str(comparison.tokens[1]))
				except:
					pass

				# if OR
				"""try:
					if str(where.tokens[4]) == "OR":
						comparison = where.tokens[6]					# comparison = "A=8";
						comparison.tokens = [x for x in comparison.tokens if str(x) != " "]		# No more white spaces			
						key = str(comparison.tokens[0])					# key = "A"
						try:
							value = int(str(comparison.tokens[2]))		# whether it is an int value on RHS of comparison or some column
							temp_table.delete_rows_by_int(key, value, str(comparison.tokens[1]))
						except:
							value = str(comparison.tokens[2])
							temp_table.delete_rows_by_col(key, value, str(comparison.tokens[1]))
				except:
					pass"""

		if '*' in column_list:
			temp_table.print_contents()
		else:
			temp = []
			for col in column_list:
				temp.append(temp_table.get_col(col))

			l = len(temp[0])
			for row in temp:
				if len(row) != l:
					raise Exception("Incompatible column lengths: Generally, this happens when there's an aggregated query wihtout GROUP BY having non-aggregated column")

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

		for command in sqlparse.split(query):
			stmt = sqlparse.parse(sqlparse.format(command, keyword_case='upper'))
			stmt = stmt[0].tokens
			qtype = str(stmt[0])
			if len(stmt) < 7:
				raise Exception("Invalid Syntax")
			if qtype == "SELECT":
				select_query(stmt)
			elif qtype == "QUIT":
				cont = False
			else:
				raise Exception(qtype + " not supported.")



if __name__ == '__main__':
	main()