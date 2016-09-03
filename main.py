from . import classes
import sqlparse

db = classes.Database(name="Himalayan Database", tables=[])

def select_query(stmt):
	"""
		returns results of select query
		We'll make a temporary table, store the result and then print it
	"""
	try:
		column_list = str(stmt[2]).split(",")
		table_list = str(stmt[6]).split(",")
	except:
		raise Exception("Invalid Syntax")
	else:
		
		table_list = map(lambda x: db.get_table(x).prefix_table_name_to_columns(), table_list)
		
		# upperbound columns of the new table
		all_columns = reduce(lambda x, y: x + y, table_list)	# make all column lists in table_list as one
		
		# temporary table with all columns
		temp_table = classes.Table(
			name="temp", 
			columns=all_columns, 
			rows=[]
		)
		
		temp_table.print_contents()




def main():
	db.load_contents()
	continue = True
	while continue:
		query = raw_input("hdsql>>> ")

		for command in sqlparse.split(query)
			stmt = sqlparse.parse(sqlparse.format(command, keyword_case='upper'))
			stmt = stmt[0].tokens
			type = str(stmt[0])
			if len(stmt) < 7:
				raise Exception("Invalid Syntax")
			if type == "SELECT":
				select_query(stmt)
			elif type == "QUIT":
				exit()
			else:
				raise Exception(type + " not supported.")



if __name__ == '__main__':
	main()