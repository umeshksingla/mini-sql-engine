import itertools

lines = ['begin','6','8','end','begin','4','begin','1','4','end']
exc = ['begin','end']
lis = []
groups = []
for k, v in itertools.groupby(lines, lambda x : x in exc):
	groups.append(list(v))

list_of_tables_and_columns = [x for x in groups if x[0] not in exc]
print groups
print list_of_tables_and_columns

a=['32','243','434','312']

for i in a:
	print i + "      ",
print
print "djhjk"
