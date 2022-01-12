import json, os
print

class Database(object):
	def __init__(self, fileName):
		self.file = fileName
		if os.path.exists(self.file):
			with open(self.file, 'r') as f:
				tablesData = f.read()
				self.database = json.loads(tablesData)
		else:
			self.database = {}
	
	def table(self, tableName, *colNames, **constraints):
		if tableName in self.database:
			table = Table(self, tableName, self.database[tableName]["columns"])
			table.data = self.database[tableName]
			table.rowCount = table.data['rowCount']
			return table
		else:	
			falseConstraint = []
			for col in colNames:
				falseConstraint.append(False)

			nullConstraint = []
			for col in colNames:
				nullConstraint.append(None)

			try:
				notNull = constraints["notNull"]
			except KeyError:
				notNull = constraints["notNull"] = falseConstraint
			try:
				unique = constraints["unique"]
			except KeyError:
				unique = constraints["unique"] = falseConstraint
			try:
				primaryKey = constraints["primaryKey"]
			except KeyError:
				primaryKey = constraints["primaryKey"] = falseConstraint
			try:
				foreginKeyTable = constraints["foreginKeyTable"]
			except KeyError:
				foreginKeyTable = constraints["foreginKeyTable"] = nullConstraint
			try:
				foreginKeyCol = constraints["foreginKeyCol"]
			except KeyError:
				foreginKeyCol = constraints["foreginKeyCol"] = nullConstraint
			try:
				defualt = constraints["defualt"]
			except KeyError:
				defualt = constraints["defualt"] = nullConstraint

			colData = {}
			count = 0
			for col in colNames:
				colData[col] = {
					"notNull": notNull[count],
					"unique": unique[count],
					"primaryKey": primaryKey[count],
					"foreginKeyTable": foreginKeyTable[count],
					"foreginKeyCol": foreginKeyCol[count],
					"defualt": defualt[count]
				}
				count += 1
			
			table = Table(self, tableName, colData)
			return table

	def delTable(self, tableName):
		self.database.pop(tableName)
	
	def commitDatabase(self):
		with open(self.file, 'w') as f:
			f.write(json.dumps(self.database, indent=2))

class Table(object):
	def __init__(self, database, tableName, cols):
		self.database = database
		self.name = tableName
		self.cols = cols
		self.rowCount = 0
		self.data = {"rowCount": 0}
		self.data["columns"] = self.cols
		self.colNames = []
		for key, _ in self.cols.items():
			self.colNames.append(key)
	
	def addRow(self, *data):
		data = list(data)
		for col, _data in self.cols.items():
			if _data["primaryKey"] == True:
				data.insert(self.colNames.index(col), self.rowCount)
		if len(data) == len(self.cols):
			self.data[f"row {self.rowCount + 1}"] = data
			self.data["rowCount"] += 1
		else:
			pass
			#raise Exception("Data Error: Incorrect Amount of Data Passed")
	
	def removeRow(self, rowNum):
		pass
	
	def commitTable(self):
		self.database.database[self.name] = self.data

	def find(self, column, value):
		colIndex = self.colNames.index(column)
		rows = []
		for key, data in self.data.items():
			if key != "name" and key != 'rowCount' and key != 'columns':
				rows.append(data[colIndex])
		rowIndex = rows.index(value)
		rowDataList = self.data[f"row {rowIndex + 1}"]
		rowDataStr = ''
		count = 0
		for value in rowDataList:
			rowDataStr += f'{self.colNames[count]}: {value}, '
			count += 1
		rowDataStr = rowDataStr[:-2]
		return rowDataStr

def test1():
	db = Database('data.json')
	names = db.table('Names', 'Primary Key', 'fname', 'lname', primaryKey=(True, False, False))
	work = db.table('Work', 'Primary Key', 'Person ID', 'Occupation', 'Salary', primaryKey=(True, False, False, False))
	names.addRow('JJ', 'Brindamour')
	work.addRow('1', 'Fisherman', 35000)
	names.commitTable()
	work.commitTable()
	print(names.find('fname', 'JJ'))
	db.commitDatabase()

def test2():
	db = Database('data.json')
	names = db.table('Names')
	work = db.table('Work')
	names.addRow('Tim', 'Smith')
	work.addRow('2', 'Software Engineer', 100000)
	names.commitTable()
	work.commitTable()
	print(work.find('Person ID', '2'))
	db.commitDatabase()

def test3():
	db = Database('data.json')
	db.delTable('Names')
	db.commitDatabase()

test1()
test2()
test3()