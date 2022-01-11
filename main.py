import json, os

class Database(object):
	def __init__(self, fileName):
		self.file = fileName
		if os.path.exists(self.file):
			with open(self.file, 'r') as f:
				tablesData = f.read()
				self.database = json.loads(tablesData)
		else:
			self.database = {}
	
	def addTable(self, tableName, *colNames):
		table = Table(self, tableName, colNames)
		self.database[tableName] = table.data

	def table(self, tableName):	
		table = Table(self, self.database[tableName]["name"], self.database[tableName]["columns"])
		table.data = self.database[tableName]
		table.rowCount = table.data['rowCount']
		return table
	
	def commitDatabase(self):
		with open(self.file, 'w') as f:
			f.write(json.dumps(self.database, indent=2))

class Table(object):
	def __init__(self, database, tableName, colNames):
		self.database = database
		self.name = tableName
		self.cols = colNames
		self.rowCount = 0
		self.data = {"name": tableName, "rowCount": 0}
		self.data["columns"] = self.cols
	
	def addRow(self, *data):
		if len(data) == len(self.cols):
			self.data[f"row {self.rowCount + 1}"] = data
			self.data["rowCount"] += 1
		else:
			raise Exception("Data Error")
	
	def removeRow(self, rowNum):
		pass
	
	def commitTable(self):
		self.database.database[self.name] = self.data

	def find(self, column, value):
		colIndex = self.cols.index(column)
		rows = []
		for key, data in self.data.items():
			if key != "name" and key != 'rowCount' and key != 'columns':
				rows.append(data[colIndex])
		rowIndex = rows.index(value)
		rowDataList = self.data[f"row {rowIndex + 1}"]
		rowDataStr = ''
		count = 0
		for value in rowDataList:
			rowDataStr += f'{self.cols[count]}: {value}, '
			count += 1
		rowDataStr = rowDataStr[:-2]
		return rowDataStr

def test1():
	db = Database('data.json')
	db.addTable('Names', 'Primary Key', 'fname', 'lname')
	db.addTable('Work', 'Person ID', 'Occupation', 'Salary')
	names = db.table('Names')
	work = db.table('Work')
	names.addRow('1', 'JJ', 'Brindamour')
	work.addRow('1', 'Fisherman', 35000)
	names.commitTable()
	print(names.find('fname', 'JJ'))
	db.commitDatabase()

def test2():
	db = Database('data.json')
	names = db.table('Names')
	work = db.table('Work')
	names.addRow('2', 'Tim', 'Smith')
	work.addRow('2', 'Software Engineer', 100000)
	names.commitTable()
	print(work.find('Person ID', '2'))
	db.commitDatabase()

test2()