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
				foreignKey = constraints["foreignKey"]
			except KeyError:
				foreignKey = constraints["foreignKey"] = nullConstraint
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
					"foreignKey": foreignKey[count],
					"defualt": defualt[count]
				}
				count += 1
			
			table = Table(self, tableName, colData)
			return table

	def delTable(self, tableName):
		self.database.pop(tableName)
	
	def commit(self):
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
		for col, info in self.cols.items():
			if info["primaryKey"] == True:
				data.insert(self.colNames.index(col), self.rowCount)
			if info["unique"] == True:
				colIndex = self.colNames.index(col)
				for row, info in self.data.items():
					if row != "rowCount" and row != "columns":
						if info[colIndex] == data[colIndex]:
							raise Exception(f"Constraint Error: \"{data[colIndex]}\" is already in the Database under column \"{self.colNames[colIndex]}\"")
			if info["notNull"] == True and data[self.colNames.index(col)] == None: raise Exception(f"Data Error: {col} cannot be Null")
			if info["foreignKey"] != None:
				pass

		if len(data) == len(self.cols):
			self.data[f"row {self.rowCount + 1}"] = data
			self.data["rowCount"] += 1
			self.rowCount += 1
		else:
			raise Exception("Data Error: Incorrect Amount of Data Passed")
	
	def removeRow(self, rowNum):
		self.data.pop(f"row {rowNum}")
		data = []
		for row, _data in self.data.items():
			data.append(_data)
		
		i = -2
		while i <= len(self.data) - 1:
			if i > rowNum:
				self.data.pop(f"row {i}")
				self.data[f"row {i - 1}"] = data[i]
				for col in self.colNames:
					if self.data["columns"][col]["primaryKey"] == True:
						data[i][self.colNames.index(col)] -= 1
			i += 1

	
	def commit(self):
		self.database.database[self.name] = self.data

	def find(self, column, value):
		colIndex = self.colNames.index(column)
		rows = []
		for key, data in self.data.items():
			if key != 'rowCount' and key != 'columns':
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
	work.addRow(0, 'Fisherman', 35000)
	names.commit()
	work.commit()
	#print(names.find('fname', 'JJ'))
	db.commit()

def test2():
	db = Database('data.json')
	names = db.table('Names')
	work = db.table('Work')
	names.addRow('Tim', 'Smith')
	work.addRow(2, 'Software Engineer', 100000)
	names.commit()
	work.commit()
	#print(work.find('Person ID', '2'))
	db.commit()

def test3():
	db = Database('data.json')
	table = db.table("Table", "Primary Key", "Unique", primaryKey=(True, False), unique=(True, True))
	table.addRow("data")
	table.commit()
	db.commit()
	table.addRow("data")

def test4():
	db = Database('data.json')
	table = db.table('Table', 'primaryKey', 'fname', 'lname', primaryKey=(True, False, False), notNull=(False, True, False))
	table.addRow('JJ', 'Brindamour')
	table.addRow(None, None)
	table.commit()
	db.commit()

def test5():
	db = Database('data.json')
	names = db.table('Names')
	work = db.table('Work')
	names.addRow('Jake', 'Ceaser')
	work.addRow(2, 'Musician', 56000)
	names.commit()
	work.commit()
	work.removeRow(2)
	work.commit()
	db.commit()

def test6():
	db = Database('data.json')
	db.delTable('Names')
	db.commit()
