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
				colIndex = self.colNames.index(col)
				keyContained = False
				for row, _data in self.database.database[info["foreignKey"][0]].items():
					if row != "rowCount" and row != "columns":
						if data[colIndex] == _data[self.colNames.index(info["foreignKey"][1])]:
							keyContained = True

				if not keyContained:
					raise Exception(f"Data Error: Foreign Key \"{data[colIndex]}\" is not in table \"{info['foreignKey'][0]}\", column \"{info['foreignKey'][1]}\"")


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