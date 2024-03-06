# CUSTOMER BEHAVIOUR AND TRENDING

## Objective

- Process, ETL raw data (~8,5 GB data log from users) into data that can be analyzed, save cleaned data in databases: MySQL.
- Data from an online media company
- Teck stack: Python, PySpark, MySQL

## Raw data

```
root 
	|-- eventID: string (nullable = true)
	|-- datetime: string (nullable = true) 
	|-- user_id: string (nullable = true) 
	|-- keyword: string (nullable = true) 
	|-- category: string (nullable = true) 
	|-- proxy_isp: string (nullable = true) 
	|-- platform: string (nullable = true) 
	|-- networkType: string (nullable = true) 
	|-- action: string (nullable = true) 
	|-- userPlansMap: array (nullable = true) 
	| |-- element: string (containsNull = true)
```

## Cleaned data

```
root 
	|-- user_id: string (nullable = true) 
	|-- Most_Search_T6: string (nullable = true) 
	|-- Category_T6: string (nullable = true) 
	|-- Most_Search_T7: string (nullable = true) 
	|-- Category_T7: string (nullable = true) 
	|-- Category_change: string (nullable = false)
```
