
import mysql.connector
import requests 
from flask import Flask, jsonify, request

app = Flask(__name__)

cursor = None
connection = None
def makeConnection():
    global cursor
    global connection
    try:
        connection = mysql.connector.connect(
            host="database-2.clfhspcuxzv5.ap-south-1.rds.amazonaws.com",
            port=3306,
            user= "admin",
            password= "SdoWEfcpUCLiNKUgoxUj",
            database= "awsDatabase"
        )
        cursor = connection.cursor()
        print("Connected to the database!")
    except Exception as e:
        print("Error:", e)

@app.route("/",methods=["GET"])
def showHome():
    htmlContent= """Goto <br> <b>/initializedb</b> For Initialize DataBase <br> <b>/statistics?month=<i>int</i></b> for Monthly Statistics<br>
    <b>/bar-chart?month=<i>int</i></b> For Bar Chart Stat <br> <b>/pie-chart?month=<i>int</i></b> For Pie Chart Stat
    """
    return htmlContent,200

@app.route("/initializedb",methods=["GET"])
def initializeData():
    makeConnection()
    cursor.execute("DROP TABLE IF EXISTS itemsData")
    create_table_query = """
    CREATE TABLE itemsData( 
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(250),
    price DECIMAL(10,2),
    description VARCHAR(250),
    category VARCHAR(250),
    image  VARCHAR(250),
    sold BOOLEAN,
    dateOfSale TIMESTAMP
    )"""
    cursor.execute(create_table_query)
    print("Table Created successfully!")
    url="https://s3.amazonaws.com/roxiler.com/product_transaction.json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        for item in data:
            insertQuery = """
            INSERT INTO itemsData(title,price,description,category,image,sold,dateOfSale)
            VALUES (%s, %s, %s, %s, %s,%s,%s)
            """
            values = (item["title"],item["price"],item["description"],item["category"],item["image"],item["sold"],item["dateOfSale"])
            cursor.execute(insertQuery,values)
    connection.commit()
    print("Data Inserted Sucessfully!")
    cursor.close()
    connection.close()
    return jsonify("Data Inserted"),200


@app.route("/statistics",methods=["GET"])
def showStatistics():
    month = request.args.get("month")
    if month == None or int(month) < 1 or int(month)>12 :
        return "<h2>Enter Valid Month in URL</h2>"
    makeConnection()
    showStatisticsQuery="""SELECT SUM(price) as total_sale_amount,
                            SUM(sold) as total_sold_items,
                            SUM(CASE WHEN sold = 0 THEN 1 ELSE 0 END) as total_not_sold_items
                            FROM itemsData  WHERE DATE_Format(dateOfSale,"%m") = {}""".format(month)
    cursor.execute(showStatisticsQuery)
    rows = cursor.fetchone()
    total_sale_amount, total_sold_items,total_not_sold_items = rows
    cursor.close()
    connection.close()
    return jsonify({"totalSaleAmount":total_sale_amount,"totalSoldItems":total_sold_items,"totalNotSoldItems":total_not_sold_items}),200


@app.route("/bar-chart")
def getBarChartData():
    month = request.args.get("month")
    if month == None or int(month) < 1 or int(month)>12 :
        return "<h2>Enter Valid Month in URL</h2>"
    makeConnection()
    showBarQuery =  """SELECT COUNT(*) as num_items,
                             CASE 
                                 WHEN price BETWEEN 0 AND 100 THEN '0-100'
                                 WHEN price BETWEEN 101 AND 200 THEN '101-200'
                                 WHEN price BETWEEN 201 AND 300 THEN '201-300'
                                 WHEN price BETWEEN 301 AND 400 THEN '301-400'
                                 WHEN price BETWEEN 401 AND 500 THEN '401-500'
                                 WHEN price BETWEEN 501 AND 600 THEN '501-600'
                                 WHEN price BETWEEN 601 AND 700 THEN '601-700'
                                 WHEN price BETWEEN 701 AND 800 THEN '701-800'
                                 WHEN price BETWEEN 801 AND 900 THEN '801-900'
                                 ELSE '901-above'
                             END as price_range
                      FROM itemsData
                      WHERE DATE_Format(dateOfSale,"%m") = {}
                      GROUP BY price_range""".format(month)
    cursor.execute(showBarQuery)
    rows = cursor.fetchall()
    priceRange=[]
    for row in rows:
        priceRange.append({"priceRange": row[1], "numOfItems": row[0]})
    print(priceRange)
    connection.close()
    return jsonify (priceRange),200


@app.route("/pie-chart")
def getPieChartData():
    month = request.args.get("month")
    if month == None or int(month) < 1 or int(month)>12 :
        return "<h2>Enter Valid Month in URL</h2>"
    makeConnection()
    pieChartQuery =  """SELECT category, COUNT(*) as num_items
                      FROM itemsData
                      WHERE DATE_FORMAT( dateOfSale,"%m") = {}
                      GROUP BY category""".format(month)
    cursor.execute(pieChartQuery)
    rows = cursor.fetchall()
    categories=[]
    for row in rows:
        categories.append({"Category": row[0], "numOfItems": row[1]})
    print(categories)
    connection.close()
    return jsonify (categories),200
    

app.run(debug=False,host="0.0.0.0")
