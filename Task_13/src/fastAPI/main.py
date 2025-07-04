import configparser
import urllib.parse
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
import pandas as pd


def connect_to_DB():
    config = configparser.ConfigParser()
    config.read(r'C:\Users\Rahul\Documents\Visual Studio 2017\python\config.config')

    username = config['SqlDB']['username']
    password = config['SqlDB']['password']
    server   = config['SqlDB']['server']
    database = config['SqlDB']['database']
    driver   = config['SqlDB']['driver']

    encoded_password = urllib.parse.quote_plus(password)

    params = urllib.parse.quote_plus(
        f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={encoded_password}"
    )

    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

app = FastAPI(title="Order CRUD API")

# Step 2: Define Pydantic model
class Order(BaseModel):
    order_id: int
    customer_name: str
    product: str
    quantity: int
    price: float

# Step 3: CRUD Endpoints

@app.get("/orders")
def get_orders():
    engine = connect_to_DB()
    query = "SELECT * FROM order_data"
    df = pd.read_sql(query, con=engine)
    return df.to_dict(orient="records")

@app.get("/orders/{order_id}")
def get_order(order_id: int):
    engine = connect_to_DB()
    query = f"SELECT * FROM order_data WHERE order_id = {order_id}"
    df = pd.read_sql(query, con=engine)
    if df.empty:
        raise HTTPException(status_code=404, detail="Order not found")
  


@app.post("/orders")
def create_order(order: Order):
    engine = connect_to_DB()
    query = text("""
        INSERT INTO order_data (order_id, customer_name, product, quantity, price)
        VALUES (:order_id, :customer_name, :product, :quantity, :price)
    """)
    with engine.begin() as conn:
        conn.execute(query, **order.dict())
    return {"message": "Order created successfully", "order": order}

@app.put("/orders/{order_id}")
def update_order(order_id: int, order: Order):
    engine = connect_to_DB()
    query = text("""
        UPDATE order_data
        SET customer_name = :customer_name,
            product = :product,
            quantity = :quantity,
            price = :price
        WHERE order_id = :order_id
    """)
    
    with engine.begin() as conn:
        result = conn.execute(query, {
            "order_id": order_id,
            "customer_name": order.customer_name,
            "product": order.product,
            "quantity": order.quantity,
            "price": order.price
        })

    return {"message": f"Order {order_id} updated successfully"}

@app.delete("/orders/{order_id}")
def delete_order(order_id: int):
    engine = connect_to_DB()
    query = text("DELETE FROM order_data WHERE order_id = :order_id")
    with engine.begin() as conn:
        result = conn.execute(query, {"order_id": order_id})
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Order not found")
    return {"message": f"Order {order_id} deleted"}
