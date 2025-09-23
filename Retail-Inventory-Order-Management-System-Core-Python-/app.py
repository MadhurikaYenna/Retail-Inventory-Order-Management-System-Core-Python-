import os
import streamlit as st
from src.config import get_supabase
from src.services import product_service, order_service, customer_service

st.title("Retail Inventory & Order Management")

# Example: show products
if st.button("Refresh products"):
    pass

products = product_service.list_products()  # implement this service wrapper
for p in products:
    st.write(f"{p['prod_id']}: {p['name']} — ₹{p['price']} — stock: {p['stock']}— categoty: {p['category']}")

# Example: add a product form
with st.form("add_product"):
    name = st.text_input("Name")
    sku = st.text_input("SKU")
    price = st.number_input("Price", min_value=0.0, format="%.2f")
    stock = st.number_input("Stock", min_value=0, step=1)
    category = st.text_input("Categoty")
    if st.form_submit_button("Add"):
        try:
            product_service.add_product(name, sku, price, int(stock),category)
            st.success("Product added")
        except Exception as e:
            st.error(f"Failed: {e}")
