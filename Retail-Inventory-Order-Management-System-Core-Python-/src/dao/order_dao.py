# src/dao/order_dao.py
from typing import Optional, List, Dict
from datetime import datetime
from src.config import get_supabase

class OrderDAO:
    def __init__(self):
        self.sb = get_supabase()

    # --- Create Order ---
    def create_order(self, cust_id: int, items: List[Dict]) -> Optional[Dict]:
        # Check customer exists
        customer = self.sb.table("customers").select("*").eq("cust_id", cust_id).limit(1).execute().data
        if not customer:
            raise Exception(f"Customer {cust_id} does not exist")
        customer = customer[0]

        # Check products and stock
        prod_ids = [item['prod_id'] for item in items]
        products = self.sb.table("products").select("*").in_("prod_id", prod_ids).execute().data
        if len(products) != len(items):
            missing = set(prod_ids) - set(p['prod_id'] for p in products)
            raise Exception(f"Products not found: {missing}")

        total_amount = 0
        for item in items:
            prod = next(p for p in products if p['prod_id'] == item['prod_id'])
            if prod['stock'] < item['quantity']:
                raise Exception(f"Not enough stock for product {prod['name']}")
            total_amount += prod['price'] * item['quantity']

        # Insert order
        order_payload = {
            "cust_id": cust_id,
            "order_date": datetime.now().isoformat(),
            "status": "PLACED",
            "total_amount": total_amount
        }
        self.sb.table("orders").insert(order_payload).execute()

        # Fetch the inserted order
        order = self.sb.table("orders").select("*").eq("cust_id", cust_id).order("order_id", desc=True).limit(1).execute().data[0]

        # Insert order items and deduct stock
        for item in items:
            prod = next(p for p in products if p['prod_id'] == item['prod_id'])
            # Insert order item
            self.sb.table("order_items").insert({
                "order_id": order['order_id'],
                "prod_id": item['prod_id'],
                "quantity": item['quantity'],
                "price": prod['price']
            }).execute()
            # Update product stock
            self.sb.table("products").update({"stock": prod['stock'] - item['quantity']}).eq("prod_id", prod['prod_id']).execute()

        # Attach customer and items for return
        order['customer'] = customer
        order['items'] = [
            {"prod_id": item['prod_id'], "quantity": item['quantity'], 
             "price": next(p['price'] for p in products if p['prod_id'] == item['prod_id'])} 
            for item in items
        ]
        return order

    # --- Get Order Details ---
    def get_order_details(self, order_id: int) -> Optional[Dict]:
        order_list = self.sb.table("orders").select("*").eq("order_id", order_id).limit(1).execute().data
        if not order_list:
            return None
        order = order_list[0]

        # Customer info
        customer_list = self.sb.table("customers").select("*").eq("cust_id", order['cust_id']).limit(1).execute().data
        order['customer'] = customer_list[0] if customer_list else None

        # Order items
        items = self.sb.table("order_items").select("*").eq("order_id", order_id).execute().data
        order['items'] = items
        return order

    # --- Cancel Order ---
    def cancel_order(self, order_id: int) -> Optional[Dict]:
        order_list = self.sb.table("orders").select("*").eq("order_id", order_id).limit(1).execute().data
        if not order_list:
            raise Exception("Order not found")
        order = order_list[0]

        if order['status'] != "PLACED":
            raise Exception("Only PLACED orders can be cancelled")

        # Restore stock
        items = self.sb.table("order_items").select("*").eq("order_id", order_id).execute().data
        for item in items:
            prod_list = self.sb.table("products").select("*").eq("prod_id", item['prod_id']).limit(1).execute().data
            if prod_list:
                prod = prod_list[0]
                self.sb.table("products").update({"stock": prod['stock'] + item['quantity']}).eq("prod_id", prod['prod_id']).execute()

        # Update order status
        self.sb.table("orders").update({"status": "CANCELLED"}).eq("order_id", order_id).execute()

        # Return updated order
        updated_order = self.get_order_details(order_id)
        return updated_order
