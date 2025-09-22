# src/dao/customer_dao.py
from typing import Optional, List, Dict
from datetime import datetime
from src.config import get_supabase

class CustomerDAO:
    def __init__(self):
        self.sb = get_supabase()

    def create_customer(self, name: str, email: str, phone: str, city: str | None = None) -> Optional[Dict]:
        # Check if email exists
        existing = self.sb.table("customers").select("*").eq("email", email).limit(1).execute()
        if existing.data:
            raise Exception(f"Email '{email}' already exists")

        payload = {
            "name": name,
            "email": email,
            "phone": phone,
            "city": city,
            "created_at": datetime.now().isoformat()
        }
        self.sb.table("customers").insert(payload).execute()
        # Fetch the inserted row
        resp = self.sb.table("customers").select("*").eq("email", email).limit(1).execute()
        return resp.data[0] if resp.data else None

    def get_customer_by_id(self, cust_id: int) -> Optional[Dict]:
        resp = self.sb.table("customers").select("*").eq("cust_id", cust_id).limit(1).execute()
        return resp.data[0] if resp.data else None

    def get_customer_by_email(self, email: str) -> Optional[Dict]:
        resp = self.sb.table("customers").select("*").eq("email", email).limit(1).execute()
        return resp.data[0] if resp.data else None

    def list_customers(self, limit: int = 100, city: str | None = None) -> List[Dict]:
        q = self.sb.table("customers").select("*").order("cust_id", desc=False).limit(limit)
        if city:
            q = q.eq("city", city)
        resp = q.execute()
        return resp.data or []

    def update_customer(self, cust_id: int, phone: str | None = None, city: str | None = None) -> Optional[Dict]:
        fields = {}
        if phone:
            fields["phone"] = phone
        if city:
            fields["city"] = city
        if not fields:
            raise Exception("Nothing to update")

        self.sb.table("customers").update(fields).eq("cust_id", cust_id).execute()
        resp = self.sb.table("customers").select("*").eq("cust_id", cust_id).limit(1).execute()
        return resp.data[0] if resp.data else None

    def delete_customer(self, cust_id: int) -> Optional[Dict]:
        # Check if customer has orders
        orders = self.sb.table("orders").select("*").eq("cust_id", cust_id).limit(1).execute()
        if orders.data:
            raise Exception("Cannot delete customer with existing orders")

        resp_before = self.sb.table("customers").select("*").eq("cust_id", cust_id).limit(1).execute()
        row = resp_before.data[0] if resp_before.data else None
        self.sb.table("customers").delete().eq("cust_id", cust_id).execute()
        return row
