# src/dao/order_dao.py
from typing import List, Dict, Optional
from src.config import get_supabase

def _sb():
    return get_supabase()

def create_order_record(cust_id: int, total_amount: float, status: str = "PLACED") -> Optional[Dict]:
    payload = {"cust_id": cust_id, "total_amount": total_amount, "status": status}
    _sb().table("orders").insert(payload).execute()
    # fetch recent order for this customer with same total (best-effort)
    resp = _sb().table("orders").select("*").eq("cust_id", cust_id).eq("total_amount", total_amount).order("order_date", desc=True).limit(1).execute()
    return resp.data[0] if resp.data else None

def create_order_items(order_id: int, items: List[Dict]) -> List[Dict]:
    """
    items: list of dicts each having prod_id, quantity, price
    """
    payloads = []
    for it in items:
        payloads.append({
            "order_id": order_id,
            "prod_id": it["prod_id"],
            "quantity": it["quantity"],
            "price": it["price"]
        })
    _sb().table("order_items").insert(payloads).execute()
    # fetch inserted items by order_id
    resp = _sb().table("order_items").select("*").eq("order_id", order_id).execute()
    return resp.data or []

def get_order(order_id: int) -> Optional[Dict]:
    # Fetch order
    r1 = _sb().table("orders").select("*").eq("order_id", order_id).limit(1).execute()
    if not r1.data:
        return None
    order = r1.data[0]
    # Fetch items
    r2 = _sb().table("order_items").select("*").eq("order_id", order_id).execute()
    order["items"] = r2.data or []
    return order

def list_orders_by_customer(cust_id: int) -> List[Dict]:
    resp = _sb().table("orders").select("*").eq("cust_id", cust_id).order("order_date", desc=True).execute()
    return resp.data or []

def update_order_status(order_id: int, status: str) -> Optional[Dict]:
    _sb().table("orders").update({"status": status}).eq("order_id", order_id).execute()
    resp = _sb().table("orders").select("*").eq("order_id", order_id).limit(1).execute()
    return resp.data[0] if resp.data else None

def delete_order(order_id: int) -> Optional[Dict]:
    # fetch order to return
    resp_before = _sb().table("orders").select("*").eq("order_id", order_id).limit(1).execute()
    row = resp_before.data[0] if resp_before.data else None
    # delete items then order
    _sb().table("order_items").delete().eq("order_id", order_id).execute()
    _sb().table("orders").delete().eq("order_id", order_id).execute()
    return row
