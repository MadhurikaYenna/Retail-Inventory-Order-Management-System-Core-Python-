# src/services/order_service.py
from typing import List, Dict
import src.dao.product_dao as product_dao
import src.dao.customer_dao as customer_dao
import src.dao.order_dao as order_dao

class OrderError(Exception):
    pass

def create_order(customer_id: int, items: List[Dict]) -> Dict:
    """
    items: [{ "prod_id": int, "quantity": int }, ...]
    Orchestration using DAOs.
    Note: Supabase REST is not transactional across multiple requests;
    we implement a best-effort flow and attempt rollback on failures.
    """
    # Validate customer
    customer = customer_dao.get_customer_by_id(customer_id)
    if not customer:
        raise OrderError("Customer not found")

    # Validate products & compute total
    order_items = []
    for it in items:
        prod = product_dao.get_product_by_id(it["prod_id"])
        if not prod:
            raise OrderError(f"Product not found: {it['prod_id']}")
        qty = int(it["quantity"])
        if qty <= 0:
            raise OrderError("Quantity must be positive")
        stock = int(prod.get("stock") or 0)
        if stock < qty:
            raise OrderError(f"Out of stock for product {prod['name']} (available {stock}, required {qty})")
        order_items.append({"prod_id": prod["prod_id"], "quantity": qty, "price": float(prod["price"])})

    total = sum(it["quantity"] * it["price"] for it in order_items)

    created_order = None
    created_items = []
    # Track products updated for rollback
    updated_products = []

    try:
        # 1) create order record
        created_order = order_dao.create_order_record(customer_id, total, status="PLACED")
        order_id = created_order["order_id"]

        # 2) create order items (records with price captured)
        created_items = order_dao.create_order_items(order_id, order_items)

        # 3) decrement product stocks
        for it in order_items:
            prod = product_dao.get_product_by_id(it["prod_id"])
            new_stock = int(prod.get("stock") or 0) - int(it["quantity"])
            # update product
            product_dao.update_product(it["prod_id"], {"stock": new_stock})
            updated_products.append({"prod_id": it["prod_id"], "delta": it["quantity"]})

        # success: return composed order
        created_order["items"] = created_items
        created_order["total_amount"] = total
        return created_order

    except Exception as e:
        # Best-effort rollback: try to restore product stocks and delete partial order records
        try:
            # restore stocks
            for up in updated_products:
                prod = product_dao.get_product_by_id(up["prod_id"])
                if prod:
                    restored_stock = (prod.get("stock") or 0) + up["delta"]
                    product_dao.update_product(up["prod_id"], {"stock": restored_stock})
            # delete created order items & order
            if created_items:
                # delete created items
                for it in created_items:
                    # safe delete by item_id
                    _ = order_dao._sb().table("order_items").delete().eq("item_id", it["item_id"]).execute()
            if created_order:
                _ = order_dao._sb().table("orders").delete().eq("order_id", created_order["order_id"]).execute()
        except Exception:
            # If rollback fails, log (here we re-raise original)
            pass
        raise OrderError(f"Failed to create order: {e}")

def get_order_details(order_id: int) -> Dict:
    order = order_dao.get_order(order_id)
    if not order:
        raise OrderError("Order not found")
    # include customer info
    cust = customer_dao.get_customer_by_id(order.get("cust_id"))
    order["customer"] = cust
    return order

def cancel_order(order_id: int) -> Dict:
    order = order_dao.get_order(order_id)
    if not order:
        raise OrderError("Order not found")
    if order.get("status") != "PLACED":
        raise OrderError("Only orders in PLACED status can be cancelled")
    # restore stock for each item
    try:
        for it in order.get("items", []):
            prod = product_dao.get_product_by_id(it["prod_id"])
            if prod:
                new_stock = (prod.get("stock") or 0) + int(it["quantity"])
                product_dao.update_product(it["prod_id"], {"stock": new_stock})
        # update order status
        updated = order_dao.update_order_status(order_id, "CANCELLED")
        return updated
    except Exception as e:
        raise OrderError(f"Failed to cancel order: {e}")
