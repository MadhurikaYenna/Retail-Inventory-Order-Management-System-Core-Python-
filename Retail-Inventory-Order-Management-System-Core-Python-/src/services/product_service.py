# src/services/product_service.py
from typing import Optional, Dict, List
import src.dao.product_dao as product_dao

class ProductError(Exception):
    """Base exception for product service errors."""
    pass

class ProductExistsError(ProductError):
    """Raised when SKU already exists."""
    pass

class ProductNotFoundError(ProductError):
    """Raised when a product is not found."""
    pass

class ProductDeleteError(ProductError):
    """Raised when product cannot be deleted (e.g., referenced in orders)."""
    pass

def add_product(name: str, sku: str, price: float, stock: int = 0, category: Optional[str] = None) -> Dict:
    """
    Validate and insert a new product.
    Raises:
      ProductExistsError if SKU already exists.
      ProductError for validation failures.
    Returns created product dict.
    """
    if not name or not name.strip():
        raise ProductError("Product name is required")
    if not sku or not sku.strip():
        raise ProductError("SKU is required")
    if price is None or price <= 0:
        raise ProductError("Price must be greater than 0")
    if stock is None or int(stock) < 0:
        raise ProductError("Stock must be >= 0")

    # SKU uniqueness check
    existing = product_dao.get_product_by_sku(sku.strip())
    if existing:
        raise ProductExistsError(f"SKU already exists: {sku.strip()} (prod_id={existing.get('prod_id')})")

    created = product_dao.create_product(name.strip(), sku.strip(), float(price), int(stock), category.strip() if category else None)
    if not created:
        raise ProductError("Failed to create product")
    return created

def get_product(prod_id: int) -> Dict:
    """
    Return product dict or raise ProductNotFoundError.
    """
    p = product_dao.get_product_by_id(prod_id)
    if not p:
        raise ProductNotFoundError(f"Product not found: {prod_id}")
    return p

def list_products(limit: int = 100, category: Optional[str] = None) -> List[Dict]:
    """
    List products. Optionally filter by category.
    """
    return product_dao.list_products(limit=limit, category=category)

def search_products_by_name(name_substr: str, limit: int = 100) -> List[Dict]:
    """
    Simple search by substring on product name.
    (Implemented in service so UI/CLI can call it; DAO currently doesn't do LIKE queries,
    so we use list_products and filter in Python.)
    """
    name_substr = (name_substr or "").strip().lower()
    if not name_substr:
        return list_products(limit=limit)
    allp = product_dao.list_products(limit=1000)
    return [p for p in allp if name_substr in (p.get("name") or "").lower()][:limit]

def update_product(prod_id: int, fields: Dict) -> Dict:
    """
    Partial update of a product.
    Allowed fields: name, sku, price, stock, category
    Validations:
      - If sku provided, must be unique (not used by another product).
      - price > 0 (if provided), stock >= 0 (if provided)
    Returns updated product dict.
    """
    existing = product_dao.get_product_by_id(prod_id)
    if not existing:
        raise ProductNotFoundError(f"Product not found: {prod_id}")

    updates = {}
    allowed = {"name", "sku", "price", "stock", "category"}
    for k, v in fields.items():
        if k not in allowed:
            continue
        if v is None:
            updates[k] = None
            continue
        if k == "sku":
            sku_val = str(v).strip()
            if not sku_val:
                raise ProductError("SKU cannot be empty")
            other = product_dao.get_product_by_sku(sku_val)
            if other and other.get("prod_id") != prod_id:
                raise ProductExistsError(f"SKU '{sku_val}' is already used by prod_id={other.get('prod_id')}")
            updates[k] = sku_val
        elif k == "price":
            price_val = float(v)
            if price_val <= 0:
                raise ProductError("Price must be greater than 0")
            updates[k] = price_val
        elif k == "stock":
            stock_val = int(v)
            if stock_val < 0:
                raise ProductError("Stock must be >= 0")
            updates[k] = stock_val
        else:
            updates[k] = str(v).strip()

    if not updates:
        return existing

    updated = product_dao.update_product(prod_id, updates)
    if not updated:
        raise ProductError("Failed to update product")
    return updated

def restock_product(prod_id: int, delta: int) -> Dict:
    """
    Increase stock by delta (delta must be positive).
    Returns updated product.
    """
    if delta is None or int(delta) <= 0:
        raise ProductError("Delta must be a positive integer")
    p = product_dao.get_product_by_id(prod_id)
    if not p:
        raise ProductNotFoundError(f"Product not found: {prod_id}")
    new_stock = (p.get("stock") or 0) + int(delta)
    return product_dao.update_product(prod_id, {"stock": new_stock})

def reduce_stock(prod_id: int, delta: int) -> Dict:
    """
    Reduce stock by delta (delta must be positive).
    Raises ProductError if insufficient stock.
    Returns updated product.
    """
    if delta is None or int(delta) <= 0:
        raise ProductError("Delta must be a positive integer")
    p = product_dao.get_product_by_id(prod_id)
    if not p:
        raise ProductNotFoundError(f"Product not found: {prod_id}")
    current = int(p.get("stock") or 0)
    if current < int(delta):
        raise ProductError(f"Insufficient stock for product {prod_id}: available={current}, required={delta}")
    new_stock = current - int(delta)
    return product_dao.update_product(prod_id, {"stock": new_stock})

def delete_product(prod_id: int) -> Dict:
    """
    Delete a product.
    Note: If the product is referenced by order_items (FK), the delete may fail at DB level.
    We attempt to delete and return the deleted row (fetched prior to delete).
    If deletion fails (e.g., due to FK), raise ProductDeleteError with a helpful message.
    """
    p = product_dao.get_product_by_id(prod_id)
    if not p:
        raise ProductNotFoundError(f"Product not found: {prod_id}")

    # Basic safety: do not delete product with non-zero stock (business decision)
    if (p.get("stock") or 0) > 0:
        raise ProductDeleteError(f"Cannot delete product with stock > 0 (stock={p.get('stock')}). Reduce stock or mark inactive instead.")

    # Attempt delete and capture failure
    try:
        deleted = product_dao.delete_product(prod_id)
        if not deleted:
            raise ProductDeleteError("Delete did not return deleted row — check DB constraints")
        return deleted
    except Exception as e:
        # Bubble up as ProductDeleteError for clearer messaging
        raise ProductDeleteError(f"Failed to delete product {prod_id}: {e}")

def get_low_stock(threshold: int = 5) -> List[Dict]:
    """
    Return products with stock <= threshold.
    """
    allp = product_dao.list_products(limit=1000)
    return [p for p in allp if (p.get("stock") or 0) <= int(threshold)]
