# src/services/customer_service.py
from typing import Optional, Dict, List
import src.dao.customer_dao as customer_dao
import src.dao.order_dao as order_dao

class CustomerError(Exception):
    """Base exception for customer service errors."""
    pass

class CustomerExistsError(CustomerError):
    """Raised when attempting to create a customer with an email that already exists."""
    pass

class CustomerNotFoundError(CustomerError):
    """Raised when a customer is not found."""
    pass

class CustomerDeleteError(CustomerError):
    """Raised when a customer cannot be deleted due to existing orders."""
    pass

def register_customer(name: str, email: str, phone: str, city: Optional[str] = None) -> Dict:
    """
    Register a new customer.

    Validations:
    - name, email, phone are required (non-empty).
    - email must be unique.

    Returns the created customer dict.

    Raises:
      CustomerExistsError if email already exists.
      CustomerError for other validation failures.
    """
    # Basic validations
    if not name or not name.strip():
        raise CustomerError("Name is required")
    if not email or not email.strip():
        raise CustomerError("Email is required")
    if not phone or not phone.strip():
        raise CustomerError("Phone is required")

    # Check uniqueness of email
    existing = customer_dao.get_customer_by_email(email.strip())
    if existing:
        raise CustomerExistsError(f"Customer with email '{email}' already exists (cust_id={existing.get('cust_id')})")

    # Create customer via DAO
    created = customer_dao.create_customer(name.strip(), email.strip(), phone.strip(), city.strip() if city else None)
    if not created:
        raise CustomerError("Failed to create customer due to unknown error")
    return created

def update_customer(cust_id: int, fields: Dict) -> Dict:
    """
    Update customer fields.

    fields: dict of column -> value; allowed keys: name, email, phone, city

    Validations:
    - If email provided, ensure uniqueness (email not used by another customer).
    - Phone if provided must be non-empty.

    Returns updated customer dict.

    Raises:
      CustomerNotFoundError if customer doesn't exist.
      CustomerExistsError if email conflicts with another customer.
      CustomerError for general failures.
    """
    cust = customer_dao.get_customer_by_id(cust_id)
    if not cust:
        raise CustomerNotFoundError(f"Customer not found: {cust_id}")

    updates = {}
    allowed = {"name", "email", "phone", "city"}
    for k, v in fields.items():
        if k not in allowed:
            continue
        if v is None:
            # allow clearing optional fields like city by passing None
            updates[k] = None
            continue
        val = str(v).strip()
        if k == "email":
            if not val:
                raise CustomerError("Email cannot be empty")
            # check email uniqueness (ignore self)
            other = customer_dao.get_customer_by_email(val)
            if other and other.get("cust_id") != cust_id:
                raise CustomerExistsError(f"Email '{val}' is already used by cust_id={other.get('cust_id')}")
            updates[k] = val
        elif k == "phone":
            if not val:
                raise CustomerError("Phone cannot be empty")
            updates[k] = val
        else:
            updates[k] = val

    if not updates:
        return cust  # nothing to update

    updated = customer_dao.update_customer(cust_id, updates)
    if not updated:
        raise CustomerError("Failed to update customer")
    return updated

def remove_customer(cust_id: int) -> Dict:
    """
    Remove a customer if they have no orders.

    Steps:
    - Check if customer exists.
    - Check orders for this customer (using order_dao.list_orders_by_customer).
    - If any orders exist, raise CustomerDeleteError.
    - Otherwise delete via DAO and return deleted row.

    Raises:
      CustomerNotFoundError, CustomerDeleteError, CustomerError
    """
    cust = customer_dao.get_customer_by_id(cust_id)
    if not cust:
        raise CustomerNotFoundError(f"Customer not found: {cust_id}")

    orders = order_dao.list_orders_by_customer(cust_id)
    if orders:
        raise CustomerDeleteError(f"Cannot delete customer {cust_id}: customer has {len(orders)} order(s)")

    deleted = customer_dao.delete_customer(cust_id)
    if not deleted:
        raise CustomerError("Failed to delete customer")
    return deleted

def get_customer(cust_id: int) -> Dict:
    """
    Return customer record or raise CustomerNotFoundError.
    """
    cust = customer_dao.get_customer_by_id(cust_id)
    if not cust:
        raise CustomerNotFoundError(f"Customer not found: {cust_id}")
    return cust

def list_customers(limit: int = 100) -> List[Dict]:
    """
    Return list of customers (delegates to DAO).
    """
    return customer_dao.list_customers(limit=limit)
