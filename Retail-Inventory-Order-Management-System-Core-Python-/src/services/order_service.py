from src.dao import order_dao

class OrderError(Exception):
    pass

class OrderService:
    def create_order(self, cust_id: int, items: list) -> dict:
        try:
            return order_dao.create_order(cust_id, items)
        except Exception as e:
            raise OrderError(str(e))

    def get_order_details(self, order_id: int) -> dict:
        order = order_dao.get_order(order_id)
        if not order:
            raise OrderError("Order not found")
        return order

    def cancel_order(self, order_id: int) -> dict:
        try:
            return order_dao.cancel_order(order_id)
        except Exception as e:
            raise OrderError(str(e))
