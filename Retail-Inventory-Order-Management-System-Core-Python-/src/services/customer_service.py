# src/services/customer_service.py

from src.dao import customer_dao

class CustomerError(Exception):
    pass

class CustomerService:
    def __init__(self):
        self.dao = customer_dao

    def add_customer(self, name, email, phone, city=None):
        try:
            return self.dao.add_customer(name, email, phone, city)
        except Exception as e:
            raise CustomerError(str(e))

    def list_customers(self):
        return self.dao.list_customers()

    def update_customer(self, cust_id, phone=None, city=None):
        try:
            return self.dao.update_customer(cust_id, phone, city)
        except Exception as e:
            raise CustomerError(str(e))

    def delete_customer(self, cust_id):
        try:
            return self.dao.delete_customer(cust_id)
        except Exception as e:
            raise CustomerError(str(e))
