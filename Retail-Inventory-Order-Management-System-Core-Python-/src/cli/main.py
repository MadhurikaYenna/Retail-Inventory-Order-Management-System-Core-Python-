# src/cli/main.py
import argparse
import json
from src.dao.product_dao import ProductDAO
from src.dao.customer_dao import CustomerDAO
from src.dao.order_dao import OrderDAO
  # import the module with functions

class RetailCLI:
    def __init__(self):
        self.product_dao = ProductDAO()
        self.customer_dao = CustomerDAO()
        self.parser = self.build_parser()
        self.order_service = OrderDAO() 
    # --- Product commands ---
    def cmd_product_add(self, args):
        try:
            p = self.product_dao.create_product(args.name, args.sku, args.price, args.stock, args.category)
            print("Created product:")
            print(json.dumps(p, indent=2, default=str))
        except Exception as e:
            print("Error:", e)

    def cmd_product_list(self, args):
        try:
            ps = self.product_dao.list_products(limit=100)
            print(json.dumps(ps, indent=2, default=str))
        except Exception as e:
            print("Error:", e)

    # --- Customer commands ---
    def cmd_customer_add(self, args):
        try:
            c = self.customer_dao.create_customer(args.name, args.email, args.phone, args.city)
            print("Created customer:")
            print(json.dumps(c, indent=2, default=str))
        except Exception as e:
            print("Error:", e)

    def cmd_customer_update(self, args):
        try:
            c = self.customer_dao.update_customer(args.id, phone=args.phone, city=args.city)
            print("Updated customer:")
            print(json.dumps(c, indent=2, default=str))
        except Exception as e:
            print("Error:", e)

    def cmd_customer_delete(self, args):
        try:
            c = self.customer_dao.delete_customer(args.id)
            print("Deleted customer:")
            print(json.dumps(c, indent=2, default=str))
        except Exception as e:
            print("Error:", e)

    # --- Order commands ---
    def cmd_order_create(self, args):
        items = []
        for item in args.item:
            try:
                pid, qty = item.split(":")
                items.append({"prod_id": int(pid), "quantity": int(qty)})
            except Exception:
                print("Invalid item format:", item)
                return
        try:
            ord = self.order_service.create_order(args.customer, items)  # call function directly
            print("Order created:")
            print(json.dumps(ord, indent=2, default=str))
        except Exception as e:
            print("Error:", e)

    def cmd_order_show(self, args):
        try:
            o = self.order_service.get_order(args.order)  # call function directly
            print(json.dumps(o, indent=2, default=str))
        except Exception as e:
            print("Error:", e)

    def cmd_order_cancel(self, args):
        try:
            o = self.order_service.cancel_order(args.order)  # call function directly
            print("Order cancelled (updated):")
            print(json.dumps(o, indent=2, default=str))
        except Exception as e:
            print("Error:", e)

    # --- CLI Parser ---
    def build_parser(self):
        parser = argparse.ArgumentParser(prog="retail-cli")
        sub = parser.add_subparsers(dest="cmd")

        # Product
        p_prod = sub.add_parser("product", help="product commands")
        pprod_sub = p_prod.add_subparsers(dest="action")
        addp = pprod_sub.add_parser("add")
        addp.add_argument("--name", required=True)
        addp.add_argument("--sku", required=True)
        addp.add_argument("--price", type=float, required=True)
        addp.add_argument("--stock", type=int, default=0)
        addp.add_argument("--category", default=None)
        addp.set_defaults(func=self.cmd_product_add)

        listp = pprod_sub.add_parser("list")
        listp.set_defaults(func=self.cmd_product_list)

        # Customer
        pcust = sub.add_parser("customer")
        pcust_sub = pcust.add_subparsers(dest="action")
        addc = pcust_sub.add_parser("add")
        addc.add_argument("--name", required=True)
        addc.add_argument("--email", required=True)
        addc.add_argument("--phone", required=True)
        addc.add_argument("--city", default=None)
        addc.set_defaults(func=self.cmd_customer_add)

        updatec = pcust_sub.add_parser("update")
        updatec.add_argument("--id", type=int, required=True)
        updatec.add_argument("--phone", default=None)
        updatec.add_argument("--city", default=None)
        updatec.set_defaults(func=self.cmd_customer_update)

        delc = pcust_sub.add_parser("delete")
        delc.add_argument("--id", type=int, required=True)
        delc.set_defaults(func=self.cmd_customer_delete)

        # Orders
        porder = sub.add_parser("order")
        porder_sub = porder.add_subparsers(dest="action")

        createo = porder_sub.add_parser("create")
        createo.add_argument("--customer", type=int, required=True)
        createo.add_argument("--item", required=True, nargs="+", help="prod_id:qty (repeatable)")
        createo.set_defaults(func=self.cmd_order_create)

        showo = porder_sub.add_parser("show")
        showo.add_argument("--order", type=int, required=True)
        showo.set_defaults(func=self.cmd_order_show)

        cano = porder_sub.add_parser("cancel")
        cano.add_argument("--order", type=int, required=True)
        cano.set_defaults(func=self.cmd_order_cancel)

        return parser

    def run(self):
        args = self.parser.parse_args()
        if not hasattr(args, "func"):
            self.parser.print_help()
            return
        args.func(args)

def main():
    cli = RetailCLI()
    cli.run()

if __name__ == "__main__":
    main()
