def is_valid_order(order):
    return (
        order.get("order_id") is not None
        and order.get("customer_id") is not None
    )