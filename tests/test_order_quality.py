from python.order_quality import is_valid_order


def test_valid_order():
    order = {
        "order_id": 1001,
        "customer_id": 25
    }

    assert is_valid_order(order) is True


def test_missing_order_id():
    order = {
        "order_id": None,
        "customer_id": 25
    }

    assert is_valid_order(order) is False


def test_missing_customer_id():
    order = {
        "order_id": 1002,
        "customer_id": None
    }

    assert is_valid_order(order) is False


def test_customer_id_not_provided():
    order = {
        "order_id": 1003
    }

    assert is_valid_order(order) is False