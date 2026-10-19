from python.order_calculations import calculate_order_total


def test_order_total():
    result = calculate_order_total(3, 20)

    assert result == 60


def test_single_item():
    result = calculate_order_total(1, 50)

    assert result == 50


def test_zero_quantity():
    result = calculate_order_total(0, 100)

    assert result == 0