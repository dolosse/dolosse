"""Test suite for the generic consumer"""

from dolosse.analysis.consumers.generic_json_consumer.generic_json_consumer import populate, update_data, find_number

def test_populate():
    test_var = {}
    populate(test_var, {"A": 3, "B": 4, "C": "0xf"}, 1)
    populate(test_var, {"A": 4, "B": 8, "C": "0xe"}, 2)
    populate(test_var, {"A": 5, "B": 4, "C": "0xd"}, 3)
    populate(test_var, {"A": 7, "D": {"A": 8, "B": 9}, "C": "0xc"}, 4)
    populate(test_var, {"A": 7, "D": {"A": 8}, "B": 4, "C": "0xb"}, 5)
    assert(test_var["A"] == [3, 4, 5, 7, 7])
    assert(test_var["B"] == [4, 8, 4, 4])
    assert(test_var["C"] == [15, 14, 13, 12, 11])
    assert(test_var["D: A"] == [8, 8])
    assert(test_var["D: B"] == [9])
    
def test_update():
    test_var = update_data('{"A": 3, "B": 4, "C": "0xf"}')
    assert(test_var["C"] == "0xf")
    assert(test_var["A"] == 3)
    assert(test_var["B"] == 4)
    test_var = update_data('{"A": 4, "B": 5, "D": 7}')
    assert(test_var["A"] == 4)
    assert(test_var["B"] == 5)
    assert(test_var["D"] == 7)

def test_find_number(): 
    number, validity = find_number(3.14)
    assert(number == 3.14)
    assert(validity == True)
    number, validity = find_number("Not a number")
    assert(number == 0)
    assert(validity == False)
    number, validity = find_number("3.14")
    assert(number == 3.14)
    assert(validity == True)
    number, validity = find_number({1: 2, 3: 4})
    assert(number == 0)
    assert(validity == False)
    number, validity = find_number("0xABC")
    assert(number == 2748)
    assert(validity == True)
    number, validity = find_number([1, 1, 2, 3, 5, 8, 13])
    assert(number == 0)
    assert(validity == False)
