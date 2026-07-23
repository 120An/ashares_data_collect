from data_collect.utils.etf_utils import to_bare_code, infer_market, is_etf_code


def test_to_bare_code():
    assert to_bare_code("510300.SH") == "510300"
    assert to_bare_code("159915.SZ") == "159915"
    assert to_bare_code("159915") == "159915"
    assert to_bare_code("sh510300") == "510300"


def test_infer_market():
    assert infer_market("510300") == "SH"
    assert infer_market("588000") == "SH"
    assert infer_market("159915") == "SZ"
    assert infer_market("160123") == "SZ"


def test_is_etf_code():
    assert is_etf_code("510300")   # 沪 ETF
    assert is_etf_code("159915")   # 深 ETF
    assert is_etf_code("588000")   # 科创 ETF
    assert not is_etf_code("600000")  # 股票
    assert not is_etf_code("000001")  # 股票
    assert not is_etf_code("30015")   # 位数不对
