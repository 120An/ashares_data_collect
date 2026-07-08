def test_package_imports():
    import tick_analysis
    assert tick_analysis.__version__ == "0.1.0"


def test_fixture_shape(simple_frame):
    assert len(simple_frame) == 4
    assert "bid_price_5" in simple_frame.columns
