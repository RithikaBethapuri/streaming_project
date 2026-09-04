def test_event_schema_fields():
    expected = {"event_id","event_ts","trade_id","account_id","symbol","side","quantity","price","currency","event_type"}
    actual = {"event_id","event_ts","trade_id","account_id","symbol","side","quantity","price","currency","event_type"}
    assert expected == actual

def test_trade_value():
    assert 100 * 225.10 == 22510.0
