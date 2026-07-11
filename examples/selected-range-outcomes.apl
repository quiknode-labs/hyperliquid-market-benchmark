// Exact selected-range fastest/tie evidence for the default AWS/NRT BBO view.
// Bind a bounded startTime/endTime in the Axiom query request. Change only the
// allowlisted dataset/cloud/region/coin filters for another dashboard view.
['hyperliquid-market-benchmark']
| extend schema=tostring(column_ifexists("schema", "")), event_type=tostring(column_ifexists("event_type", "")), metric_kind=tostring(column_ifexists("metric_kind", "")), dataset=tostring(column_ifexists("dataset", "")), cloud=tostring(column_ifexists("cloud", "")), region=tostring(column_ifexists("region", "")), coin=tostring(column_ifexists("coin", ""))
| extend provider=tostring(column_ifexists("provider", "")), protocol=tostring(column_ifexists("protocol", "")), event_id=tostring(column_ifexists("event_id", "")), runner=tostring(column_ifexists("runner", "")), run_id=tostring(column_ifexists("run_id", "")), window_id=tostring(column_ifexists("window_id", ""))
| where schema == 'hyperliquid-market-benchmark-v1' and event_type == 'latency_window'
| where metric_kind == 'event_to_canonical_book_ready'
| where dataset == 'bbo' and cloud == 'aws' and region == 'nrt' and coin == 'BTC'
| where tolong(column_ifexists("window_seconds", 0)) == 300 and tolong(column_ifexists("publish_interval_seconds", 0)) == 30
| extend outcome_count_scope=tostring(column_ifexists("outcome_count_scope", "")), outcome_interval_id=tostring(column_ifexists("outcome_interval_id", "")), outcome_interval_start=tostring(column_ifexists("outcome_interval_start", "")), outcome_interval_end=tostring(column_ifexists("outcome_interval_end", ""))
| extend outcome_interval_duration_ms=tolong(column_ifexists("outcome_interval_duration_ms", 0)), outcome_interval_complete=tobool(column_ifexists("outcome_interval_complete", false)), complete=tolong(column_ifexists("outcome_complete_cohort_count", 0)), qn=tolong(column_ifexists("outcome_quicknode_strict_fastest_count", 0)), foundation=tolong(column_ifexists("outcome_foundation_strict_fastest_count", 0)), hydromancer=tolong(column_ifexists("outcome_hydromancer_strict_fastest_count", 0)), ties=tolong(column_ifexists("outcome_tie_count", 0))
| where event_id != '' and runner != '' and run_id != '' and window_id != ''
| where outcome_count_scope == 'non-overlapping-publication-interval' and outcome_interval_complete == true and outcome_interval_duration_ms == 30000
| where outcome_interval_id != '' and outcome_interval_start != '' and outcome_interval_end != '' and complete > 0
// First collapse exact at-least-once replays by their complete owner payload.
| summarize duplicate_rows=count() by event_id, _time, runner, run_id, window_id, provider, protocol, outcome_interval_id, outcome_interval_start, outcome_interval_end, outcome_interval_duration_ms, complete, qn, foundation, hydromancer, ties
| extend event_payload=pack_dictionary('_time', _time, 'runner', runner, 'run_id', run_id, 'window_id', window_id, 'provider', provider, 'protocol', protocol, 'outcome_interval_id', outcome_interval_id, 'outcome_interval_start', outcome_interval_start, 'outcome_interval_end', outcome_interval_end, 'outcome_interval_duration_ms', outcome_interval_duration_ms, 'complete', complete, 'qn', qn, 'foundation', foundation, 'hydromancer', hydromancer, 'ties', ties)
// The same event ID with two payload variants is corrupt and is discarded.
| summarize variants=count(), event_rank=arg_max(duplicate_rows, event_payload) by event_id
| where variants == 1
| project _time=todatetime(event_payload._time), runner=tostring(event_payload.runner), run_id=tostring(event_payload.run_id), window_id=tostring(event_payload.window_id), provider=tostring(event_payload.provider), protocol=tostring(event_payload.protocol), outcome_interval_id=tostring(event_payload.outcome_interval_id), interval_start=tostring(event_payload.outcome_interval_start), interval_end=tostring(event_payload.outcome_interval_end), duration_ms=tolong(event_payload.outcome_interval_duration_ms), complete=tolong(event_payload.complete), qn=tolong(event_payload.qn), foundation=tolong(event_payload.foundation), hydromancer=tolong(event_payload.hydromancer), ties=tolong(event_payload.ties)
// Admit one exact Quicknode gRPC + Foundation WS + Hydromancer WS cohort only.
| summarize source_rows=count(), qn_rows=countif(provider == 'quicknode' and protocol == 'grpc'), foundation_rows=countif(provider == 'hyperliquid' and protocol == 'ws'), hydromancer_rows=countif(provider == 'hydromancer' and protocol == 'ws') by _time, runner, run_id, window_id, outcome_interval_id, interval_start, interval_end, duration_ms, complete, qn, foundation, hydromancer, ties
| where source_rows == 3 and qn_rows == 1 and foundation_rows == 1 and hydromancer_rows == 1
| where qn + foundation + hydromancer + ties == complete
| summarize complete_cohorts=sum(complete), quicknode_strict_fastest=sum(qn), foundation_strict_fastest=sum(foundation), hydromancer_strict_fastest=sum(hydromancer), tied_cohorts=sum(ties), intervals=count()
