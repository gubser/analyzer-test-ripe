import ripe.atlas.sagan as ripe


def generate_rtt(action_id, analyzer_id, kv):
    seqKey = kv[0]
    meta, data = kv[1]

    lines = data.split('\n')

    observations = []
    for line in lines:
        res = ripe.TracerouteResult(line)

        rtt = res.last_median_rtt
        # ignore measurement without rtt
        if rtt is None:
            continue

        # ignore private address probes
        if res.origin != res.source_address:
            continue

        path = [res.source_address, '*', res.destination_address]

        condition = res.protocol.lower() + "-rtt-median"

        observations.append({
            'condition': condition,
            'path': path,
            'time': res.created.timestamp(),
            'value': res.last_median_rtt,
            'analyzer_id': analyzer_id,
            'action_id': action_id,
            'sources': [meta['action_id']],
            'deprecated': False
        })
    return observations
