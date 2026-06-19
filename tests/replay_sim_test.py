import desktop_app

with desktop_app._burst_cache_lock:
    desktop_app._burst_cache.clear()

# Simulate prefetch LogonId match but not confirmed under new rules
desktop_app._burst_cache_put(
    '192.168.254.105',
    'DESKTOP-KGG55PU\\User',
    'DESKTOP-0EDUBAP',
    '192.168.254.106',
    'DESKTOP-0EDUBAP\\User',
    local_actor=False,
    event_type='added',
    logon_id_confirmed=False,
)
print('After prefetch-like put:')
with desktop_app._burst_cache_lock:
    for k,v in desktop_app._burst_cache.items():
        print(k, '->', v)

# Later local-actor detection should overwrite (not be skipped)
desktop_app._burst_cache_put(
    '192.168.254.105',
    'DESKTOP-KGG55PU\\User',
    'DESKTOP-KGG55PU',
    '192.168.254.105',
    'DESKTOP-KGG55PU\\User',
    local_actor=True,
    event_type='added',
    logon_id_confirmed=False,
)
print('\nAfter local-actor put:')
with desktop_app._burst_cache_lock:
    for k,v in desktop_app._burst_cache.items():
        print(k, '->', v)
