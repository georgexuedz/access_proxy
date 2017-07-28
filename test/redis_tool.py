import redis

mac_ls = ['mac' + str(i) for i in range(10000)]


redis_o = redis.Redis(
    host='localhost',
    port=6379,
    db=0
)
redis_mac_set_key = 'redis_mac_set'

for mac in mac_ls:
	redis_o.sadd(redis_mac_set_key, mac)

