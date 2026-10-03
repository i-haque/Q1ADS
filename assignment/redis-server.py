import redis
import re


def init_redis_cache():
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        with open("dummy.txt") as file:
            for line in file:
                words = re.findall(r'\b[a-zA-Z]+\b', line)
                for word in words:
                    r.incr(word, 1)

    except:
        print('could not connect to redis!!!')

    finally:
        r.close()


if __name__ == "__main__":
    init_redis_cache()
