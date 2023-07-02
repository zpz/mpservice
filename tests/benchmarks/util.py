import json
from faker import Faker

fake = Faker()


def make_message(seed=1234):
    fake = Faker()
    Faker.seed(seed)
    data = {
        'data': {
            'float': [x + 0.3 for x in range(10000)],
            'int': list(range(10000)),
        },
        'attributes': {fake.name(): fake.sentence() for _ in range(5000)},
        'details': [fake.text() for _ in range(2000)],
    }
    return data


def expensive(n=1000):
    # Return a random list of values going through json/unjson many times to simulate
    # an expensive computation. ``n=1000`` roughly will take 1 sec.
    # The time taken is roughly linear to ``n``.
    x = fake.json(num_rows=n)
    dumps, loads = json.dumps, json.loads
    x = loads(x)
    for _ in range(1000):
        x = loads(dumps(x))
    return x
