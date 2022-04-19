
import numpy
from faker import Faker


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
