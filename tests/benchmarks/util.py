
import numpy
from faker import Faker


def make_message(seed=1234):
    fake = Faker()
    Faker.seed(seed)
    data = {
        'data': {
            'np': numpy.random.rand(100, 100),
            'float': [x + 0.3 for x in range(1000)],
            'int': list(range(1000)),
            },
        'attributes': {fake.name(): fake.sentence() for _ in range(500)},
        'details': [fake.text() for _ in range(200)],
        }
    return data
