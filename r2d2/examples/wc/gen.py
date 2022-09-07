import random
words = ["shota", "lekva", "kencho", "bichina", "ia"]

for pid in range(0, 10):
    with open(f'./in/{pid}', 'w') as f:
        for _ in range(0, 1000):
            for _ in range(0, 100):
                f.write(f'{words[random.randint(0, len(words)-1)]} ')
            f.write(f'\n')
