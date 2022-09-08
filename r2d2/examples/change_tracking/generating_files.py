#!/usr/bin/python
import csv
import random
import sys

records=int(sys.argv[1])
print("Making %d records\n" % records)
id = 0
for partition_id in range(10):
  fieldnames=['id','name', 'surname', 'age','city']
  writer_1 = csv.DictWriter(open(f"people_jan/{partition_id}", "w"), fieldnames=fieldnames)
  writer_2 = csv.DictWriter(open(f"people_feb/{partition_id}", "w"), fieldnames=fieldnames)

  # names=['Deepak', 'Sangeeta', 'Geetika', 'Anubhav', 'Sahil', 'Akshay']
  namess = ["Tamta", "Zviki", "Sandro", "Giorgi", "Gocha", "Lika", "Lali", "Elizabedi", "Vika", "Vera", "Fridoni", "Romeo", "Ilia", "Ana", "Taso", "Sesili", "Lela", "Nika", "Gela", "Eka", "Guja", "Teo"]
  surnames = [name + "shvili" for name in namess]
  cities = ['Delhi', 'Kolkata', 'Chennai', 'Mumbai', 'Salonik', "Tbilisi", "Khashuri", "Batumi", "Sachkhere", "Zestafoni", "Gurjaani", "Telavi", "Abasha"]

  # writer_1.writerow(dict(zip(fieldnames, fieldnames)))
  # writer_2.writerow(dict(zip(fieldnames, fieldnames)))

  for i in range(0, records):
    id += 1
    # print('a')
    name = random.choice(namess)
    surname = random.choice(surnames)
    age = random.randint(0, 100)
    city = random.choice(cities)
    # print('b')
    writer_1.writerow(dict([
      ('id', id),
      ('name', name),
      ('surname', surname),
      ('age', age),
      ('city', city)]))
    # print('c')
    new_city = random.choice(cities)
    city = random.choices([city, new_city], weights=(0.7, 0.3), k=1)[0]
    # print('d')
    writer_2.writerow(dict([
      ('id', id),
      ('name', name),
      ('surname', surname),
      ('age', age),
      ('city', city)]))
    # print()

