import tfrecord

reader = tfrecord.example_loader(
    'ints.tfrecord', None, description={'data': 'byte'})

for example in reader:
    print(example['data'].shape)

