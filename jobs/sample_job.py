def processor(self):
    return 'I am a processor.'

def split(self, input_file, processor):
    yield taskunit.TaskUnit(data='hello', processor=processor)

def combine(self, taskunits):
    for t in taskunits:
        print(t.result)
    return

input_data = '12345\n6789'
input_file = None
