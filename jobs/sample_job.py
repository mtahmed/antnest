def processor(self):
    return 'I am a processor.'

def split(self, input_data, processor):
    yield taskunit.TaskUnit(data='hello', processor=processor)

def combine(self):
    for t in self.taskunits:
        print(t.result)
    return

input_data = '12345\n6789'
