def processor(self, string):
    return string[::-1]

def split(self, input_data, processor):
    input_lines = input_data.split('\n')
    for input_line in input_lines:
        yield taskunit.TaskUnit(data=input_line, processor=processor)

def combine(self):
    for t in self.taskunits:
        print(t.result)
    return

input_data = 'hello\nworld\nokay\nbye'
