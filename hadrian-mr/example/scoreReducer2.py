from antinous import *

input = record(key={"type": "record", "name": "CompositeKey", "fields": [{"name": "groupby", "type": "string"}, {"name": "sortby", "type": "double", "order": "descending"}]},
               value=record(name=string, mass=double, radius=double, max_distance=double))

output = record("Output", key=string, distances=array(double))

tally = []

def action(input):
    tally.append(input.value.max_distance)
    emit({"key": input.key.groupby, "distances": tally})
