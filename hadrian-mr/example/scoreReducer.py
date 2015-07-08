from antinous import *

input = record(key=string, value=record("Value", mass=double, radius=double, max_distance=double))
input.fields["value"].fields["name"] = string   # same as mapReduceMapper.py

output = record("Output", key=string, distances=array(double))

tally = []

def action(input):
    tally.append(input.value.max_distance)
    emit({"key": input.key, "distances": tally})
