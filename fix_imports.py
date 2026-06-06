with open('src/lavello_mlips/process_omol25.py', 'r') as f:
    lines = f.readlines()

new_lines = []
skip = False
for i, line in enumerate(lines):
    if i == 28:
        continue
    if i == 29:
        continue
    new_lines.append(line)

with open('src/lavello_mlips/process_omol25.py', 'w') as f:
    f.writelines(new_lines)
