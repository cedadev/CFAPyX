# Give this the path to a module.
import os
import re
# Highlight specific functions

# Could take all global variables and track back to origin as well - could be a useful feature.

function_list = [
    'xarray.conventions.ensure_not_multiindex'
]

def extract_function(lines, func):
    line_count = 0
    started  = False
    finished = False
    func_content = ''
    while not finished and line_count < len(lines):
        line = lines[line_count]

        if started and re.search(f'def ', lines[line_count+1]):
            finished = True

        if re.search(f'def {func}', line):
            started = True
        
        if started:
            func_content += line

        line_count += 1

    return func_content

def search_function(base, fpath, fname):
    script_path = os.path.join(base, os.path.join(*fpath)) + '.py'

    with open(script_path) as f:
        contents = f.readlines()

    func = extract_function(contents, fname)
    return func
    
base = '/home/users/dwest77/Documents/cfa_python_dw/cf_dw/xarray'

collection = '## NOTE: Not a script which is meant to be run\n\n\n'

for f in function_list:
    fpath = f.split('.')
    fname = fpath[-1]
    fpath.pop()
    collection += search_function(base, fpath, fname)

with open('usefuls.py','w') as f:
    f.write(collection)
