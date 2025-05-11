import os

def set_env(proj_folder):
    with open(proj_folder/'.env', 'r') as file:
        for line in file:
            if line.find('=') > 0:
                var, val = line.split('=')
                os.environ[str(var).strip().lstrip()] = str(val).strip().lstrip()
