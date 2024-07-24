import cf

pwd = '/'.join(__file__.split('/')[:-2]) + '/testfiles'
print(f'Opening files under {pwd}')
f = cf.read(f'{pwd}/huss*')

print(dir(f))