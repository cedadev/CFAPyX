import cf

f = cf.read('../testfiles/rainmaker.nca')
q = f['p'].to_numpy()