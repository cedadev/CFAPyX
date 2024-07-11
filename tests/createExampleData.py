import cf
import numpy as np

def create(i):
    # Create blank field with standard name
    p = cf.Field(properties={
        'standard_name':'rain'
    })

    shape = (2, 180, 360)
    size = np.prod(shape)

    domain_axisT   = cf.DomainAxis(shape[0])
    domain_axisLat = cf.DomainAxis(shape[1])
    domain_axisLon = cf.DomainAxis(shape[2])

    # Create an axis construct for the field with length 3
    time_axis      = p.set_construct(domain_axisT)
    latitude_axis  = p.set_construct(domain_axisLat)
    longitude_axis = p.set_construct(domain_axisLon)

    dt = np.random.rand(size).reshape(shape)

    data = cf.Data(dt)
    p.set_data(data)

    dimT = cf.DimensionCoordinate(
        properties={'standard_name': 'time',
                    'units'        : 'days since 2018-12-01'},
        data=cf.Data(
            [2*i +j +1 for j in range(shape[0])]
        )
    )

    dimLat = cf.DimensionCoordinate(
        properties={'standard_name': 'latitude',
                    'units'        : 'degrees_north'},
        data=cf.Data(np.arange(shape[1])-90)
    )

    dimLon = cf.DimensionCoordinate(
        properties={'standard_name': 'longitude',
                    'units'        : 'degrees_east'},
        data=cf.Data(np.arange(shape[2])-90)
    )

    p.set_construct(dimT)
    p.set_construct(dimLat)
    p.set_construct(dimLon)

    p.nc_set_variable('p')

    dimT.nc_set_variable('time')
    dimLat.nc_set_variable('lat')
    dimLon.nc_set_variable('lon')

    cf.write(p,f'rain/example{i}.nc')


for i in range(10):
    create(i)