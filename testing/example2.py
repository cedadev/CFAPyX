import numpy
import cf

# Initialise the field construct with properties
Q = cf.Field(properties={'project': 'research',
                           'standard_name': 'specific_humidity',
                           'units': '1'})

# Create the domain axis constructs
domain_axisT = cf.DomainAxis(1)
domain_axisY = cf.DomainAxis(5)
domain_axisX = cf.DomainAxis(8)

# Insert the domain axis constructs into the field. The
# set_construct method returns the domain axis construct key that
# will be used later to specify which domain axis corresponds to
# which dimension coordinate construct.
axisT = Q.set_construct(domain_axisT)
axisY = Q.set_construct(domain_axisY)
axisX = Q.set_construct(domain_axisX)

# Create and insert the field construct data
data = cf.Data(numpy.arange(40.).reshape(5, 8))
Q.set_data(data)