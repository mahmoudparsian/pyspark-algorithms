from __future__ import print_function
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
import random
#---------------------------------------------------------
# Create 1000,000,000 "<key><,><value>" pairs such that 
#   key is a random number in range of 1 to 10,000
#   value is a random number in range of 1 to 5
#---------------------------------------------------------
for x in range(1000000000):
    print(str(random.randint(1,10000)) + "," + str(random.randint(1,5)))
