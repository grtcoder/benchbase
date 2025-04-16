"""An example of constructing a profile with two ARM64 nodes connected by a LAN.
Instructions:
Wait for the profile instance to start, and then log in to either host via the
ssh ports specified below.
"""

import geni.portal as portal
import geni.rspec.pg as rspec

request = portal.context.makeRequestRSpec()

# Create two raw "PC" nodes
num_servers=2
num_brokers=2
num_directory=1

servers=[]
for i in range(num_servers):
    node=request.RawPC("server"+str(i+1))
    node.hardware_type = "c220g2"
    node.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU20-64-STD"
    servers.append(node)
    iface1 = node.addInterface("if1")

    # Specify the component id and the IPv4 address
    iface1.component_id = "eth1"
    iface1.addAddress(rspec.IPv4Address("192.168.1."+str(i+1), "255.255.255.0"))

brokers=[]
for i in range(num_brokers):
    node=request.RawPC("broker"+str(i+1))
    node.hardware_type = "m400"
    node.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU20-64-STD"
    brokers.append(node)
    iface1 = node.addInterface("if1")

    # Specify the component id and the IPv4 address
    iface1.component_id = "eth1"
    iface1.addAddress(rspec.IPv4Address("192.168.2."+str(i+1), "255.255.255.0"))

directory=request.RawPC("directory")
directory.hardware_type = "c220g2"
directory.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU20-64-STD"
iface1 = directory.addInterface("if1")

iface1.component_id = "eth1"
iface1.addAddress(rspec.IPv4Address("192.168.3."+str(i+1), "255.255.255.0"))

portal.context.printRequestRSpec()